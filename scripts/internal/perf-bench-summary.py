#!/usr/bin/env python3
"""Render a Markdown summary of a perf-bench run.

Reads:
  $OUTDIR/metrics/<phase>.<pod>.txt   — Prometheus exposition snapshots
  $OUTDIR/logs/<pod>.log              — broker stdout/stderr
  $OUTDIR/soak.json                   — cmd/soak's verdict

Emits to stdout a Markdown summary covering:
  * Soak-rig verdict at the headline.
  * For each stage histogram (publish / delivery / pgx-acquire), the
    cumulative count/sum delta from baseline to final, plus the implied
    average and an indicator of whether the p99 bucket grew.
  * The "slow stage" events the broker logged during the run, counted
    by stage and listing the top-N slowest.
  * Janitor sweep activity from the new janitor_swept_rows_total
    counter.
  * Resource gauges (goroutines, GC counts, RSS) sampled at final.

Intentionally read-only post-processing — the bench script captures
raw data; this turns it into something a human can scan in 30 seconds.
"""

import json
import os
import re
import sys
from collections import Counter, defaultdict
from glob import glob


# Bucket tags match the on-wire `le` label exactly. Prometheus's Go
# client emits the value as Go's default float formatting (no leading
# zero stripping: `0.01` not `.01`, `1` not `1.0`).
HIST_BUCKETS = [
    ("0.0001", 0.0001),
    ("0.0002", 0.0002),
    ("0.0005", 0.0005),
    ("0.001", 0.001),
    ("0.002", 0.002),
    ("0.005", 0.005),
    ("0.01", 0.01),
    ("0.02", 0.02),
    ("0.05", 0.05),
    ("0.1", 0.1),
    ("0.25", 0.25),
    ("0.5", 0.5),
    ("1", 1),
    ("2.5", 2.5),
    ("5", 5),
    ("+Inf", float("inf")),
]


def parse_metrics(path):
    """Return dict mapping (metric_name, frozenset_of_(k,v)_label_pairs) → float.

    Label-order-agnostic: Go's prometheus client emits labels in
    declaration order (so {stage="alloc",le="0.001"} for our histograms,
    not {le="0.001",stage="alloc"}). Storing as a frozenset of pairs
    decouples the lookup from the on-wire ordering.
    """
    out = {}
    if not os.path.exists(path):
        return out
    label_re = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\]|\\.)*)"')
    with open(path) as f:
        for line in f:
            line = line.rstrip()
            if not line or line.startswith("#"):
                continue
            m = re.match(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{([^}]*)\})?\s+(\S+)", line)
            if not m:
                continue
            name = m.group(1)
            labels_blob = m.group(3) or ""
            val = m.group(4)
            try:
                v = float(val)
            except ValueError:
                continue
            pairs = frozenset(label_re.findall(labels_blob))
            out[(name, pairs)] = v
    return out


def get(snapshots, name, **labels):
    """Sum (name, labels) across a list of snapshots, label-order-agnostic."""
    want = frozenset(labels.items())
    total = 0.0
    for snap in snapshots:
        total += snap.get((name, want), 0)
    return total


def sum_across_pods(prefix, snapshots):
    """Legacy string-key form retained for non-labeled metrics. prefix is
    'name' or 'name{le=\"...\"}' style. Parses out labels and forwards
    to get()."""
    m = re.match(r'^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{([^}]*)\})?$', prefix)
    if not m:
        return 0.0
    name = m.group(1)
    labels_blob = m.group(3) or ""
    label_re = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\]|\\.)*)"')
    pairs = dict(label_re.findall(labels_blob))
    return get(snapshots, name, **pairs)


def hist_summary(label, phase_snapshots_pairs):
    """Return (count_delta, sum_delta, avg_ms, max_bucket_with_growth)
    for a histogram across all pods, computed as final - baseline.

    phase_snapshots_pairs is a list of (phase_name, [snap, snap, ...])
    in the order [baseline, final]. Each snap is a dict from
    parse_metrics.
    """
    if len(phase_snapshots_pairs) != 2:
        return (0, 0.0, 0.0, None)
    (_, base_snaps), (_, final_snaps) = phase_snapshots_pairs
    base_count = sum_across_pods(f"{label}_count", base_snaps)
    final_count = sum_across_pods(f"{label}_count", final_snaps)
    base_sum = sum_across_pods(f"{label}_sum", base_snaps)
    final_sum = sum_across_pods(f"{label}_sum", final_snaps)
    dcount = final_count - base_count
    dsum = final_sum - base_sum
    avg_ms = (dsum / dcount * 1000) if dcount > 0 else 0
    # Look at the highest bucket whose count grew — rough p99-ish indicator
    max_growth_bucket = None
    for tag, _ in reversed(HIST_BUCKETS):
        # All pods, all stage labels (caller filters by including stage in label)
        base_b = sum_across_pods(f"{label}_bucket{{le=\"{tag}\"}}", base_snaps)
        final_b = sum_across_pods(f"{label}_bucket{{le=\"{tag}\"}}", final_snaps)
        if final_b - base_b > 0:
            max_growth_bucket = tag
            break
    return (dcount, dsum, avg_ms, max_growth_bucket)


def discover_stages(snapshots, family):
    """Find all stage label values present in `family`_count series."""
    stages = set()
    name = f"{family}_count"
    for snap in snapshots:
        for (mname, pairs), _ in snap.items():
            if mname != name:
                continue
            for k, v in pairs:
                if k == "stage":
                    stages.add(v)
    return sorted(stages)


def hist_for_stage(family, stage, base_snaps, final_snaps):
    dcount = get(final_snaps, f"{family}_count", stage=stage) - get(base_snaps, f"{family}_count", stage=stage)
    dsum = get(final_snaps, f"{family}_sum", stage=stage) - get(base_snaps, f"{family}_sum", stage=stage)
    avg_ms = (dsum / dcount * 1000) if dcount > 0 else 0
    p99_bucket = None
    p100_bucket = None
    if dcount > 0:
        p99_target = dcount * 0.99
        # Cumulative buckets: count(le) is "observations ≤ le". p99 is
        # the smallest le whose cumulative count delta covers 99%.
        for tag, _ in HIST_BUCKETS:
            delta = get(final_snaps, f"{family}_bucket", stage=stage, le=tag) - get(base_snaps, f"{family}_bucket", stage=stage, le=tag)
            if delta >= p99_target:
                p99_bucket = tag
                break
        # p100 (max observed) = smallest le that captures EVERY new
        # observation — i.e. count(le) == dcount. +Inf always satisfies
        # this trivially; the useful answer is the smallest non-+Inf le
        # that does, which tells us the actual tail bucket.
        for tag, _ in HIST_BUCKETS:
            delta = get(final_snaps, f"{family}_bucket", stage=stage, le=tag) - get(base_snaps, f"{family}_bucket", stage=stage, le=tag)
            if delta >= dcount:  # all observations covered
                p100_bucket = tag
                break
    return dcount, dsum, avg_ms, p99_bucket, p100_bucket


def slow_stage_events(logs_dir):
    """Parse 'slow stage' lines from broker logs. Each line is structured
    slog output; pull kind/stage/dur_ms via regex."""
    events = []
    for path in glob(os.path.join(logs_dir, "*.log")):
        pod = os.path.basename(path).rsplit(".", 1)[0]
        with open(path) as f:
            for line in f:
                if '"slow stage"' not in line and "msg=\"slow stage\"" not in line:
                    continue
                kind = re.search(r"\bkind=(\S+)", line)
                stage = re.search(r"\bstage=(\S+)", line)
                dur = re.search(r"\bdur_ms=(\d+)", line)
                if not (kind and stage and dur):
                    continue
                events.append({
                    "pod": pod,
                    "kind": kind.group(1),
                    "stage": stage.group(1),
                    "dur_ms": int(dur.group(1)),
                    "line": line.strip(),
                })
    return events


def load_phase(metrics_dir, phase):
    snaps = []
    for path in sorted(glob(os.path.join(metrics_dir, f"{phase}.*.txt"))):
        snaps.append(parse_metrics(path))
    return snaps


def main():
    outdir = sys.argv[1] if len(sys.argv) > 1 else "."
    metrics_dir = os.path.join(outdir, "metrics")
    logs_dir = os.path.join(outdir, "logs")
    soak_json = os.path.join(outdir, "soak.json")

    print("# perf-bench summary")
    print()

    if os.path.exists(soak_json):
        try:
            soak = json.load(open(soak_json))
            recv = sum(r.get("received", 0) for r in soak.get("sub_reports") or [])
            pub = soak.get("published", 0)
            pct = round(recv / (pub * len(soak.get("sub_reports") or [1])) * 100, 1) if pub else 0
            print(f"## Soak verdict\n")
            print(f"- duration: `{soak.get('duration')}`")
            print(f"- rate: `{soak.get('rate')}/s`")
            print(f"- pubs/subs/qos: `{soak.get('pubs')}/{soak.get('subs')}/{soak.get('qos')}`")
            print(f"- published: **{pub}**")
            print(f"- received: **{recv}** ({pct}% of expected {pub * len(soak.get('sub_reports') or [1])})")
            print(f"- lost / dups: `{soak.get('total_lost', 0)} / {soak.get('total_dups', 0)}`")
            print()
        except (json.JSONDecodeError, KeyError) as e:
            print(f"_soak.json parse error: {e}_\n")

    base = load_phase(metrics_dir, "00-baseline")
    final = load_phase(metrics_dir, "99-final")

    if not base or not final:
        print(f"_missing metric snapshots; base={len(base)} final={len(final)}_")
        return

    print("## Per-stage histograms (baseline → final delta)\n")
    print("p99 = smallest bucket whose cumulative count covers 99% of new observations. ")
    print("max = smallest bucket that covers 100% (i.e. nothing observed beyond it). ")
    print("Buckets are in seconds.\n")
    print("| family | stage | count | avg ms | p99 ≤ | max ≤ |")
    print("|---|---|---:|---:|---|---|")
    for family in ("pgmqtt_publish_seconds", "pgmqtt_delivery_seconds"):
        for stage in discover_stages(final, family):
            dcount, _, avg_ms, p99, top = hist_for_stage(family, stage, base, final)
            if dcount == 0:
                continue
            short = family.replace("pgmqtt_", "").replace("_seconds", "")
            print(f"| {short} | {stage} | {int(dcount)} | {avg_ms:.2f} | {p99 or '—'} | {top or '—'} |")
    # Single-histogram (no stage label)
    print()
    print("### Pool acquire (no stage label)\n")
    pgx_count_delta = sum_across_pods("pgmqtt_pgx_acquire_seconds_count", final) - sum_across_pods("pgmqtt_pgx_acquire_seconds_count", base)
    pgx_sum_delta = sum_across_pods("pgmqtt_pgx_acquire_seconds_sum", final) - sum_across_pods("pgmqtt_pgx_acquire_seconds_sum", base)
    if pgx_count_delta > 0:
        print(f"- acquires: **{int(pgx_count_delta)}**")
        print(f"- avg ms: **{pgx_sum_delta/pgx_count_delta*1000:.2f}**")
        # Smallest bucket that covers 100% — the actual tail bucket.
        top = None
        for tag, _ in HIST_BUCKETS:
            delta = get(final, "pgmqtt_pgx_acquire_seconds_bucket", le=tag) - get(base, "pgmqtt_pgx_acquire_seconds_bucket", le=tag)
            if delta >= pgx_count_delta:
                top = tag
                break
        # p99
        p99 = None
        for tag, _ in HIST_BUCKETS:
            delta = get(final, "pgmqtt_pgx_acquire_seconds_bucket", le=tag) - get(base, "pgmqtt_pgx_acquire_seconds_bucket", le=tag)
            if delta >= pgx_count_delta * 0.99:
                p99 = tag
                break
        print(f"- p99 ≤ `{p99}` s, max ≤ `{top}` s")
    print()

    print("## Slow-stage events (broker logged when stage exceeded SLOW_STAGE_LOG_MS)\n")
    events = slow_stage_events(logs_dir)
    if not events:
        print("_no slow-stage events captured. Either every observation was under threshold, or PGMQTT_SLOW_STAGE_LOG_MS is unset/0._\n")
    else:
        print(f"Total: **{len(events)}** events.\n")
        by_stage = Counter(f"{e['kind']}/{e['stage']}" for e in events)
        print("| kind/stage | count | max ms | median ms |")
        print("|---|---:|---:|---:|")
        groups = defaultdict(list)
        for e in events:
            groups[f"{e['kind']}/{e['stage']}"].append(e["dur_ms"])
        for ks, durs in sorted(groups.items(), key=lambda kv: -len(kv[1])):
            durs_sorted = sorted(durs)
            mx = durs_sorted[-1]
            md = durs_sorted[len(durs_sorted)//2]
            print(f"| {ks} | {len(durs)} | {mx} | {md} |")
        print()
        print("### Top 10 slowest events\n")
        events.sort(key=lambda e: -e["dur_ms"])
        for e in events[:10]:
            print(f"- `{e['kind']}/{e['stage']}` **{e['dur_ms']} ms** ({e['pod']}): `{e['line'][:200]}`")
        print()

    print("## Janitor sweep activity (delta)\n")
    swept_jobs = set()
    for snap in final:
        for (name, pairs), _ in snap.items():
            if name != "pgmqtt_janitor_swept_rows_total":
                continue
            for k, v in pairs:
                if k == "job":
                    swept_jobs.add(v)
    if not swept_jobs:
        print("_no swept-rows metric available — running an older broker?_\n")
    else:
        print("| job | rows swept |")
        print("|---|---:|")
        for job in sorted(swept_jobs):
            d = get(final, "pgmqtt_janitor_swept_rows_total", job=job) - get(base, "pgmqtt_janitor_swept_rows_total", job=job)
            if d > 0:
                print(f"| {job} | {int(d)} |")
        print()

    print("## Resource snapshot (final phase, per pod)\n")
    print("| pod | goroutines | gc count | RSS approx (B) |")
    print("|---|---:|---:|---:|")
    for path in sorted(glob(os.path.join(metrics_dir, "99-final.*.txt"))):
        pod = os.path.basename(path).rsplit(".", 2)[1]
        snap = parse_metrics(path)
        gor = int(snap.get(("go_goroutines", frozenset()), 0))
        gc = int(snap.get(("go_gc_duration_seconds_count", frozenset()), 0))
        rss = int(snap.get(("process_resident_memory_bytes", frozenset()), 0))
        print(f"| {pod} | {gor} | {gc} | {rss:,} |")


if __name__ == "__main__":
    main()
