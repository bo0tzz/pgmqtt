#!/usr/bin/env python3
"""
Run the Eclipse Paho MQTT conformance suite against a running pgmqttd.

Usage:
    python3 paho-conformance.py --paho /path/to/paho.mqtt.testing \
        --host 127.0.0.1 --port 11883 --version 5

The Paho driver scripts use getopt + unittest.main in a way that makes
running individual tests with custom host/port awkward. This wrapper sets
all the module-level globals the __main__ block would have set, then runs
unittest.main on the requested test module.
"""
from __future__ import annotations

import argparse
import importlib.util
import os
import signal
import sys
import unittest


def _import_module_from_path(path: str, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not import {path}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


def _setup_v311(ct, host: str, port: int) -> None:
    ct.host = host
    ct.port = port
    # Same defaults as the script's __main__ block.
    ct.topics = ("TopicA", "TopicA/B", "Topic/C", "TopicA/C", "/TopicA")
    ct.wildtopics = ("TopicA/+", "+/C", "#", "/#", "/+", "+/+", "TopicA/#")
    ct.nosubscribe_topics = ("test/nosubscribe",)


def _setup_v5(ct, host: str, port: int) -> None:
    # setData() in client_test5.py sets topics/wildtopics/nosubscribe_topics
    # but NOT topic_prefix — that only happens in __main__. Replicate that here
    # so test_shared_subscriptions doesn't NameError on topic_prefix.
    ct.host = host
    ct.port = port
    ct.topic_prefix = "client_test5/"
    ct.topics = [
        ct.topic_prefix + t
        for t in ["TopicA", "TopicA/B", "Topic/C", "TopicA/C", "/TopicA"]
    ]
    ct.wildtopics = [
        ct.topic_prefix + t
        for t in ["TopicA/+", "+/C", "#", "/#", "/+", "+/+", "TopicA/#"]
    ]
    ct.nosubscribe_topics = ("test/nosubscribe",)


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--paho", required=True, help="path to paho.mqtt.testing checkout")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=11883)
    p.add_argument("--version", choices=["311", "5", "both"], default="both")
    p.add_argument("--per-test-timeout", type=int, default=60,
                   help="seconds; tests that exceed this are reported as TIMEOUT")
    p.add_argument("--only", nargs="*", help="restrict to these test names")
    args = p.parse_args()

    interop = os.path.join(args.paho, "interoperability")
    sys.path.insert(0, interop)

    overall_pass = True
    versions = ["311", "5"] if args.version == "both" else [args.version]

    def handler(_signum, _frame):
        raise TimeoutError("test exceeded per-test budget")

    for version in versions:
        if version == "311":
            mod_path = os.path.join(interop, "client_test.py")
            ct = _import_module_from_path(mod_path, "client_test_v311")
            _setup_v311(ct, args.host, args.port)
        else:
            mod_path = os.path.join(interop, "client_test5.py")
            ct = _import_module_from_path(mod_path, "client_test_v5")
            _setup_v5(ct, args.host, args.port)

        candidates = [m for m in dir(ct.Test) if m.startswith("test") and (
            args.only is None or m in args.only
        )]
        candidates.sort()

        print(f"\n=== Paho v{version} — {len(candidates)} tests ===", flush=True)
        results = []
        for name in candidates:
            signal.signal(signal.SIGALRM, handler)
            signal.alarm(args.per_test_timeout)
            suite = unittest.TestLoader().loadTestsFromName(name, ct.Test)
            runner = unittest.TextTestRunner(verbosity=0, stream=open(os.devnull, "w"))
            try:
                r = runner.run(suite)
                if r.wasSuccessful():
                    results.append((name, "PASS", ""))
                else:
                    why = ""
                    for f in r.failures + r.errors:
                        lines = [l for l in str(f[1]).strip().splitlines() if l]
                        why = lines[-1][:160] if lines else ""
                        break
                    results.append((name, "FAIL", why))
            except TimeoutError:
                results.append((name, "TIMEOUT", f">{args.per_test_timeout}s"))
            except Exception as e:
                results.append((name, "EXCEPT", str(e)[:160]))
            finally:
                signal.alarm(0)
            name_, status, why = results[-1]
            tag = "✓" if status == "PASS" else "✗"
            print(f"  {tag} {name_:50s} {status} {why}", flush=True)

        passes = sum(1 for _, s, _ in results if s == "PASS")
        print(f"=== v{version}: {passes}/{len(results)} passing ===", flush=True)
        if passes != len(results):
            overall_pass = False

    return 0 if overall_pass else 1


if __name__ == "__main__":
    sys.exit(main())
