# Verification checklist

Tests in code (run `go test ./... -count=1`):

- [x] migrations idempotency, topic-match function (`internal/db/migrate_test.go`)
- [x] `mqtt_next_packet_id` allocator (`internal/db/migrate_test.go`)
- [x] codec round-trip — PINGREQ, CONNECT->PUBLISH v5 (`internal/mqtt/codec_test.go`)
- [x] topic name + filter validators (`internal/mqtt/topic_test.go`)
- [x] QoS 0 / 1 publish-subscribe round-trip (`internal/engine/engine_test.go`)
- [x] retained delivery + retain clear with empty payload
- [x] persistent session resume across reconnect
- [x] NoLocal suppression
- [x] will fires on ungraceful disconnect
- [x] will suppressed on graceful disconnect
- [x] cross-Pod fanout (`internal/listener/listener_test.go`)
- [x] takeover closes the prior Pod's socket
- [x] leader election: only one acquires; second promotes on first's stop
  (`internal/leader/leader_test.go`)
- [x] janitor fires will from a dead-broker session
- [x] janitor sweeps orphan messages older than the grace
- [x] reconciler — auto-generated Secret + bcrypt-upsert
- [x] reconciler — BYO Secret path (no auto-generated Secret created)
- [x] reconciler — deletion removes the DB row
- [x] janitor — `inbound_qos2` tombstone GC for disconnected sessions
- [x] engine — slow-subscriber backpressure → DISCONNECT 0x97
- [x] engine — max-connections cap → CONNACK 0x9F
- [x] engine — per-conn inbound rate limit → DISCONNECT 0x96
- [x] config — env-driven defaults + bcrypt cost out-of-range rejection
- [x] metrics — registered series render via promhttp; Serve binds and
  shuts down cleanly

Manual / CI in-cluster (run via the GH Action `smoke` job, also runnable
locally — see [docs/CONFORMANCE.md](CONFORMANCE.md)):

- [x] helm install brings up N replicas; each acquires its own broker
      advisory lock.
- [x] `kubectl apply` a User; auto-generated credentials Secret appears
      with the expected wire-details; mosquitto round-trip works using the
      Secret's host/port/username/password.
- [x] `kubectl delete user`; the auto-generated Secret is GC'd by
      Kubernetes via owner-ref; the DB row is gone.
- [x] `kubectl delete pod --grace-period=0 --force <one>` repeatedly while
      a sustained QoS-1 stream is in flight; no message loss. Implemented
      as `cmd/soak` + `scripts/soak.sh`. Local smoke verified zero loss
      and zero dups for QoS-1 and QoS-2.

Eclipse Paho conformance suite
([`paho.mqtt.testing`](https://github.com/eclipse-paho/paho.mqtt.testing)) —
results recorded in [docs/CONFORMANCE.md](CONFORMANCE.md). Last run:

- v3.1.1: 9/10 pass. Only `test_subscribe_failure` fails (needs ACLs;
  out of v1 scope).
- v5: 23/27 pass deterministically; the 2 flakes (`test_request_response`,
  `test_subscribe_options`) are upstream `waitfor` typos referencing the
  wrong callback. Of the remaining 2 fails: `test_subscribe_failure`
  (ACLs, out of scope) and `test_shared_subscriptions` (shared subs, out
  of scope).

Soak (manual):

- [x] 1k msg/s while killing a random Pod every 30 s; QoS-1 shows zero
      loss; QoS-2 dedup holds. Implemented as `cmd/soak` plus
      `scripts/soak.sh`; the latter wires the chaos loop and runs the
      generator. See also `scripts/paho-multi-broker.sh` (3-Pod
      conformance) and `scripts/ha-z2m-soak.sh` (HA + synthetic Z2M
      under chaos).
