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

Manual / CI in-cluster (run `bash scripts/smoke.sh` or via the GH Action):

- [ ] helm install brings up N replicas; each acquires its own broker
      advisory lock.
- [ ] `kubectl apply` a User; auto-generated credentials Secret appears with
      the expected wire-details; mosquitto round-trip works using the
      Secret's `uri`.
- [ ] `kubectl delete user`; the auto-generated Secret is GC'd by Kubernetes
      via owner-ref; the DB row is gone.
- [ ] `kubectl delete pod --grace-period=0 --force <one>` repeatedly while a
      sustained QoS-1 stream is in flight; no message loss.

Soak (manual):

- [ ] 1k msg/s for 10 min while killing a random Pod every 30 s; QoS-1
      shows zero loss; QoS-2 dedup holds.
