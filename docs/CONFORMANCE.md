# Conformance results

Recorded against the Eclipse Paho `paho.mqtt.testing` interoperability
suite ([`eclipse-paho/paho.mqtt.testing`](https://github.com/eclipse-paho/paho.mqtt.testing)).

## Running the suite

Start a broker with anonymous auth so the suite can connect without seeded
users:

```bash
docker run --rm -d --name pgmqtt-pg -p 55432:5432 \
  -e POSTGRES_USER=pgmqtt -e POSTGRES_PASSWORD=pgmqtt -e POSTGRES_DB=pgmqtt \
  postgres:16-alpine

go build -o pgmqttd ./cmd/pgmqttd
PGMQTT_DATABASE_URL='postgres://pgmqtt:pgmqtt@localhost:55432/pgmqtt?sslmode=disable' \
PGMQTT_TCP_ADDR=127.0.0.1:11883 \
PGMQTT_WS_ADDR= \
PGMQTT_ALLOW_ANONYMOUS=true \
  ./pgmqttd
```

Clone Paho and run the wrapper:

```bash
git clone --depth=1 https://github.com/eclipse-paho/paho.mqtt.testing.git
python3 scripts/paho-conformance.py \
  --paho /path/to/paho.mqtt.testing \
  --port 11883 \
  --version both \
  --per-test-timeout 60
```

The wrapper exists because the Paho driver scripts use `getopt` + a
`unittest.main` argv handoff in a way that conflicts with custom host/port
flags on the command line; it imports each test module by path, populates
the same module globals the upstream `__main__` block sets (including
`topic_prefix` for v5, which `setData()` doesn't set), and runs each test
under a per-test alarm timeout.

## v3.1.1 — `client_test.py`

| Test | Result | Notes |
| - | - | - |
| testBasic | PASS | |
| test_dollar_topics | PASS | |
| test_keepalive | PASS | |
| test_offline_message_queueing | PASS | |
| test_overlapping_subscriptions | PASS | We deliver one PUBLISH per matching client, not one per matching subscription. |
| test_redelivery_on_reconnect | PASS | |
| test_retained_messages | PASS | |
| test_unsubscribe | PASS | |
| test_zero_length_clientid | PASS | |
| test_subscribe_failure | **FAIL** | Needs ACLs to selectively reject filters; ACLs are documented out-of-scope. |

**9/10.** The single fail is the only test that requires features outside the
v1 plan (ACLs).

## v5 — `client_test5.py`

| Test | Result | Notes |
| - | - | - |
| test_assigned_clientid | PASS | |
| test_basic | PASS | |
| test_client_topic_alias | PASS | |
| test_dollar_topics | PASS | |
| test_flow_control1 | PASS | Outbound flow ctrl: per-conn token bucket sized to client's ReceiveMaximum. |
| test_flow_control2 | PASS | Inbound flow ctrl: server-advertised ReceiveMaximum (100); excess inbound QoS>0 → DISCONNECT 0x93. |
| test_keepalive | PASS | Will fires on keepalive timeout. |
| test_maximum_packet_size | PASS | Outbound size enforced; oversize PUBLISHes dropped. |
| test_offline_message_queueing | PASS | |
| test_overlapping_subscriptions | PASS | |
| test_payload_format | PASS | |
| test_publication_expiry | PASS | MessageExpiryInterval enforced; remaining time decremented on outbound. |
| test_redelivery_on_reconnect | PASS | |
| test_retained_message | PASS | UserProperty round-trips through retained. |
| test_server_keep_alive | PASS | Keepalive capped at 60s; ServerKeepAlive advertised when overridden. |
| test_server_topic_alias | PASS | Outbound topic-alias map per-conn, capacity = client TopicAliasMaximum. |
| test_session_expiry | PASS | Janitor expires sessions past `session_expires_at`. |
| test_subscribe_identifiers | PASS | Multiple matching sub-ids aggregated into the SubscriptionIdentifier list. |
| test_unsubscribe | PASS | |
| test_user_properties | PASS | |
| test_will_delay | PASS | Janitor fires delayed wills at `will_fire_at`; reconnect cancels. |
| test_will_message | PASS | |
| test_zero_length_clientid | PASS | |
| test_request_response | **FLAKY** | Paho test races: `waitfor(callback.subscribeds, ...)` checks the publishing client's callback after the subscribing client subscribes (callback vs. callback2 typo). When the broker is fast enough to commit bclient's SUBSCRIBE before aclient's PUBLISH, this passes; otherwise fails with `0 != 1`. Fix is upstream in Paho. |
| test_subscribe_options | **FLAKY** | Same `waitfor` race in the noLocal block. |
| test_subscribe_failure | **FAIL** | Needs ACLs; out of v1 scope. |
| test_shared_subscriptions | **FAIL** | Shared subscriptions are out of v1 scope per the design plan. |

**23/27** deterministic pass; 24/27 with the Paho `waitfor` flake counted
as a pass when timing favours it. 2 fails on documented out-of-scope
features (shared subs, ACLs).

## Local kind smoke

```bash
kind create cluster --name pgmqtt-test
docker build -t pgmqtt:test .
kind load docker-image pgmqtt:test --name pgmqtt-test

kubectl create namespace mqtt
kubectl -n mqtt apply -f .github/ci/postgres.yaml
kubectl -n mqtt rollout status statefulset/postgres --timeout=180s

helm install pgmqtt deploy/helm/pgmqtt \
  --namespace mqtt \
  --set image.repository=pgmqtt --set image.tag=test --set image.pullPolicy=IfNotPresent \
  --set database.url='postgres://pgmqtt:pgmqtt@postgres.mqtt.svc:5432/pgmqtt?sslmode=disable'

bash .github/ci/smoke-in-cluster.sh   # apply User CR, mosquitto round-trip, delete
```

End-to-end pass — both pods Running, User CR → Secret → mosquitto round-trip
→ Secret GC on User delete.
