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

## v5 spec areas not exercised by the Paho suite

The Paho v5 conformance suite covers ~70% of the protocol surface
practically used by MQTT clients in the wild. A spec walk-through
identified these areas where pgmqtt's behaviour is conformant by
*omission* rather than by passing a test:

- **Enhanced authentication (3.15, 4.12).** AUTH packets and the
  `AuthenticationMethod` / `AuthenticationData` properties are not
  supported. CONNECT with `AuthenticationMethod` set is rejected with
  CONNACK 0x8C (Bad authentication method); a stray AUTH packet
  mid-connection elicits DISCONNECT 0x82 (Protocol error). Both are
  spec-compliant rejections — the broker just doesn't advertise any
  enhanced-auth method and so doesn't have to implement the protocol
  flow.
- **ResponseInformation (3.1.2.11.7 / 3.2.2.3.15).** The server is
  permitted ("MAY") to return a ResponseInformation property in
  CONNACK when the client sets `RequestResponseInformation=1`. We
  return nothing — also compliant by MAY-omission. Useful for
  brokers that publish a topic prefix for request/response patterns;
  pgmqtt has no opinion on request/response topic conventions.
- **`SessionExpiryInterval=0xFFFFFFFF` (3.1.2.11.2).** Honoured as
  "session never expires." Internally stored as `*uint32`. Values in
  the `[0x80000000, 0xFFFFFFFE]` range are also handled correctly
  (previously a `*int32` cast made them appear negative); the
  `expiry_interval` column on `sessions` clamps to `MaxInt32` for
  storage but the in-memory authoritative value preserves the full
  uint32 range.
- **Will-publish MessageExpiryInterval after delay.** When a delayed
  will fires (`WillDelayInterval > 0`, fired by janitor at
  `will_fire_at`), the published message gets its full
  `MessageExpiryInterval` from the firing point — i.e. the
  delay-window time is *not* subtracted. Per spec 3.3.2.3.3 this is
  defensible: the will message wasn't "waiting" during the delay, it
  hadn't been published yet. Brokers that treat the will as
  "received at CONNECT and held" would subtract; pgmqtt treats it as
  "published when the delay elapses." Documented here so test riggers
  and conformance-suite authors know which side we're on.

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

## Multi-broker via Service VIP (kind, 3 replicas)

Same Paho suite, but driven from inside a 3-replica kind cluster against
the broker `Service` VIP (`pgmqtt.mqtt.svc.cluster.local:1883`). Each
test's clients land on whichever Pod kube-proxy picks; subscribers and
publishers therefore routinely sit on different brokers, exercising the
Postgres-coordinated handoff path.

The runner is a Pod (`paho-runner` image: Python + Paho test repo) so the
clients connect via in-cluster DNS and stable VIP rather than a flaky
`kubectl port-forward`.

| Suite | Single-broker | Multi-broker |
| - | - | - |
| v3.1.1 (`client_test.py`) | 9/10 | 9/10 |
| v5 (`client_test5.py`)    | 23/27 deterministic | 23/27 deterministic |

Same deterministic result. The two `waitfor` flakes
(`test_request_response`, `test_subscribe_options`) each land on the
favourable side roughly half the time; on the recorded multi-broker run
`test_request_response` passed and `test_subscribe_options` failed for a
24/27 raw score, but both flakes can flip in either direction on either
deployment. The deterministic 23/27 — and the two genuine fails on
out-of-scope features (shared subs, ACLs) — are what's reproducible.

What this verifies that single-broker doesn't: every test that involves
two clients (basically all of them) is exercising the Postgres-coordinated
handoff path between Pods, since kube-proxy's per-connection round-robin
puts pub and sub on different brokers most of the time. Single-broker
runs in-process, so the broker→broker handoff never fires.
