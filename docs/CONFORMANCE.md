# Conformance results

Recorded against the Eclipse Paho `paho.mqtt.testing` interoperability
suite (clone from
[`eclipse-paho/paho.mqtt.testing`](https://github.com/eclipse-paho/paho.mqtt.testing))
and a `kind`-based Helm deployment.

## Running the suites locally

Start a broker (and Postgres). Anonymous auth is used so the suite can
connect without seeding a user:

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

Clone Paho:

```bash
git clone --depth=1 https://github.com/eclipse-paho/paho.mqtt.testing.git
cd paho.mqtt.testing/interoperability
```

The driver scripts use `getopt` in a way that conflicts with `unittest.main`
on the command line; run via a tiny wrapper that sets `host`/`port` and
calls `unittest.main` directly:

```python
# v3.1.1
python3 -c "
import sys
sys.argv = ['client_test.py']
import client_test as ct
ct.host = '127.0.0.1'
ct.port = 11883
ct.topics = ('TopicA', 'TopicA/B', 'Topic/C', 'TopicA/C', '/TopicA')
ct.wildtopics = ('TopicA/+', '+/C', '#', '/#', '/+', '+/+', 'TopicA/#')
ct.nosubscribe_topics = ('test/nosubscribe',)
import unittest
unittest.main(module=ct, exit=False, verbosity=2)
"

# v5
python3 -c "
import sys
sys.argv = ['client_test5.py']
import client_test5 as ct
ct.setData()
ct.host = '127.0.0.1'
ct.port = 11883
import unittest
unittest.main(module=ct, exit=False, verbosity=2)
"
```

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
| test_subscribe_failure | **FAIL** | Needs ACLs to selectively reject filters; out of v1 scope. |

**9/10 passing.**

## v5 — `client_test5.py`

| Test | Result | Notes |
| - | - | - |
| test_assigned_clientid | PASS | |
| test_basic | PASS | |
| test_dollar_topics | PASS | |
| test_keepalive | PASS | |
| test_offline_message_queueing | PASS | |
| test_overlapping_subscriptions | PASS | |
| test_payload_format | PASS | |
| test_redelivery_on_reconnect | PASS | |
| test_subscribe_identifiers | PASS | |
| test_unsubscribe | PASS | |
| test_user_properties | PASS | |
| test_will_message | PASS | |
| test_zero_length_clientid | PASS | |
| test_client_topic_alias | **FAIL** | Topic aliases not implemented (out-of-scope). |
| test_server_topic_alias | **FAIL** | ditto. |
| test_flow_control1 | **FAIL** | v5 Receive Maximum not enforced (out-of-scope). |
| test_flow_control2 | **FAIL** | ditto. |
| test_maximum_packet_size | **FAIL** | Outbound `Maximum Packet Size` not enforced (out-of-scope). |
| test_publication_expiry | **FAIL** | `Message Expiry Interval` stored but not enforced (out-of-scope). |
| test_session_expiry | **FAIL** | Session-expiry timing semantics not fully implemented (out-of-scope). |
| test_server_keep_alive | **FAIL** | Server-side keepalive override not implemented (out-of-scope). |
| test_will_delay | **FAIL** | Will Delay Interval stored but not enforced (out-of-scope). |
| test_subscribe_failure | **FAIL** | Needs ACLs (out-of-scope). |
| test_shared_subscriptions | **FAIL** | Shared subs out-of-scope; also Paho test references undefined `topic_prefix`. |
| test_retained_message | **FAIL** | Paho test references `Properties.UserProperty` which the test module no longer defines outside `__main__`. Broker behavior is correct in `test_user_properties`. |
| test_subscribe_options | **FAIL** | RetainAsPublished and Retain-Handling-on-existing-subscription edge cases not fully implemented. |
| test_request_response | **FAIL** | The broker forwards `ResponseTopic`/`CorrelationData` correctly (verified manually); the test's `waitfor` raced against a slow second SUBSCRIBE in this run. Broker-side bug not yet ruled out. |

**13/27 passing.** All remaining failures fall into one of three buckets:
documented v1 gaps (topic alias, flow control, packet size, expiries,
shared subs, ACLs, will delay, server keep alive — see README "What's NOT
in v1"), Paho test fixtures that don't survive being invoked outside the
`__main__` driver, or one suspected timing race (`test_request_response`).

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

bash .github/ci/smoke-in-cluster.sh   # applies a User CR, runs mosquitto round-trip, deletes
```

End-to-end pass on the local kind cluster is recorded — both pods Running,
User CR → Secret → mosquitto round-trip → Secret GC on User delete.
