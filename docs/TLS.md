# TLS termination

`pgmqttd` does not terminate TLS. Listeners speak plaintext MQTT (`mqtt://`)
and unencrypted WebSocket (`ws://`). Production deployments are expected to
terminate TLS in a sidecar / ingress / external L4 load balancer that
forwards plaintext traffic to the broker on the cluster network.

This document collects working configurations for the common terminators.
The chart does not ship a terminator — the right choice depends on what
you already run.

## Why not in-broker?

Three reasons.

1. **Cert rotation is somebody else's solved problem.** ingress-nginx,
   cert-manager, HAProxy + the SDS API, AWS NLB — all of these reload certs
   without dropping connections. Building that into `pgmqttd` would be a
   big chunk of code that an operator already has running for HTTP.
2. **L7 routing for `wss://` is free with any HTTPS ingress.** The same
   reverse proxy that handles `https://` for the rest of the cluster can
   pass `Upgrade: websocket` to the broker.
3. **mTLS at the broker collides with the User CRD model.** Authentication
   is username/password against the `users` table; client certs would need
   a parallel auth path. Out-of-scope for v1.

## Pattern A — `wss://` via standard HTTPS Ingress

Works with any Ingress controller that supports websocket upgrade.

```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: pgmqtt-wss
  namespace: mqtt
  annotations:
    nginx.ingress.kubernetes.io/proxy-read-timeout: "3600"
    nginx.ingress.kubernetes.io/proxy-send-timeout: "3600"
spec:
  ingressClassName: nginx
  tls:
    - hosts: ["mqtt.example.com"]
      secretName: pgmqtt-tls
  rules:
    - host: mqtt.example.com
      http:
        paths:
          - path: /mqtt
            pathType: Prefix
            backend:
              service:
                name: pgmqtt
                port:
                  number: 8083
```

Clients connect to `wss://mqtt.example.com/mqtt`. The websocket upgrade
forwards to `pgmqtt:8083` on the cluster network in plaintext.

## Pattern B — `mqtts://` via ingress-nginx `tcp-services`

For raw MQTT-over-TLS you need L4 (stream-mode) TLS termination.
ingress-nginx supports this via the `tcp-services` ConfigMap when you also
expose the corresponding port on the controller's Service.

Step 1 — controller `tcp-services` ConfigMap:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: tcp-services
  namespace: ingress-nginx
data:
  "8883": "mqtt/pgmqtt:1883"
```

Step 2 — open port 8883 on the ingress-nginx Service (Type=LoadBalancer or
NodePort). Note ingress-nginx in `tcp-services` mode does NOT terminate
TLS itself — it tunnels TCP. For TLS termination at this layer, prefer
HAProxy or an NLB with ACM in front. ingress-nginx + cert-manager works
only for L7 (`wss://`).

## Pattern C — HAProxy `frontend tls / backend tcp`

A bog-standard HAProxy (running as a Deployment in the same cluster, or as
a sidecar) does the TLS termination cleanly:

```haproxy
frontend mqtts
    bind :8883 ssl crt /etc/haproxy/certs/pgmqtt.pem alpn mqtt
    mode tcp
    default_backend pgmqtt

backend pgmqtt
    mode tcp
    option tcp-check
    server p1 pgmqtt.mqtt.svc.cluster.local:1883 check
```

The cert is delivered by mounting the cert-manager-issued Secret as a file
under `/etc/haproxy/certs/`.

## Pattern D — Cloud LB with ACM (AWS) / GCLB (GCP)

If you already terminate TLS at a cloud LB (e.g. AWS NLB with ACM), point
the LB target group at the `pgmqtt` Service's `mqtt` port. ALPN `mqtt` is
optional but recommended for cleaner client probes.

## Smoke-test from a client

```bash
# Pattern A (wss):
mosquitto_pub -h mqtt.example.com -p 443 \
  --capath /etc/ssl/certs/ \
  -u "$(kubectl -n mqtt get secret demo-mqtt-credentials -o jsonpath='{.data.username}' | base64 -d)" \
  -P "$(kubectl -n mqtt get secret demo-mqtt-credentials -o jsonpath='{.data.password}' | base64 -d)" \
  --tls-version tlsv1.2 \
  -L "wss://mqtt.example.com/mqtt" \
  -t test/topic -m hello

# Pattern B/C (mqtts on 8883):
mosquitto_pub -h mqtt.example.com -p 8883 \
  --capath /etc/ssl/certs/ \
  -u user -P pass \
  -t test/topic -m hello
```

## Scope reminder

These configurations are documented, not shipped. The chart's
`Values.service.type` defaults to `ClusterIP` precisely so an operator
can put their preferred terminator in front. If you need a one-stop helm
install with TLS baked in, raise an issue describing your terminator of
choice and we can discuss bundling it as an optional sidecar.
