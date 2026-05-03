# Web UI

`pgmqtt` does not ship its own web UI. The chart can optionally install
**[MQTTX Web](https://mqttx.app/web)** (EMQX's open-source MQTT 5 client)
as a sibling Deployment in the same namespace, behind its own ClusterIP
Service.

This document covers the install + access pattern. The UI is a static SPA
that runs entirely in the user's browser and connects to the broker over
the broker's WebSocket port (default 8083); the chart does not auto-mount
broker credentials into the page (browsers can't read Kubernetes Secrets).

What we verified about the image (used to pick the default values):

- Image: `emqx/mqttx-web:v1.13.0` (latest stable as of 2026-01-23).
- Multi-arch: `linux/amd64`, `linux/arm64` — runs on both x86 and Apple Silicon nodes.
- ~53 MB, exposes port 80, served by nginx; **no backend required**.
- Supports MQTT 3.1.1 and 5.0 natively, including WebSocket connections.

## Install

```bash
helm install pgmqtt deploy/helm/pgmqtt \
  --namespace mqtt --create-namespace \
  --set database.url='postgres://pgmqtt:pgmqtt@postgres.mqtt.svc:5432/pgmqtt?sslmode=disable' \
  --set ui.enabled=true
```

This creates two extra resources:
- `Deployment/pgmqtt-ui` (one replica of `emqx/mqttx-web`)
- `Service/pgmqtt-ui` (ClusterIP, port 80)

## Access

The UI is not exposed externally by default. Use `kubectl port-forward`
to reach it from your workstation:

```bash
kubectl -n mqtt port-forward svc/pgmqtt-ui 8888:80
# then visit http://localhost:8888
```

For longer-lived access, terminate TLS in front of the UI Service and
add an Ingress object — same pattern as exposing any other internal
HTTP service. **Do not expose this UI publicly without auth in front of
it.** It accepts arbitrary broker connection details, so if anyone can
reach it they can use it as a stepping-stone to test creds against
*any* MQTT broker.

## Connect to the broker

The UI's "New Connection" panel needs five things from your User CR's
auto-generated credentials Secret. Pull them with:

```bash
USER=demo  # the metadata.name of your User CR
kubectl -n mqtt get secret ${USER}-mqtt-credentials \
  -o go-template='Host:     {{`{{ .data.host | base64decode }}`}}{{"\n"}}Port:     {{`{{ index .data "ws-port" | base64decode }}`}}{{"\n"}}Path:     /mqtt{{"\n"}}Username: {{`{{ .data.username | base64decode }}`}}{{"\n"}}Password: {{`{{ .data.password | base64decode }}`}}{{"\n"}}'
```

Paste those into MQTTX Web. Protocol = `ws://`, MQTT Version = 5.0
(works for 3.1.1 too).

If you exposed `wss://` via a TLS-terminating ingress (see `docs/TLS.md`),
flip Protocol to `wss://` and Port to 443.

## Why not auto-mount credentials?

Two reasons:

1. **Browsers can't read kube Secrets.** The static SPA running in the
   browser would need a server-side shim to expose the Secret. Adding a
   shim means adding an authenticated proxy with its own access controls
   — which is the kind of thing a real cluster ingress already provides.
2. **Multi-user UIs need an identity story.** The User CR model is
   per-username; there's no logged-in-user concept in MQTTX Web that the
   shim could map onto.

If you want a one-click "open the UI and it knows who I am" experience,
the realistic path is OIDC ingress in front of the UI plus a small
auth-proxy that exchanges the OIDC subject for a User CR Secret. Out of
scope for the chart; raise an issue if you want to discuss.

## Alternatives evaluated

The TODO listed four candidates for the optional UI. Verdict:

| Candidate | Verdict |
| - | - |
| **MQTTX Web** (`emqx/mqttx-web`) | **Selected.** Commercial-quality v5 client, runs as static nginx, single image, no backend. |
| **mqtt-explorer** | Skip. Desktop-only (Electron); no docker/web build that's appropriate for in-cluster install. |
| **HiveMQ Web Client** | Skip. SaaS only; no self-hostable image. |
| Various **mqtt-web-client** forks | Skip. Unmaintained or single-author; no security track record. |
