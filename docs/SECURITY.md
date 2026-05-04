# Security threat model

What `pgmqttd` defends against, what it deliberately doesn't, and
where operators are expected to plug the gaps with infrastructure
(TLS terminator, ingress controller, NetworkPolicy).

## Trust boundaries

```
                ┌──────────────────────┐
                │  Cluster operator    │  Trusted: applies CRDs, sets env vars,
                │  (kubectl/helm)      │  picks the postgres connection string.
                └──────────┬───────────┘
                           │
                           ▼
   ┌──────────────────────────────────────────────────┐
   │  Kubernetes API server                            │
   │  + cert-manager / Secret store                    │  Trusted infra.
   └──────────────────────────┬───────────────────────┘
                              │
                              │ (Service mesh / NetworkPolicy enforce)
                              │
   ┌────────┐    TLS   ┌──────┴───────────┐  TCP    ┌──────────────────┐
   │ Client │◀────────▶│  L4/L7 terminator │◀──────▶│  pgmqttd Pod(s)  │
   │ (mqtt) │           │  (haproxy/ingress)│         │ + listener       │
   └────────┘           └───────────────────┘         └──────┬───────────┘
       Untrusted                                              │
       (random internet                                        │ pgxpool +
        or your IoT fleet)                                    │ LISTEN/NOTIFY
                                                              │
                                                       ┌──────▼─────────┐
                                                       │  Postgres       │
                                                       │  (operator-    │
                                                       │   provisioned)  │
                                                       └─────────────────┘
```

**Trusted:** Kubernetes API + the Postgres database + the cluster
operator. Anything that has a cluster role to apply User CRDs or read
the broker's database can extract every credential the broker knows.

**Untrusted:** the MQTT client. We assume an attacker on the public
internet who can dial the listener and try anything they want.

## What the broker enforces

### Authentication

- CONNECT requires a `username + password` unless
  `PGMQTT_ALLOW_ANONYMOUS=true`. We discourage anonymous mode in
  production; it exists for test rigs.
- Passwords are checked with `bcrypt.CompareHashAndPassword` against
  the `users.password_hash` column. Default cost is 10
  (`bcrypt.DefaultCost`); configurable up to 31 via
  `PGMQTT_BCRYPT_COST`. Bcrypt 10 is ~70 ms per check on modern
  hardware — slow enough to make brute-force online attacks
  expensive (`MaxConnections` per Pod also caps the rate).
- A failed CONNECT returns CONNACK reason 0x86 (Bad Username or
  Password) and closes the socket. No timing-channel leak between
  "user doesn't exist" and "wrong password" — both go through the
  bcrypt compare path.

### Per-conn limits

- `PGMQTT_MAX_CONNECTIONS` (default 5000 per Pod) — over-cap accepts
  emit CONNACK 0x9F and close before processing CONNECT.
- `PGMQTT_MAX_INBOUND_MSGS_PER_SEC` (default 1000) — per-conn
  token bucket on PUBLISH/SUBSCRIBE; trip → DISCONNECT 0x96.
- `PGMQTT_MAX_QUEUED_DELIVERIES_PER_CLIENT` (default 10000) — slow
  subscribers DISCONNECT 0x97 once their queue saturates.
- v5 `ServerReceiveMaximum` (default 100) — too many un-ACKed inbound
  QoS>0 PUBLISHes → DISCONNECT 0x93 (Receive Maximum Exceeded).

### Resource hygiene

- Inbound topic aliases are rejected (`ServerTopicAliasMaximum=0`
  default). A v5 client that tries one gets DISCONNECT 0x94.
- Maximum keepalive caps client-supplied keepalive at
  `PGMQTT_KEEPALIVE_MAX_SEC` (default 60 s) so a client can't sit
  idle for hours holding a slot.
- Janitor sweeps:
    * `inbound_qos2` rows older than 1 h for disconnected sessions
      (prevents tombstone accumulation from a v5 client that sends
      QoS-2 PUBLISH but never PUBREL).
    * Orphan `messages` older than `orphanGrace` (default 10 min)
      with no referencing `deliveries`.

## What the broker explicitly DOES NOT enforce

These are infrastructure responsibilities, by design:

1. **Transport confidentiality.** `pgmqttd` listens in plaintext on
   `:1883` (mqtt) and `:8083` (ws). Production deployments terminate
   TLS in front. See `docs/TLS.md` for four working patterns.
2. **Topic-level authorization (ACLs).** A successfully-authenticated
   user can publish/subscribe to any topic. If you need per-user
   ACLs, put a policy proxy in front (e.g. a custom plugin to a
   different broker, or a sidecar that vets topics against the
   user). ACLs are explicitly out of scope per the v1 plan.
3. **Client certificate authentication (mTLS).** Same reasoning as
   ACLs — the User CR model is username/password, no parallel
   identity. Use TLS-PSK or a translating proxy if mTLS is required.
4. **Anti-replay or message signing.** MQTT is not a delivery-receipt
   protocol; if you need non-repudiation, sign payloads at the
   application layer.
5. **DDoS mitigation at L4.** A determined attacker can saturate the
   ingress layer's TCP accept queue independent of `MaxConnections`.
   Use a CDN / anti-DDoS service / network ACLs as the first line.

## Recommendations for production

1. **Always front pgmqttd with TLS.** No exceptions. `mqtt://` over a
   public network is a credential-harvesting trap; use `mqtts://` or
   `wss://`.
2. **Restrict ingress by source.** Set
   `Values.networkPolicy.enabled=true` (or your CNI's equivalent) to
   limit who can reach the broker ports.
3. **Restrict egress to Postgres.** The same NetworkPolicy template
   has an `egress.postgres` block; populate it with a selector that
   matches your Postgres pods. Prevents a compromised broker from
   exfiltrating to arbitrary endpoints.
4. **Rotate user passwords on a schedule.** Update the User CR's
   referenced Secret; the reconciler picks it up and re-hashes. The
   old hash is overwritten in `users.password_hash`.
5. **Bcrypt cost ≥ 12 if hardware allows.** Default 10 is fine for
   homelab; if your auth-failed rate is in the thousands per second
   from a brute-force attempt, jump to 12 or 13. Each +1 doubles
   compute time.
6. **Watch `pgmqtt_rate_limited_total` and `pgmqtt_quota_exceeded_total`.**
   Sustained increases indicate either a misbehaving client or an
   attack; alert at the metric layer.
7. **Avoid `PGMQTT_ALLOW_ANONYMOUS=true` in production.** It's a
   testing convenience that should never reach a non-isolated
   network.

## Secrets handling

- The broker reads `PGMQTT_DATABASE_URL` from environment. In Helm,
  `Values.database.existingSecret` lets you reference an existing
  Secret containing the URL key — preferred over `Values.database.url`,
  which embeds the password in plaintext in the Helm release values.
- User-CR-generated `<name>-mqtt-credentials` Secrets contain the raw
  password. They are owned by the User CR (deletion of the User
  cascades to Secret deletion). Store them as `corev1.SecretTypeOpaque`,
  not as ConfigMaps.
- Bcrypt-hashed passwords in `users.password_hash` are *not* a substitute
  for protecting the database — anyone who can read the table can
  brute-force the hashes offline. Restrict DB access at the
  Postgres-role level (`pgmqtt` role gets only the privileges the
  broker actually needs; no `SUPERUSER`).

## Operator RBAC trade-off

The pgmqtt operator's ClusterRole grants
`get/list/watch/create/update/patch` on `secrets` cluster-wide. This is
deliberate and operators should understand the implication before
deploying.

**What's permitted.** The broker Pod's ServiceAccount can read and
write any Secret in any namespace on the cluster — not just Secrets in
its own namespace.

**Why we accept it.** The whole point of the User CR is that operators
put them anywhere they want — per-app, per-team, per-tenant. A User CR
in namespace `team-alpha` causes the operator to create a
`<name>-mqtt-credentials` Secret in `team-alpha` so the team's apps
can mount it. Restricting Secret RBAC to the operator's own namespace
would silently break every cross-namespace User reconcile: the
operator would log an authorization error and the Secret would never
appear next to the workload that needs it.

**Threat.** A compromised `pgmqttd` container — RCE in the MQTT
parser, malicious image swap, supply-chain compromise — can exfiltrate
every Secret cluster-wide. That includes `kube-system` ServiceAccount
tokens, CNPG superuser credentials, sealed-secrets controller
outputs, TLS bundles, container-registry pull secrets, and anything
else the cluster stores as a Secret. The broker has no business
reading any of these, but the API permission grant doesn't
distinguish.

**Mitigations.** Pick the ones appropriate to your threat model:

1. **Dedicated namespace + NetworkPolicy egress denylist.** Deploy
   pgmqtt in its own namespace and apply a NetworkPolicy that allows
   egress only to (a) the Kubernetes API server, (b) the Postgres
   service, (c) any TLS terminator in front. Blocks data exfiltration
   to attacker-controlled endpoints even if the Secret read succeeds.
2. **Projected ServiceAccount tokens with short audience+expiration.**
   Mount the operator's API credential as a projected token (audience
   bound to `kubernetes.default.svc`, expirationSeconds ≈ 3600) rather
   than the legacy long-lived Secret-backed token. Limits the blast
   radius of a leaked token to one hour.
3. **Per-namespace Roles for single-namespace deployments.** If you
   genuinely run all User CRs in one namespace (the operator's), fork
   the chart's RBAC template to use a `Role` + `RoleBinding` instead
   of `ClusterRole` + `ClusterRoleBinding`. This is *not* the default
   because it breaks the documented multi-tenant pattern, but it's a
   reasonable hardening choice for a homelab where every User lives
   alongside the broker.

We don't ship option 3 by default because the silent-failure mode for
cross-namespace Users is the worse outcome: a User CR sits Pending
forever with no visible error in the namespace the operator owns.

## Reporting a vulnerability

This is a personal/homelab-grade project. Email the maintainer
(public address in the GitHub repo) with the details; do not file a
public issue for unfixed vulnerabilities.
