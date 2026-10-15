# NATS JetStream → Kafka Sink Connector

A Go connector that pulls messages from a NATS JetStream stream, batches them, and produces them to Kafka. Designed to run as a single high-throughput consumer instance (scaled via CPU/memory limits rather than replica count) that binds to one durable JetStream pull consumer via `Consumer.Consume` and drains it as fast as the batching and Kafka settings allow.

## Features

- **Single durable JetStream pull consumer** — the connector creates (idempotently) one durable consumer directly on the source stream and drains it via [`Consumer.Consume`](https://pkg.go.dev/github.com/nats-io/nats.go/jetstream#Consumer), always with `AckPolicy=Explicit` so every message is acked, naked, or terminated individually based on the Kafka write's outcome.
- **Batched, concurrent produces** — batches up to `BATCH_MAX_MESSAGES` messages (or whatever arrives within `BATCH_MAX_WAIT`) and produces every record in the batch concurrently, waiting for all delivery confirmations (bounded by a fixed write timeout) before acking the underlying NATS messages.
- **Byte-for-byte pass-through** — this connector never decodes the message payload; the raw NATS message body becomes the Kafka record value unchanged.
- **At-least-once, with a partition-aware key strategy** — JetStream redelivery after a crash is expected, not a special case. The Kafka record key is derived from `STREAM_SUBJECTS`: the originating NATS subject when it can vary (`STREAM_SUBJECTS` contains a wildcard), or no key at all when every message already shares one literal subject — see [Kafka key strategy](#kafka-key-strategy--idempotency-posture) below for what this does and doesn't guarantee under redelivery.
- **Stream ownership stays external** — the connector never creates or modifies the source NATS stream itself. It verifies at startup that the configured stream exists and covers the expected subjects, and shuts down cleanly if not. (It *does* own the durable pull consumer created directly on top of that stream — see [`CONSUMER_NAME`](#nats--stream).)
- **Pluggable NATS authentication** — decentralized Nkey/JWT via a `.creds` file or its content inline (`JWT_NKEY_FILE`), or as a separate JWT and NKey seed pair (`JWT_NKEY`), a shared token, a NKey or username/password; unset (or `NONE`) for an unauthenticated connection.
- **Optional TLS and mutual TLS to NATS and to Kafka, and SASL/SCRAM to Kafka** — using the same env variables and values as the Kafka source connector, so one ConfigMap/Secret can serve both.
- **Kubernetes-friendly by default** — `/healthz` and `/readyz` HTTP endpoints, graceful shutdown that drains an in-flight batch before exiting, and every setting is an env var (no flags, no config file).

The connector batches whatever messages it's handed, produces the batch to Kafka in one concurrent call, and then acks (on success), naks with backoff (on a retryable failure), or terminates (after `MAX_DELIVER` attempts) each message individually based on the result.

## Prerequisites

- Go 1.26 or later (only needed for building from source — see [Running with Docker](#running-with-docker) to avoid a local Go install)
- A NATS server or cluster with JetStream enabled
- The target **stream already created** on that server (see [`STREAM_NAME`](#nats--stream) below — this connector does not create the source stream, only the durable consumer on top of it)
- A reachable Kafka cluster and an existing target topic

## Getting started (local)

```bash
cp .env.example .env
```

Fill in at least `NATS_URL`, `STREAM_NAME`, `STREAM_SUBJECTS`, `CONSUMER_NAME`, `KAFKA_BOOTSTRAP_SERVERS`, and `KAFKA_TOPIC` — see the [configuration reference](#configuration-reference) for what each one means and what happens if it's left out.

```bash
go run .
```

`.env` is loaded automatically on startup as a local-dev convenience (via [godotenv](https://github.com/joho/godotenv)) and is never required in production — see the note at the top of the reference table below.

## Configuration reference

Every setting is an environment variable. There are no command-line flags and no config file. Variables marked **Required** cause the connector to exit immediately at startup with a descriptive error if unset — see [`internal/config/natsSourceConfig.go`](internal/config/natsSourceConfig.go) (NATS/stream/consumer-group/batching settings) and [`internal/config/kafkaSinkConfig.go`](internal/config/kafkaSinkConfig.go) (Kafka settings) for the exact validation; shared env-parsing helpers live in [`internal/config/utils`](internal/config/utils/envUtils.go).

> **A note on `.env` vs. real deployments**: `.env` (loaded via `godotenv`) is a local-development convenience only. It is not required, and a missing or unreadable `.env` file is never treated as an error — in Kubernetes, these variables are expected to come from a `ConfigMap`/`Secret` via the Pod spec instead (see [`yamls/1-configmap.yaml`](yamls/1-configmap.yaml)).

### NATS & stream

| Variable | Required | Default | Description |
|---|---|---|---|
| `NATS_URL` | Yes | — | NATS server URL(s), e.g. `nats://localhost:4222` or `tls://...`. Comma-separate multiple URLs for a cluster. |
| `NATS_CLIENT_NAME` | No | `consumer-service` | The client name this connection reports to NATS (visible in server monitoring, e.g. `/connz`). Consider templating this per-pod (e.g. via the Downward API) to tell replicas apart in monitoring. |
| `STREAM_NAME` | Yes | — | Name of the JetStream stream to consume from. **This stream must already exist** — it's expected to be provisioned by a separate owner/process (e.g. a management UI or IaC pipeline). The connector only looks it up; if it doesn't exist, the connector logs a clear error and exits non-zero rather than creating it. |
| `STREAM_SUBJECTS` | Yes | — | Comma-separated subject(s) this connector expects to consume, e.g. `orders.created,orders.updated`. Used to validate that `STREAM_NAME` actually covers what's expected (supports NATS wildcards on either side of the comparison), and also determines the Kafka key strategy — see [Kafka key strategy](#kafka-key-strategy--idempotency-posture). If the stream doesn't cover a listed subject, the connector exits at startup with an error naming the mismatch. |
| `CONSUMER_NAME` | Yes | — | Name of the single **durable JetStream pull consumer** the connector binds to via `Consumer.Consume`. Unlike the source stream, this consumer *is* owned and managed by the connector — created (and kept up to date) idempotently on startup via `CreateOrUpdateConsumer`, directly on `STREAM_NAME`, filtered to `STREAM_SUBJECTS`, always with `AckPolicy=Explicit`. |

### NATS authentication (optional)

Opt-in via `NATS_AUTH_MECHANISM` — leaving it unset makes no change to the connection (anonymous).

| Variable | Required | Default | Description |
|---|---|---|---|
| `NATS_AUTH_MECHANISM` | No | *(none)* | Selects how the connector authenticates to NATS: `JWT_NKEY_FILE` (decentralized Nkey/JWT auth via a `.creds` file), `JWT_NKEY` (decentralized Nkey/JWT auth via a separate JWT and NKey seed), `NKEY` (raw NKey seed, no JWT), `TOKEN` (a single shared token), or `USERNAME_PASSWORD` (username/password). Leave unset, or set explicitly to `NONE`, for an unauthenticated connection — any other value is rejected at startup. |
| `NATS_CREDS_FILE` | Only for `JWT_NKEY_FILE`\* | *(none)* | Path to a `.creds` file for NKey/JWT authentication — prefer mounting this from a Secret over embedding credentials elsewhere. Rejected at startup if set while `NATS_AUTH_MECHANISM` isn't `JWT_NKEY_FILE`. |
| `NATS_CREDS` | Only for `JWT_NKEY_FILE`\* | *(none)* | The `.creds` file's content supplied inline instead of a file path (e.g. from a Secret exposed via `secretKeyRef`). |
| `NATS_JWT` | Only if `NATS_AUTH_MECHANISM=JWT_NKEY` | *(none)* | The user JWT, supplied on its own instead of a combined `.creds` file. Required together with `NATS_NKEY`. |
| `NATS_NKEY` | Only if `NATS_AUTH_MECHANISM=JWT_NKEY` | *(none)* | The user NKey seed paired with `NATS_JWT` — prefer sourcing this from a Secret. |
| `NATS_TOKEN` | Only if `NATS_AUTH_MECHANISM=TOKEN` | — | The shared token to authenticate with. |
| `NATS_USERNAME` | Only if `NATS_AUTH_MECHANISM=USERNAME_PASSWORD` | — | NATS username. |
| `NATS_PASSWORD` | Only if `NATS_AUTH_MECHANISM=USERNAME_PASSWORD` | — | NATS password — prefer sourcing this from a Secret. |
| `NATS_USER_SEED` | Only if `NATS_AUTH_MECHANISM=NKEY` | — | The raw NKey seed used to sign the connect challenge directly, with no JWT involved — prefer sourcing this from a Secret. |

\* `NATS_AUTH_MECHANISM=JWT_NKEY_FILE` requires at least one of `NATS_CREDS_FILE` or `NATS_CREDS`. If both are set, `NATS_CREDS_FILE` takes precedence and `NATS_CREDS` is ignored. `NATS_AUTH_MECHANISM=JWT_NKEY` is the same decentralized Nkey/JWT auth, but with the JWT and NKey seed supplied as two separate values (`NATS_JWT`/`NATS_NKEY`) instead of one combined `.creds` file — both are required. `NATS_AUTH_MECHANISM` itself is compared case-sensitively against these exact uppercase values (`JWT_NKEY_FILE`, `JWT_NKEY`, `NKEY`, `TOKEN`, `USERNAME_PASSWORD`) and `NONE` — lowercase values such as `none` or `token` are rejected.

### NATS TLS (optional)

Opt-in via `NATS_TLS_ENABLED` — leaving it unset (or `false`) changes nothing about the connection.

| Variable | Required | Default | Description |
|---|---|---|---|
| `NATS_TLS_ENABLED` | No | `false` | Master switch for TLS to NATS. Parsed as a general boolean (`true`/`false`, `TRUE`, `1`, etc.). |
| `NATS_TLS_CA` | Only if `NATS_TLS_ENABLED=true` | *(none)* | A private/self-signed CA certificate to trust, as inline PEM content. Required when TLS is enabled — there's no file-path variant. |
| `NATS_TLS_MODE` | No | *(none)* | Set to `ONE_WAY_TLS` for server-only TLS, or set to `MUTUAL_TLS` to also present a client certificate (`NATS_TLS_CERT`/`NATS_TLS_KEY`) to NATS. `MUTUAL_TLS` requires `NATS_TLS_ENABLED=true`; any other value is rejected at startup. **This is the switch that actually applies the client certificate** — `NATS_TLS_CERT`/`NATS_TLS_KEY` set without `NATS_TLS_MODE=MUTUAL_TLS` are ignored, with a warning logged at startup. |
| `NATS_TLS_CERT` | Only if `NATS_TLS_MODE=MUTUAL_TLS` | *(none)* | Client certificate for mutual TLS, as inline PEM content. Required (together with `NATS_TLS_KEY`) when `NATS_TLS_MODE=MUTUAL_TLS`. |
| `NATS_TLS_KEY` | Only if `NATS_TLS_MODE=MUTUAL_TLS` | *(none)* | Client private key for mutual TLS, paired with `NATS_TLS_CERT` — prefer sourcing this from a Secret. |
| `NATS_TLS_INSECURE_SKIP_VERIFY` | No | `false` | Skips server certificate verification. **Development/testing only** — never enable this against a real deployment. |

### Batching & consumer flow control

| Variable | Required | Default | Description |
|---|---|---|---|
| `BATCH_MAX_MESSAGES` | No | `1000` | A batch is flushed once it holds this many messages or `BATCH_MAX_WAIT` elapses, whichever comes first; also sizes the Kafka producer's in-flight record buffer, since at most one such batch is ever outstanding per pod. |
| `BATCH_MAX_WAIT` | No | `2s` | Maximum time a partially-filled batch is held before flushing anyway, so low-traffic periods don't stall pending messages indefinitely. Go [duration syntax](https://pkg.go.dev/time#ParseDuration) (e.g. `500ms`, `2s`, `1m`). |
| `ACK_WAIT` | No | `60s` | How long JetStream waits for an ack before considering a message eligible for redelivery. Should comfortably exceed your worst-case time to produce a full batch to Kafka — too short causes spurious redeliveries of messages that are still being processed. |
| `MAX_DELIVER` | No | `5` | Maximum redelivery attempts for a message before it's terminated (dropped, logged, not redelivered again) instead of retried forever. Protects against a single poison message looping indefinitely. |
| `MAX_ACK_PENDING` | No | `50000` | Maximum unacknowledged messages JetStream will hand out on the durable consumer before it stops delivering more. Must be at least `BATCH_MAX_MESSAGES`, or a single in-flight batch could exceed it and stall delivery. Since this connector is designed to run as a single high-throughput instance, size it generously — it's the ceiling on how much can be in flight at once, tuned alongside the pod's CPU/memory limits rather than a replica count. |
| `SHUTDOWN_GRACE_PERIOD` | No | `25s` | On `SIGINT`/`SIGTERM`, how long an in-flight batch is given to finish producing and acking before the process exits. Keep this comfortably below your Pod's `terminationGracePeriodSeconds`, or Kubernetes will `SIGKILL` mid-flush. |
| `NATS_DELIVER_POLICY` | No | `all` | JetStream delivery policy for a newly-created consumer. `all` starts at the stream's first message. `last` starts at the newest message and continues live. `new` delivers only messages that arrive after the consumer is created. |

### Kafka connection

| Variable | Required | Default | Description |
|---|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | Yes | — | Comma-separated Kafka broker address(es), e.g. `broker1:9092,broker2:9092`. |
| `KAFKA_TOPIC` | Yes | — | Target topic. Every NATS message becomes one Kafka record here. |
| `KAFKA_SECURITY_PROTOCOL` | No | `NONE` | How the connector authenticates to Kafka: `NONE` (no authentication) or `SASL_PLAINTEXT` (SASL/SCRAM, requires `KAFKA_USERNAME`/`KAFKA_PASSWORD`). Compared case-insensitively (normalized to uppercase); any other value is rejected at startup. |
| `KAFKA_SCRAM_AUTH_MECHANISM` | No | `SHA512` | SCRAM mechanism for `SASL_PLAINTEXT`: `SHA256` or `SHA512`. Compared case-insensitively. |
| `KAFKA_USERNAME` | Only if `KAFKA_SECURITY_PROTOCOL=SASL_PLAINTEXT` | — | SASL/SCRAM username. |
| `KAFKA_PASSWORD` | Only if `KAFKA_SECURITY_PROTOCOL=SASL_PLAINTEXT` | — | SASL/SCRAM password — prefer sourcing this from a Secret. |

### Kafka TLS (optional)

Opt-in via `KAFKA_TLS_ENABLED` — leaving it unset (or `false`) makes no change to the connection. TLS is independent of `KAFKA_SECURITY_PROTOCOL`: `SASL_PLAINTEXT` with TLS enabled carries SASL over an encrypted connection (Kafka's `SASL_SSL`), and `NONE` with TLS enabled is server-only TLS with no SASL layer (Kafka's `SSL`).

| Variable | Required | Default | Description |
|---|---|---|---|
| `KAFKA_TLS_ENABLED` | No | `false` | Master switch for TLS to Kafka. Parsed as a general boolean (`true`/`false`, `TRUE`, `1`, etc.). |
| `KAFKA_TLS_CA` | No | *(none)* | A private/self-signed CA certificate to trust, as inline PEM content (there's no file-path variant). Leaving it unset falls back to the system's default CA trust store — the common case for managed Kafka (Confluent Cloud, MSK, Aiven, etc.) with publicly-signed broker certificates. |
| `KAFKA_TLS_MODE` | No | *(none)* | Leave unset for server-only TLS, or set to `MUTUAL_TLS` to also present a client certificate (`KAFKA_TLS_CERT`/`KAFKA_TLS_KEY`) to the brokers. Any other value is rejected at startup. |
| `KAFKA_TLS_CERT` | Only if `KAFKA_TLS_MODE=MUTUAL_TLS` | *(none)* | Client certificate for mutual TLS, as inline PEM content. Required together with `KAFKA_TLS_KEY`. |
| `KAFKA_TLS_KEY` | Only if `KAFKA_TLS_MODE=MUTUAL_TLS` | *(none)* | Client private key for mutual TLS, as inline PEM content — prefer sourcing this from a Secret (e.g. via `secretKeyRef`). |
| `KAFKA_TLS_INSECURE_SKIP_VERIFY` | No | `false` | Disables broker certificate verification. Only ever intended for testing against a self-signed dev cluster — never enable this in production, as it makes the connection vulnerable to on-path tampering. |

`KAFKA_TLS_CERT`/`KAFKA_TLS_KEY` set without `KAFKA_TLS_MODE=MUTUAL_TLS` are ignored, with a warning logged at startup. Any `KAFKA_TLS_*` setting (CA, mode, cert or key) while `KAFKA_TLS_ENABLED` is not true is rejected at startup, rather than silently connecting without TLS. PEM values may contain escaped `\n` sequences in place of real newlines.

### Observability & runtime

| Variable | Required | Default | Description |
|---|---|---|---|
| `HEALTH_ADDR` | No | `:8080` | Address the health HTTP server listens on — see [Health & readiness endpoints](#health--readiness-endpoints). |
| `PPROF_ENABLED` | No | `false` | Exposes Go's profiling endpoints under `/debug/pprof/` on the health server and turns on block/mutex profiling. Opt-in only — the profiles expose process memory and add overhead; see [`registerPprof`](internal/health/health.go). |
| `LOG_LEVEL` | No | `info` | Accepted and validated, but **not yet wired to actual log verbosity** — the connector currently logs unconditionally via the standard `log` package regardless of this value. Reserved for when leveled logging is added. |

## Health & readiness endpoints

The connector exposes a small HTTP server (see [`internal/health`](internal/health/health.go)) on `HEALTH_ADDR`:

- **`/healthz`** — liveness. Returns `200` once the process has started. Use for a `livenessProbe`.
- **`/readyz`** — readiness. Returns `200` only when the NATS connection is up *and* the Kafka client reports itself ready (a broker `Ping`); otherwise `503`. Use for a `readinessProbe`.

Because this is a pull-based consumer rather than a request/response service, readiness doesn't gate any inbound traffic — it mainly protects rollout safety (don't consider a new replica "up" mid-deployment if it can't yet reach NATS or Kafka) and feeds anything inspecting pod health.

## Running with Docker

The included [`Dockerfile`](Dockerfile) is a multi-stage, non-root, multi-architecture build:

```bash
docker build -t kafka-sink-connector:local .
docker run --rm --env-file .env -p 8080:8080 kafka-sink-connector:local
```

## Deploying to Kubernetes

Reference manifests are in [`yamls/`](yamls/) (kept out of version control — see `.gitignore` — so treat them as a starting point, not a source of truth).

`KAFKA_USERNAME`/`KAFKA_PASSWORD` currently live in the ConfigMap in this reference setup — for a real deployment, source them from a Secret instead so credentials aren't stored or transmitted in plain text. The target `STREAM_NAME` must already exist in NATS with `STREAM_SUBJECTS` covered before the Deployment will start successfully — see [NATS & stream](#nats--stream) above.

## Kafka key strategy & idempotency posture

JetStream pull consumers are **at-least-once**: the pod can crash after producing a batch to Kafka but before acking NATS, causing that batch to be redelivered and re-produced once it's back up. Kafka is an append-only log rather than a keyed store, so this connector cannot make a redelivery land as a harmless overwrite — it can only make duplicates cheap to detect or eliminate downstream:

- Every record's Kafka key is derived from `STREAM_SUBJECTS` at startup (see [`KafkaKeyStrategy`](internal/config/kafkaSinkConfig.go)): `SUBJECT_NAME` (key = the message's own NATS subject) whenever `STREAM_SUBJECTS` contains a wildcard, otherwise no key at all (`NULL_KEY`) — keying a single, always-identical literal subject would just pile every message onto one partition for no benefit.
- A record's key alone only becomes a true "latest value wins" upsert if the target topic is configured with `cleanup.policy=compact` — otherwise it's just useful for partition affinity and manual dedup.
- Every record also carries an `Nats-Message-Sequence` header (plus a copy of the original NATS message headers), so a duplicate can always be identified and filtered downstream even without compaction.
- Idempotency at the produce layer itself is limited to what the underlying Kafka client's idempotent producer gives for free — deduping retries of the *same* send via a broker-tracked sequence number. It does not protect against the crash-before-ack scenario above, which is a genuine re-produce of a "new" send from the broker's perspective.

If you need exactly-once semantics end to end, that has to be built downstream (compaction, a dedup consumer, or transactional consumption keyed on `Nats-Message-Sequence`) — this connector's guarantee stops at "at-least-once, with the tools to dedup."
