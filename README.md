# twingate-log-shipper

A lightweight Python service that captures Twingate connector real-time analytics from stdout and ships batched, gzip-compressed NDJSON to any S3-compatible storage endpoint.

> **Experimental / Unsupported.** This project is published by the Twingate Solutions team as a community resource. It is not an official Twingate product and carries no support or SLA. Use at your own discretion.

---

## How It Works

The Twingate connector emits analytics events to stdout when `TWINGATE_LOG_ANALYTICS=v2` is set. The log shipper collects those events, buffers them into batches, and uploads compressed NDJSON files to S3-compatible storage.

```
Twingate Connector
  stdout: ANALYTICS {...}
        |
        v
  Log Shipper
  ┌─────────────────────────────────────┐
  │  Collector  →  Batcher  →  Shipper  │
  │  (docker or    (NDJSON    (S3 upload │
  │   journald)     + gzip)   + retry)  │
  └─────────────────────────────────────┘
        |
        v
  S3-Compatible Storage
  (AWS S3, MinIO, Cloudflare R2, Backblaze B2, ...)
```

**Three deployment modes:**

| Mode | Description |
|---|---|
| `docker` (sidecar) | Shipper container runs alongside a single connector in the same Compose stack or ECS/ACS task |
| `docker` (host-level) | One shipper per Docker host, watching all connector containers by name filter |
| `journald` | Systemd service on the same host as a systemd-managed Twingate connector |

The shipper filters only lines starting with `ANALYTICS `, strips the prefix, parses the JSON, and ignores all other connector output.

---

## Quickstart — Docker Sidecar

Use this when you run a single Twingate connector in Docker and want the shipper alongside it in the same Compose stack.

**1. Create a `.env` file:**

```dotenv
# Twingate connector credentials
TWINGATE_NETWORK=acme
TWINGATE_ACCESS_TOKEN=your-access-token
TWINGATE_REFRESH_TOKEN=your-refresh-token

# S3 destination
S3_BUCKET=your-bucket-name
S3_ACCESS_KEY_ID=your-access-key-id
S3_SECRET_ACCESS_KEY=your-secret-access-key

# Optional overrides
# S3_ENDPOINT_URL=https://your-custom-endpoint   # leave unset for AWS S3
# S3_REGION=us-east-1
# BATCH_INTERVAL=60
```

> **Note on variable naming:** The `.env` file uses short names (e.g. `S3_BUCKET`) that `docker-compose.sidecar.yml` maps to the full `TWINGATE_SHIPPER_*` environment variables inside the container. If you run the container directly with `docker run`, use the full `TWINGATE_SHIPPER_*` prefix instead (e.g. `TWINGATE_SHIPPER_S3_BUCKET`).

**2. Start the stack:**

```bash
docker compose -f docker-compose.sidecar.yml up -d
```

**3. Check logs:**

```bash
docker compose -f docker-compose.sidecar.yml logs -f twingate-log-shipper
```

> **Note:** Twingate tokens are visible via `docker inspect` to anyone with Docker access. For production deployments, prefer Docker Secrets or a secrets manager such as AWS Secrets Manager with ECS task definitions.

---

## Quickstart — Docker Host-Level

Use this when you want a single shipper per Docker host watching all Twingate connector containers (matched by container name containing `twingate`).

**1. Create a `.env` file:**

```dotenv
# S3 destination
S3_BUCKET=your-bucket-name
S3_ACCESS_KEY_ID=your-access-key-id
S3_SECRET_ACCESS_KEY=your-secret-access-key

# Optional overrides
# S3_ENDPOINT_URL=https://your-custom-endpoint   # leave unset for AWS S3
# S3_REGION=us-east-1
# BATCH_INTERVAL=60
```

> **Note on variable naming:** The `.env` file uses short names (e.g. `S3_BUCKET`) that `docker-compose.host.yml` maps to the full `TWINGATE_SHIPPER_*` environment variables inside the container. If you run the container directly with `docker run`, use the full `TWINGATE_SHIPPER_*` prefix instead (e.g. `TWINGATE_SHIPPER_S3_BUCKET`).

**2. Start the shipper:**

```bash
docker compose -f docker-compose.host.yml up -d
```

**3. Check logs:**

```bash
docker compose -f docker-compose.host.yml logs -f twingate-log-shipper
```

The shipper mounts `/var/lib/docker/containers` read-only from the host and tails log files for all containers whose name contains `twingate`. Make sure each connector container has `TWINGATE_LOG_ANALYTICS=v2` set — see [Enabling Analytics](#enabling-analytics-on-the-connector) below.

---

## Quickstart — systemd

Use this when the Twingate connector is managed by systemd (not Docker).

**1. Install the package:**

```bash
sudo python3.12 -m venv /opt/twingate-log-shipper/venv
sudo /opt/twingate-log-shipper/venv/bin/pip install "git+https://github.com/twingate-solutions/twingate-connector-log-shipper.git"
```

**2. Create the service user:**

```bash
sudo useradd -r -s /usr/sbin/nologin twingate-shipper
sudo usermod -aG systemd-journal twingate-shipper
```

**3. Create the credentials file:**

```bash
sudo mkdir -p /etc/twingate
sudo tee /etc/twingate/log-shipper.env > /dev/null <<EOF
TWINGATE_SHIPPER_MODE=journald
TWINGATE_SHIPPER_S3_BUCKET=your-bucket-name
TWINGATE_SHIPPER_S3_ACCESS_KEY_ID=your-access-key-id
TWINGATE_SHIPPER_S3_SECRET_ACCESS_KEY=your-secret-access-key
# TWINGATE_SHIPPER_S3_ENDPOINT_URL=https://your-custom-endpoint
# TWINGATE_SHIPPER_S3_REGION=us-east-1
# TWINGATE_SHIPPER_BATCH_INTERVAL_SECONDS=60
# TWINGATE_SHIPPER_LOG_LEVEL=INFO
EOF

sudo chmod 600 /etc/twingate/log-shipper.env
sudo chown root:twingate-shipper /etc/twingate/log-shipper.env
```

**4. Install and start the service:**

```bash
sudo cp systemd/twingate-log-shipper.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now twingate-log-shipper
```

**5. Check status:**

```bash
sudo systemctl status twingate-log-shipper
sudo journalctl -u twingate-log-shipper -f
```

---

## Enabling Analytics on the Connector

The connector must have `TWINGATE_LOG_ANALYTICS=v2` set to emit analytics events.

**Docker (add to your connector's environment):**

```yaml
environment:
  TWINGATE_LOG_ANALYTICS: "v2"
```

This is already set in `docker-compose.sidecar.yml`. For host-level Docker deployments, add it to each connector container's environment manually or via your orchestration platform.

**systemd (add to the connector's unit override or environment file):**

```bash
sudo systemctl edit twingate-connector.service
```

Add:

```ini
[Service]
Environment="TWINGATE_LOG_ANALYTICS=v2"
```

Then reload and restart the connector:

```bash
sudo systemctl daemon-reload
sudo systemctl restart twingate-connector.service
```

---

## Configuration Reference

All settings are provided via environment variables prefixed with `TWINGATE_SHIPPER_`. No configuration files are read for secrets.

| Variable | Default | Description |
|---|---|---|
| `TWINGATE_SHIPPER_MODE` | `auto` | Collector mode: `auto`, `docker`, or `journald`. `auto` tries Docker first, then journald. |
| `TWINGATE_SHIPPER_DOCKER_LOG_PATH` | `/var/lib/docker/containers` | Path to Docker container log directory (Docker mode only). |
| `TWINGATE_SHIPPER_DOCKER_CONTAINER_NAME_FILTER` | `twingate/connector` | Substring matched against the container name or image reference. Default matches any container running the `twingate/connector` image regardless of container name. |
| `TWINGATE_SHIPPER_JOURNALD_UNIT` | `twingate-connector.service` | systemd unit name to read from (journald mode only). |
| `TWINGATE_SHIPPER_S3_ENDPOINT_URL` | _(none)_ | Custom S3 endpoint URL. Leave unset for AWS S3. Set for MinIO, Cloudflare R2, Backblaze B2, DigitalOcean Spaces, etc. |
| `TWINGATE_SHIPPER_S3_BUCKET` | _(required)_ | S3 bucket name. |
| `TWINGATE_SHIPPER_S3_PREFIX` | `twingate-analytics` | Key prefix (folder) within the bucket. |
| `TWINGATE_SHIPPER_S3_REGION` | `us-east-1` | AWS/provider region. Required for AWS S3; may be required by some S3-compatible providers. |
| `TWINGATE_SHIPPER_S3_ACCESS_KEY_ID` | _(required)_ | S3 access key ID. |
| `TWINGATE_SHIPPER_S3_SECRET_ACCESS_KEY` | _(required)_ | S3 secret access key. |
| `TWINGATE_SHIPPER_BATCH_INTERVAL_SECONDS` | `60` | Maximum seconds between batch uploads. Range: 10–3600. |
| `TWINGATE_SHIPPER_BATCH_MAX_EVENTS` | `10000` | Maximum events per batch. Flush triggered when reached. |
| `TWINGATE_SHIPPER_BATCH_MAX_BYTES` | `10485760` | Maximum uncompressed batch size in bytes (default 10 MB). Flush triggered when reached. |
| `TWINGATE_SHIPPER_UPLOAD_MAX_RETRIES` | `3` | Maximum S3 upload retry attempts before discarding the batch. Range: 0–10. |
| `TWINGATE_SHIPPER_UPLOAD_BACKOFF_BASE` | `2.0` | Exponential backoff base (seconds) between upload retries. |
| `TWINGATE_SHIPPER_UPLOAD_BACKOFF_MAX` | `60.0` | Maximum backoff delay (seconds) between upload retries. |
| `TWINGATE_SHIPPER_SHUTDOWN_TIMEOUT_SECONDS` | `30` | Seconds to wait for in-flight batch upload to complete on SIGTERM/SIGINT. |
| `TWINGATE_SHIPPER_LOG_LEVEL` | `INFO` | Log level: `DEBUG`, `INFO`, `WARNING`, `ERROR`, or `CRITICAL`. |

---

## S3 Key Format

Uploaded files use the following key structure:

```
{prefix}/{YYYY}/{MM}/{DD}/{HH}-{MM}_{uuid8}.ndjson.gz
```

**Example:**

```
twingate-analytics/2024/01/15/12-00_a3f7b2c1.ndjson.gz
```

- `prefix` is set by `TWINGATE_SHIPPER_S3_PREFIX` (default: `twingate-analytics`)
- Date and time components (`YYYY`, `MM`, `DD`, `HH`, `MM`) reflect the UTC time the batch was closed
- `uuid8` is an 8-character hex string from a random UUID, ensuring uniqueness across restarts and parallel instances
- Files are gzip-compressed NDJSON; each line is a single analytics event JSON object

---

## License

Apache 2.0. See [LICENSE](LICENSE).

This project is published by the Twingate Solutions team as a community resource. It is experimental and unsupported — not an official Twingate product.
