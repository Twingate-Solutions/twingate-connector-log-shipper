# Architecture

This document describes the internal design of twingate-log-shipper: how each
component works, how they connect to each other, and how the service starts up,
processes data, and shuts down cleanly.

---

## Overview

The service is a single Python asyncio process. Three components run as
concurrent tasks wired together by bounded `asyncio.Queue` instances:

```
Twingate Connector stdout
         │
         ▼
  ┌─────────────┐   asyncio.Queue[dict | None]   ┌─────────────┐
  │  Collector  │  ──────────────────────────►  │   Batcher   │
  │             │        maxsize=10,000          │             │
  └─────────────┘                               └──────┬──────┘
                                                       │
                                      asyncio.Queue[tuple[Path, str] | None]
                                               maxsize=1,000
                                                       │
                                               ┌───────▼──────┐
                                               │   Shipper    │
                                               │              │
                                               └──────┬───────┘
                                                      │
                                                      ▼
                                              S3-compatible storage
```

- The **event queue** (capacity 10,000) carries parsed event dicts from the
  Collector to the Batcher. A `None` sentinel signals the Batcher to flush and
  stop.
- The **upload queue** (capacity 1,000) carries `(local_path, s3_key)` tuples
  from the Batcher to the Shipper. A `None` sentinel signals the Shipper to
  drain and stop.

Back-pressure is built in: if the Batcher falls behind, the Collector blocks on
`event_queue.put()` rather than buffering unboundedly in memory.

---

## Collector

### Interface (`collectors/base.py`)

`BaseCollector` is an abstract base class with two methods that all
implementations must provide:

- `events() -> AsyncGenerator[dict, None]` — yields one parsed analytics event
  dict per `ANALYTICS ` line. Implementations must filter only lines starting
  with `ANALYTICS `, strip the prefix (10 characters including the trailing
  space), parse the remainder as JSON, log WARN and skip malformed JSON, and
  never raise an exception that terminates the generator.
- `close() -> None` — releases any held resources (file handles, journal
  reader).

### DockerCollector (`collectors/docker.py`)

Used when the shipper runs as a Docker container on the host or as a sidecar.

**Container discovery.** `_find_container_log()` walks the Docker containers
directory (default `/var/lib/docker/containers`). For each subdirectory it
reads `config.v2.json` and checks whether the filter string (case-insensitive)
is a substring of either the container's `Name` field **or** its image reference
(`Config.Image`). Checking both fields means the default filter `"twingate/connector"`
reliably matches any container running the official connector image regardless of
what the user has named the container or service.
The first match whose log file exists is returned. The log file path is
`<containers_dir>/<container_id>/<container_id>-json.log`.

**Tailing.** The collector opens the log file in text mode (UTF-8, errors
replaced) and seeks to the end with `f.seek(0, 2)`, so historical log entries
are not replayed on startup. It then reads one line at a time with
`f.readline()`.

**EOF and rotation handling.** When `readline()` returns an empty string the
collector has reached EOF. It checks whether the inode of the file on disk
(`log_path.stat().st_ino`) still matches the inode recorded when the file was
opened (`os.fstat(f.fileno()).st_ino`). If the inodes differ (or the file no
longer exists), Docker has rotated the log: the collector closes the handle,
clears all state, and re-runs container discovery from scratch. If the inodes
match, it is a normal EOF (no new data yet) and sleeps for 0.5 s before
polling again.

**ANALYTICS filtering.** Each raw line read from the file is a JSON object
written by the Docker log driver, of the form:

```json
{"log": "ANALYTICS {...}\n", "stream": "stdout", "time": "..."}
```

The collector extracts the inner `"log"` string, strips its trailing newline,
and checks whether it starts with `"ANALYTICS "`. Lines that do not start with
this prefix are silently discarded. Lines that do are stripped of the prefix
and parsed as JSON to produce the event dict yielded to the queue.

**If no matching container is found** at startup the collector logs an error
and retries every 5 s.

### JournaldCollector (`collectors/journald.py`)

Used when the shipper runs as a systemd service on the host alongside a
connector that is itself managed by systemd.

**Dependency.** Requires the `systemd-python` package (`pip install
twingate-log-shipper[journald]`), which requires `libsystemd` and is only
available on Linux. If the package is not installed, the constructor raises
`RuntimeError` immediately.

**Reader setup.** A `systemd.journal.Reader` is created, filtered to
`INFO`-level and above, and further filtered to entries from the configured
unit (`_SYSTEMD_UNIT` match, default `twingate-connector.service`). The reader
seeks to the tail with `reader.seek_tail()` followed by `reader.get_previous()`
to consume the tail marker, so only new journal entries are processed.

**Polling.** The main loop calls `reader.process()`. If the return value is
`systemd.journal.APPEND`, new entries are available and are iterated
immediately. Otherwise the loop sleeps for 0.5 s before polling again.

**ANALYTICS filtering.** Each journal entry's `MESSAGE` field is checked for
the `"ANALYTICS "` prefix using the same logic as the Docker collector.

### Mode auto-detection (`main.py`)

When `TWINGATE_SHIPPER_MODE=auto` (the default), `_create_collector()` tries
sources in order:

1. **Docker** — if the directory at `docker_log_path` (default
   `/var/lib/docker/containers`) exists, a `DockerCollector` is returned
   immediately.
2. **journald** — if the Docker directory does not exist, `systemctl is-active
   <journald_unit>` is run with a 5 s timeout. If the return code is 0, a
   `JournaldCollector` is returned.
3. **Failure** — if neither condition is met, a `RuntimeError` is raised with
   a message explaining what was checked and advising the operator to set
   `TWINGATE_SHIPPER_MODE` explicitly.

When mode is set explicitly to `"docker"` or `"journald"`, the corresponding
collector is instantiated directly without any probing.

---

## Batcher

`Batcher` (`batcher.py`) accumulates parsed event dicts into an in-memory list
and flushes them to a gzipped NDJSON temp file when any one of three thresholds
is reached:

| Threshold | Default | Environment variable |
|---|---|---|
| Time since last flush | 60 s | `TWINGATE_SHIPPER_BATCH_INTERVAL_SECONDS` |
| Event count | 10,000 events | `TWINGATE_SHIPPER_BATCH_MAX_EVENTS` |
| Serialised bytes | 10,485,760 (10 MB) | `TWINGATE_SHIPPER_BATCH_MAX_BYTES` |

**Event loop.** On each iteration the batcher calculates the remaining time
until the interval threshold and calls `asyncio.wait_for(event_queue.get(),
timeout=remaining)`. If the timeout fires and the batch is non-empty, a flush
is triggered immediately. If an event arrives it is serialised to a compact
JSON string (`json.dumps` with `separators=(",", ":")`), appended to the batch
list, and the running byte total is updated (accounting for the trailing `\n`).
After each append the count and byte thresholds are checked.

**Flush.** All accumulated lines are joined with `\n` and terminated with a
final `\n` to produce valid NDJSON. The bytes are gzip-compressed and written
to a temp file (created with `tempfile.mkstemp`, prefix `twingate_batch_`,
suffix `.ndjson.gz`) using `asyncio.to_thread` to avoid blocking the event
loop.

**S3 key format.** The key is generated at flush time from the UTC wall clock:

```
<s3_prefix>/<YYYY>/<MM>/<DD>/<HH>-<MM>_<8-hex-chars>.ndjson.gz
```

For example, with the default prefix `twingate-analytics`, a batch flushed at
14:07 UTC on 2 April 2025 with UUID fragment `a3f1c2b4` would produce:

```
twingate-analytics/2025/04/02/14-07_a3f1c2b4.ndjson.gz
```

The 8-character hex UUID suffix prevents key collisions when multiple batches
flush within the same minute.

**Shutdown.** When a `None` sentinel is received from the event queue, any
remaining events are flushed before the `run()` coroutine returns.

---

## Shipper

`Shipper` (`shipper.py`) consumes `(local_path, s3_key)` tuples from the
upload queue and uploads each temp file to S3 using `aiobotocore`.

**S3 client.** An `aiobotocore` S3 client is created once for the lifetime of
`run()` using the configured region, credentials, and optional `endpoint_url`.
The `endpoint_url` parameter enables compatibility with any S3-compatible
storage (MinIO, Cloudflare R2, Backblaze B2, DigitalOcean Spaces, GCS
interoperability, etc.). botocore retry handling is disabled (`max_attempts=1`)
because the shipper manages its own retry loop.

**Upload headers.** Each `put_object` call sets:
- `ContentEncoding: gzip`
- `ContentType: application/x-ndjson`

**Retry logic.** Uploads are attempted up to `upload_max_retries + 1` times
(default 3 retries, 4 total attempts). The backoff between attempts is:

```
delay = min(backoff_base ** attempt + random.uniform(0, 1), backoff_max)
```

With defaults (`backoff_base=2.0`, `backoff_max=60.0`) this gives approximately
1–2 s, 2–3 s, 4–5 s before the final attempt, each with up to 1 s of random jitter.

Retry is applied to:
- HTTP 429, 500, 502, 503, 504 responses
- All non-`ClientError` exceptions (network errors, timeouts)

The following S3 error codes are treated as fatal and are **not** retried:
`AccessDenied`, `NoSuchBucket`, `InvalidAccessKeyId`. Any other
non-retryable status code also causes an immediate abort.

**Discard on exhaustion.** If all retry attempts are exhausted, the batch is
logged at ERROR level (`batch_upload_failed_discard`) and dropped. This is
intentional: analytics data is treated as best-effort and the service should
not block indefinitely.

**Temp file cleanup.** The local temp file is always deleted after the upload
attempt completes, regardless of success or failure, using
`asyncio.to_thread(local_path.unlink)` with `OSError` suppressed.

**Shutdown.** When `None` is received from the upload queue, `run()` returns.
Any in-flight items that were already dequeued before the sentinel will have
completed their upload (or retry cycle) before the sentinel is reached.

---

## Graceful shutdown

The shutdown sequence is orchestrated in `_async_main()` in `main.py`. Signal
handlers are registered for `SIGTERM` and `SIGINT` using
`loop.add_signal_handler` (Linux/macOS only — this is intentional, as the
service is designed to run on Linux).

**Sequence:**

1. A signal arrives (`SIGTERM` or `SIGINT`). The signal handler calls
   `shutdown_event.set()`.
2. The main coroutine was waiting on `asyncio.wait([producer_task,
   shutdown_waiter], return_when=FIRST_COMPLETED)`. The `shutdown_waiter` task
   completes, cancelling the other pending waiter.
3. The producer task is cancelled and awaited with a 2 s timeout.
4. `collector.close()` is called to release the file handle or journal reader.
5. `None` is put on `event_queue`. The Batcher receives it, flushes any
   accumulated events to a temp file, and returns.
6. `None` is put on `upload_queue`. The Shipper receives it after draining
   any previously queued uploads, and returns.
7. Each of steps 5 and 6 is given `shutdown_timeout_seconds / 2` seconds
   (default 15 s each). If either times out, a warning is logged and the exit
   code is set to 1.
8. The process exits with the final exit code (0 on clean shutdown, 1 on any
   timeout or unexpected producer exit).

If the producer task exits unexpectedly without a signal, the main coroutine
detects this (`not shutdown_event.is_set()`), logs an error, and proceeds
through the same shutdown sequence with exit code 1.

---

## Configuration

All configuration is provided through environment variables prefixed with
`TWINGATE_SHIPPER_`. There are no configuration files. Secrets (S3 credentials)
are never written to disk.

`ShipperConfig` in `config.py` uses `pydantic-settings` (`BaseSettings`) to
validate and parse the environment. Required fields (`s3_bucket`,
`s3_access_key_id`, `s3_secret_access_key`) have no default and will cause
startup to fail with a validation error if not set.

| Environment variable | Type | Default | Description |
|---|---|---|---|
| `TWINGATE_SHIPPER_MODE` | `str` | `auto` | Collector mode: `auto`, `docker`, or `journald` |
| `TWINGATE_SHIPPER_DOCKER_LOG_PATH` | `str` | `/var/lib/docker/containers` | Docker containers directory |
| `TWINGATE_SHIPPER_DOCKER_CONTAINER_NAME_FILTER` | `str` | `twingate` | Substring matched against container names |
| `TWINGATE_SHIPPER_JOURNALD_UNIT` | `str` | `twingate-connector.service` | systemd unit to follow |
| `TWINGATE_SHIPPER_S3_ENDPOINT_URL` | `str \| None` | `None` (AWS S3) | Custom S3-compatible endpoint URL |
| `TWINGATE_SHIPPER_S3_BUCKET` | `str` | **required** | Target S3 bucket |
| `TWINGATE_SHIPPER_S3_PREFIX` | `str` | `twingate-analytics` | S3 key prefix |
| `TWINGATE_SHIPPER_S3_REGION` | `str` | `us-east-1` | AWS region (used even for non-AWS endpoints) |
| `TWINGATE_SHIPPER_S3_ACCESS_KEY_ID` | `str` | **required** | S3 access key ID |
| `TWINGATE_SHIPPER_S3_SECRET_ACCESS_KEY` | `str` | **required** | S3 secret access key |
| `TWINGATE_SHIPPER_BATCH_INTERVAL_SECONDS` | `int` (10–3600) | `60` | Max batch age before flush |
| `TWINGATE_SHIPPER_BATCH_MAX_EVENTS` | `int` (≥100) | `10000` | Max events per batch |
| `TWINGATE_SHIPPER_BATCH_MAX_BYTES` | `int` (≥1048576) | `10485760` | Max uncompressed bytes per batch |
| `TWINGATE_SHIPPER_UPLOAD_MAX_RETRIES` | `int` (0–10) | `3` | Retry attempts per upload |
| `TWINGATE_SHIPPER_UPLOAD_BACKOFF_BASE` | `float` (≥1.0) | `2.0` | Exponential backoff base (seconds) |
| `TWINGATE_SHIPPER_UPLOAD_BACKOFF_MAX` | `float` (≥5.0) | `60.0` | Maximum backoff duration (seconds) |
| `TWINGATE_SHIPPER_SHUTDOWN_TIMEOUT_SECONDS` | `int` | `30` | Total graceful shutdown budget |
| `TWINGATE_SHIPPER_LOG_LEVEL` | `str` | `INFO` | Log level: DEBUG, INFO, WARNING, ERROR, CRITICAL |

---

## Deployment modes

The same Python codebase supports three deployment scenarios. The only
difference is which collector is used and how the process accesses the connector
logs.

| Mode | Collector | How logs are accessed | Typical use |
|---|---|---|---|
| Docker sidecar | `DockerCollector` | Shared `logs` volume or `--volumes-from` the connector container. The connector writes to stdout; Docker captures it to a JSON log file in the shared volume. | ECS task definition, ACS container group, Kubernetes pod sidecar |
| Docker host-level | `DockerCollector` | `/var/lib/docker/containers` mounted read-only from the host (`ro` bind mount). The shipper scans all container directories and matches by name. | Single host running one or more Twingate connector containers |
| systemd service | `JournaldCollector` | systemd journal, accessed via `systemd.journal.Reader` in-process. No volume mounts required. | Linux host running the Twingate connector as a systemd unit |

The connector must be started with `TWINGATE_LOG_ANALYTICS=v2` in all three
cases. Without this environment variable the connector does not emit
`ANALYTICS ` lines and no events will be collected.
