# Directory Exporter

A Prometheus exporter that monitors filesystem directories and exposes file counts and modification timestamps as metrics. Designed to detect stuck queues, halted ingest pipelines, and inaccessible directories — without modifying the applications that write to those directories.

---

## What Problem It Solves

File-based pipelines — log shippers, stream buffers, ETL queues, media ingest — share the same failure modes:

| Failure | Symptom on disk | Without this exporter |
|---|---|---|
| Consumer stopped | Files pile up, count keeps growing | Silent until disk fills |
| Producer stopped | No new files arriving | Silent until downstream notices |
| Permission error | Directory becomes unreadable | Silent until manual check |
| Stuck file | One very old file never moves | Invisible in standard monitoring |

Application metrics and logs tell you what the app *thinks* is happening. Directory Exporter tells you what is *actually on disk* — a ground-truth signal that requires zero cooperation from the monitored applications.

---

## How It Works

```
┌─────────────────────────────────────────────────────────────────┐
│                        Container                                │
│                                                                 │
│  ┌──────────────┐  scan every      ┌─────────────────────────┐ │
│  │  Scan Loop   │──120s (default)──▶  scanDir() per directory │ │
│  │              │                  │  1. ReadDir (getdents)   │ │
│  │  Discovery   │──every 6h────────▶     → exact file count  │ │
│  │  Loop        │                  │  2. lstat up to N files  │ │
│  └──────┬───────┘                  │     → oldest/newest mtime│ │
│         │ results                  └────────────┬────────────┘ │
│         ▼                                       │              │
│  ┌──────────────┐◀────────────────────────────── ┘             │
│  │  In-Memory   │                                              │
│  │  Cache       │  (metrics served from here — zero disk I/O) │
│  └──────┬───────┘                                              │
│         ▼                                                       │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  HTTP Server  :9200                                       │  │
│  │  GET  /metrics      →  Prometheus text format            │  │
│  │  GET  /-/healthy    →  liveness probe (always 200)       │  │
│  │  GET  /-/ready      →  readiness probe (503 on cold start)│  │
│  │  POST /-/reload     →  re-discover + re-scan (authed)    │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                 │
│  Volume mount (read-only)                                       │
│  /streams  ←──────────── host: /your/actual/path               │
└─────────────────────────────────────────────────────────────────┘
```

**Two-phase scan design:**

1. **Count phase** — reads all directory entries via `getdents` (kernel call, no per-file I/O). File count is always exact and fast.
2. **Stat phase** — calls `lstat` on up to `max_stat_files` entries to get modification times. Capped to keep scans fast on large directories. When capped, `directory_scan_truncated` is set to `1`.

Metrics are always served from the in-memory cache. The `/metrics` endpoint never touches disk — response time is sub-millisecond regardless of directory size or file count.

---

## Metrics Reference

All per-directory metrics carry `base`, `stream`, and `type` labels derived from the path:

```
/streams/orders/buffer  →  base="/streams"  stream="orders"  type="buffer"
/streams/logs           →  base="/streams"  stream="logs"    type=""
```

| Metric | Type | Description |
|---|---|---|
| `directory_cache_ready` | Gauge | `1` after first scan completes, `0` during cold start |
| `directory_last_scan_timestamp_seconds` | Gauge | Unix timestamp of the last completed scan cycle |
| `directory_watched_total` | Gauge | Number of directories currently being monitored |
| `directory_scan_errors_total` | Counter | Cumulative scan errors (permission denied, vanished dir, etc.) |
| `directory_reload_total` | Counter | Cumulative `/-/reload` calls received |
| `directory_file_count` | Gauge | Total regular files in the directory — always exact, non-recursive |
| `directory_oldest_file_timestamp_seconds` | Gauge | mtime of the oldest file — detects stuck queues |
| `directory_newest_file_timestamp_seconds` | Gauge | mtime of the newest file — detects halted ingest |
| `directory_scrape_duration_seconds` | Gauge | Wall-clock time of the last scan for this directory |
| `directory_scrape_success` | Gauge | `1` = readable, `0` = permission error or directory vanished |
| `directory_scan_truncated` | Gauge | `1` if stat phase was capped by `max_stat_files`; timestamps are approximate |

### Recommended Alert Rules

```yaml
# Consumer down — queue growing
- alert: DirectoryQueueGrowing
  expr: directory_file_count > 10000
  for: 10m

# Producer down — no new files
- alert: DirectoryIngestHalted
  expr: time() - directory_newest_file_timestamp_seconds > 3600
  for: 5m

# Stuck file — oldest file hasn't moved in 2 hours
- alert: DirectoryStuckFile
  expr: time() - directory_oldest_file_timestamp_seconds > 7200
  and directory_file_count > 0

# Directory disappeared or permission error
- alert: DirectoryScrapeDown
  expr: directory_scrape_success == 0
  for: 1m

# Exporter scan loop stalled
- alert: DirectoryExporterCacheStale
  expr: time() - directory_last_scan_timestamp_seconds > 300
  for: 5m
```

---

## Components

```
Exporter/
├── main.go           Startup, signal handling, graceful shutdown
├── config.go         Config loading: defaults → YAML file → env vars
├── watchlist.go      Directory discovery (auto and explicit), thread-safe WatchList
├── scanner.go        Low-level dir scan: getdents counting + capped lstat timestamps
├── exporter.go       Scan loop, discovery loop, parallel worker pool
├── cache.go          Thread-safe in-memory metrics store (atomic counters, RWMutex)
├── metrics.go        Prometheus text format renderer (zero allocations on scrape path)
├── server.go         HTTP handlers: /metrics, /-/reload, /-/healthy, /-/ready
│
├── targets.yml           ← Edit this: what to monitor on this deployment
├── .env                  ← Edit this: port, secret, paths for this server
│
├── docker-compose.yml            Standalone deployment (no external dependencies)
├── docker-compose.monitoring.yml Override to join an existing Prometheus/Grafana stack
├── Dockerfile                    Two-stage build → scratch image (~9 MB, zero OS)
│
├── targets.example.yml   Annotated config template — copy to targets.yml
└── .env.example          Annotated env template — copy to .env
```

---

## Configuration

Configuration loads in three layers — each overrides the previous:

```
1. Built-in defaults
       ↓
2. targets.yml  (path set via CONFIG_FILE env var)
       ↓
3. Individual environment variables
```

### `targets.yml` — Primary Config File

Mount this file into the container and edit it to control what is monitored. No rebuild required — trigger a reload after editing.

```yaml
# NOTE: Do NOT put reload_secret here.
# targets.yml must be world-readable by the container process (UID 65534).
# Set RELOAD_SECRET in .env instead — that file is owner-only (600).

# Scan tuning (all optional — defaults shown)
scan_interval:      "120s"   # how often to scan all directories
scan_workers:       2         # parallel scan workers
scan_timeout:       "90s"    # abort a scan cycle after this long
discovery_interval: "6h"     # how often to re-discover new subdirectories

# Stat cap: number of lstat() calls per directory for timestamp computation.
# 0 = stat every file (accurate but slow on large dirs).
# 5000 = fast; timestamps reflect only the first 5k files (sets scan_truncated=1).
max_stat_files:    5000
max_files_per_dir: 0         # 0 = count every file (always exact)

targets:
  # Explicit list — only these subdirs are monitored (recommended for production)
  - base: /streams
    dirs:
      - buffer
      - discarded
      - rejected

  # Deeper path — labels become stream="logs", type="nginx"
  - base: /streams
    dirs:
      - logs/nginx
      - logs/app

  # Auto-discover — finds all subdirs up to max_depth levels deep
  # Good for dev/test environments where directories change often
  - base: /data
    max_depth: 1

  # Multiple base paths on different mounts
  - base: /mnt/nas/archive
    dirs:
      - 2024
      - 2025
```

**Important:** `base` values must match the container-side mount paths declared in `docker-compose.yml`.

### `.env` — Per-Deployment Settings

```env
CONTAINER_NAME=directory-exporter

# Secret for /-/reload. Leave blank to auto-generate (logged at startup).
RELOAD_SECRET=your-secret-here

# Host port for the metrics endpoint. Change this if 9200 is already in use.
PORT=9200

# Name of the external Docker network created by your Prometheus/Grafana stack.
# Only needed when using docker-compose.monitoring.yml.
MONITORING_NETWORK=monitoring_monitoring

# Host path to your targets.yml (mounted read-only into the container)
CONFIG_FILE_HOST=./targets.yml

# Host path(s) for your data directories — one per base declared in targets.yml
DATA_PATH_1=/your/actual/data/path

# Scan tuning overrides (optional — set in targets.yml instead when possible)
SCAN_INTERVAL=120s
MAX_STAT_FILES=5000

# Resource limits
CPU_LIMIT=0.25
MEMORY_LIMIT=128M
GOMEMLIMIT=96MiB    # Go runtime soft cap — must use MiB/GiB suffix, not M/G
```

### All Environment Variables

| Variable | Default | Description |
|---|---|---|
| `CONFIG_FILE` | — | Path inside container to `targets.yml` |
| `RELOAD_SECRET` | auto-generated | Bearer token for `POST /-/reload` |
| `LISTEN_ADDR` | `:9200` | Address the HTTP server binds to |
| `SCAN_INTERVAL` | `120s` | How often to scan all directories |
| `SCAN_WORKERS` | `2` | Parallel scan workers |
| `SCAN_TIMEOUT` | `90s` | Max wall-clock time per scan cycle before aborting |
| `DISCOVERY_INTERVAL` | `6h` | How often to re-discover subdirectories automatically |
| `MAX_STAT_FILES` | `5000` | Max `lstat` calls per directory (0 = unlimited) |
| `MAX_FILES_PER_DIR` | `0` | Max files counted per directory (0 = unlimited) |
| `BASE_PATH` | — | **Legacy.** Single base path without a config file |
| `MAX_DEPTH` | `1` | **Legacy.** Auto-discovery depth when using `BASE_PATH` |

---

## Deployment

### Requirements

- Docker and Docker Compose v2
- Host directories to monitor

---

### Option 1 — Standalone (any server, no existing monitoring stack)

```bash
# 1. Copy the Exporter/ directory to the server

# 2. Create your env file
cp .env.example .env
# Edit .env: set PORT, DATA_PATH_1, RELOAD_SECRET

# 3. Create your targets config
cp targets.example.yml targets.yml
# Edit targets.yml: set base paths and dirs to monitor

# 4. Start
docker compose up -d --build

# 5. Verify
curl http://localhost:9200/-/ready     # → Ready
curl http://localhost:9200/metrics     # → Prometheus metrics
```

Prometheus scrape config:
```yaml
scrape_configs:
  - job_name: directory-exporter
    static_configs:
      - targets: ["<server-ip>:9200"]
```

---

### Option 2 — Integrated with Existing Prometheus/Grafana Stack

The monitoring overlay adds the exporter to your existing Docker network so Prometheus can reach it by container name without exposing a port to the host.

```bash
# Confirm the name of your monitoring network
docker network ls | grep monitoring

# Set it in .env
MONITORING_NETWORK=monitoring_monitoring   # or whatever yours is called

# Start with both compose files
docker compose -f docker-compose.yml -f docker-compose.monitoring.yml up -d --build
```

Prometheus scrape config (container DNS — no host port needed):
```yaml
scrape_configs:
  - job_name: directory-exporter
    static_configs:
      - targets: ["directory-exporter:9200"]
```

---

### Option 3 — Image Registry (build once, deploy everywhere)

Build the image once and push it to a registry. Target servers only need `.env` and `targets.yml` — no source code, no Go toolchain.

```bash
# Build and push from your build machine
docker build -t ghcr.io/your-org/directory-exporter:latest .
docker push ghcr.io/your-org/directory-exporter:latest

# On each target server — replace the build: block in docker-compose.yml with:
#   image: ghcr.io/your-org/directory-exporter:latest
docker compose up -d
```

---

### Option 4 — Transfer Image File (air-gapped servers)

```bash
# Export on your build machine
docker save directory-exporter:latest | gzip > directory-exporter.tar.gz

# Transfer the file, then on the target server
docker load < directory-exporter.tar.gz
docker compose up -d    # no --build needed
```

---

### New Server Checklist

- [ ] Copy `docker-compose.yml`, `docker-compose.monitoring.yml`, `Dockerfile`
- [ ] `cp .env.example .env` → set `PORT`, `DATA_PATH_1`, `RELOAD_SECRET`
- [ ] `cp targets.example.yml targets.yml` → set `base` paths and `dirs`
- [ ] If monitoring multiple base paths: add a volume entry per base in `docker-compose.yml`
- [ ] `docker compose up -d --build`
- [ ] `curl http://localhost:<PORT>/-/ready` → `Ready`
- [ ] `curl http://localhost:<PORT>/metrics` → check `directory_cache_ready 1`

---

## Operations

### Reload config without restarting

After editing `targets.yml`, trigger a reload:

```bash
curl -X POST \
  -H "Authorization: Bearer your-secret" \
  http://localhost:9200/-/reload
# → 202 Accepted (scan runs asynchronously)
```

This re-discovers directories and runs a full scan. The container keeps serving stale-but-valid metrics from the cache during the reload.

### Health checks

```bash
curl http://localhost:9200/-/healthy   # liveness:  200 OK if process is alive
curl http://localhost:9200/-/ready     # readiness: 200 OK after first scan completes
```

### Logs

```bash
docker logs directory-exporter -f
docker logs directory-exporter --tail 50
```

Logs are structured JSON. Key entries:

| Message | Meaning |
|---|---|
| `target configured` | Each target printed at startup — confirms config was read |
| `target validated` | Base path is accessible and readable |
| `watch list updated` | Discovery found N directories to monitor |
| `scan cycle complete` | Normal healthy operation |
| `scan truncated` | `max_stat_files` cap hit — file count is exact but timestamps are approximate |
| `scan cycle timed out` | Scan exceeded `scan_timeout` — partial results written |
| `target base path not accessible` | Volume mount is missing or path is wrong |
| `RELOAD_SECRET not configured` | Secret was auto-generated — **not logged**; set it in `.env` to make it stable |

---

## Tuning for Large Directories

When directories contain hundreds of thousands of files, two settings control the speed/accuracy trade-off:

| Setting | Fast (default) | Accurate |
|---|---|---|
| `max_stat_files` | `5000` | `0` (unlimited) |
| File count | Always exact | Always exact |
| Timestamps (oldest/newest) | Approximate — first 5k files only | Exact — all files statted |
| `directory_scan_truncated` | `1` | `0` |
| Scan time (333k files, SSD) | ~4–7 seconds | ~10–20 minutes |

If you need accurate timestamps on large directories, increase `max_stat_files` and adjust `scan_timeout` and `scan_interval` proportionally:

```yaml
max_stat_files:  0        # stat every file
scan_timeout:    "30m"    # must be longer than the expected scan time
scan_interval:   "35m"    # must be longer than scan_timeout
```

---

## The Docker Image

Built `FROM scratch` — no OS, no shell, no package manager, zero CVE surface from OS packages.

```
Image size: ~9 MB
Contents:   /directory-exporter   (single static Go binary)
```

The image contains only the binary. Config and data are always external:

| What | Where |
|---|---|
| Targets config | `targets.yml` mounted as `/config/targets.yml` |
| Data to monitor | Host directories mounted as read-only volumes |
| Secrets / ports | `.env` file read by Docker Compose at startup |

The image is identical across all deployments. Only `.env` and `targets.yml` differ per server.

---

## Security

### Threat Model

The exporter runs as a read-only observer. It never writes to the directories it monitors. The only writable surfaces are:

- **`POST /-/reload`** — triggers a re-scan. Protected by a Bearer token.
- **`GET /metrics`** — read-only scrape endpoint. No authentication by default.
- **`/config/targets.yml`** — mounted read-only into the container.

The main risks are: secret leakage, unauthorised access to the metrics endpoint (which discloses filesystem paths), and container escape leading to host-level access.

---

### Hardening Applied

The following issues were found during a security audit and fixed:

| Finding | Severity | Fix applied |
|---|---|---|
| Process ran as **root (UID 0)** — no `USER` in Dockerfile | Critical | `USER 65534:65534` added to Dockerfile; verified `Uid: 65534` at runtime |
| `GONOSUMDB='*'` in Dockerfile — bypassed Go checksum database for all packages | High | Removed; build now uses `-mod=readonly` with pinned `go.sum` hashes |
| Port bound to **`0.0.0.0`** — metrics endpoint reachable from any network interface | High | Changed to `127.0.0.1:${PORT}` — localhost-only by default |
| `reload_secret` in `targets.yml` — which must be **world-readable** by the container | High | Removed; secret lives only in `.env` (`chmod 600`, owner-read only) |
| **Auto-generated secret printed in plaintext** in structured logs | Medium | Log message now emits a hint only — the secret value is never logged |
| **No request body size limit** on `/-/reload` — goroutine exhaustion possible | Medium | `http.MaxBytesReader(1024)` added to the reload handler |
| `.env` and `targets.yml` had world-readable permissions (`664`) | Medium | `.env` → `600` (owner only); `targets.yml` → `644` (no secrets inside) |
| **No `.gitignore`** — `.env` and `targets.yml` could be committed with secrets | Medium | `.gitignore` created, excludes both files and the compiled binary |

Items that were already correct and verified:

| Control | How it works |
|---|---|
| `cap_drop: ALL` | All Linux capabilities dropped from the container |
| `no-new-privileges: true` | The process cannot gain new capabilities via setuid/setgid |
| `read_only: true` | Container root filesystem is mounted read-only |
| Constant-time secret comparison | `subtle.ConstantTimeCompare` — no timing side-channel on the reload token |
| Reload flood protection | `reloadCh` is buffered at 1; excess triggers are dropped silently |
| Symlink traversal | `WalkDir` uses `lstat` — symlinks to directories are never followed |
| Memory and CPU limits | cgroup limits enforced via `deploy.resources` in compose |
| Read-only volume mount | Data directories mounted `:ro` — exporter cannot write to monitored data |

---

### Remaining Risks

These require infrastructure changes rather than code changes.

**1. No TLS — the Bearer token travels in plaintext**

`POST /-/reload` sends the secret as an HTTP Authorization header. On a trusted Docker network (container-to-container) this is generally acceptable. When calling from outside the host, put a TLS-terminating reverse proxy in front:

```
# Caddy — automatic HTTPS, minimal config
directory-exporter.internal {
    reverse_proxy 127.0.0.1:9200
}
```

```
# nginx
server {
    listen 443 ssl;
    ssl_certificate     /etc/ssl/certs/exporter.crt;
    ssl_certificate_key /etc/ssl/private/exporter.key;
    location / { proxy_pass http://127.0.0.1:9200; }
}
```

**2. `/metrics` has no authentication**

The scrape endpoint is open — anyone who can reach `127.0.0.1:9200` can read the metrics, including the base paths and directory names of the monitored filesystem. The localhost-only port binding (fix #3 above) limits this to local processes on the host.

If the host has untrusted local users, or if you expose this port to a network, add authentication at the reverse proxy layer:

```
# Caddy with basic auth
directory-exporter.internal {
    basicauth {
        prometheus $2a$14$<bcrypt-hash>
    }
    reverse_proxy 127.0.0.1:9200
}
```

Or configure Prometheus to scrape via the Docker internal network only (integrated mode with `docker-compose.monitoring.yml`) and never expose the port to the host at all.

---

### Secret Management

**Where the reload secret lives:**

```
.env  (chmod 600, owner-read only)
  └── RELOAD_SECRET=your-secret   ← set this
        │
        ▼ injected as environment variable at container start
  container process (UID 65534)
        │
        ▼ held in process memory only
  compared via subtle.ConstantTimeCompare on each POST /-/reload
```

The secret is **never**:
- Written to `targets.yml` (world-readable)
- Printed in logs (Docker log JSON files are readable by any docker user)
- Stored in the container filesystem

**Generating a strong secret:**

```bash
openssl rand -hex 32
# or
python3 -c "import secrets; print(secrets.token_hex(32))"
```

Paste the output into `.env` as `RELOAD_SECRET=<value>`.

---

### File Permission Reference

| File | Permission | Why |
|---|---|---|
| `.env` | `600` | Contains `RELOAD_SECRET` — owner-read only |
| `targets.yml` | `644` | Contains paths and tuning only — no secrets; must be readable by container UID 65534 |
| `docker-compose.yml` | `644` | No secrets — ports and structure only |
| `.gitignore` | `644` | Not sensitive |

---

## Building from Source

```bash
# Development build
go build -o directory-exporter .

# Production build (static, stripped)
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
  go build -ldflags="-s -w" -trimpath -o directory-exporter .

# Docker image
docker build -t directory-exporter:latest .
```

Output: ~6 MB static ELF binary. Docker image: ~9 MB.
