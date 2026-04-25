# pfc-gateway

**Bidirectional HTTP gateway for PFC cold archives — no DuckDB required.**

pfc-gateway makes PFC archives on S3 (or local storage) queryable by **any tool** — Grafana, Python, curl, PowerBI — through a simple HTTP API. It also **receives** NDJSON from Fluent Bit, Vector, Telegraf, or any HTTP client and compresses it to `.pfc` archives automatically.

Part of the [PFC Ecosystem](https://github.com/ImpossibleForge).

---

## What it does

```
[Fluent Bit / Vector / Telegraf / curl]
          │
          ▼  POST /ingest — push NDJSON rows
     pfc-gateway  (this server)  ←─────────── also receives data
          │
          ├── .pfc_buffer.jsonl  (live buffer)
          └── ingest_<ts>.pfc    (auto-rotated on size or time)

[Grafana / Python / curl / PowerBI / your own tools]
          │
          ▼  POST /query — HTTP REST, no client library needed
     pfc-gateway  (this server)  ────────────► serves data
          │
          ▼  pfc_jsonl s3-fetch — HTTP Range requests
     .pfc archives on S3 / local
          │
          ▼  only ~4% of the archive is read per query
     NDJSON stream back to client
```

**One server. Ingest from any tool. Query from any tool.** No DuckDB, no custom plugins.

---

## Install

```bash
# 1. Install pfc_jsonl binary (required)
# Linux x64:
curl -L https://github.com/ImpossibleForge/pfc-jsonl/releases/latest/download/pfc_jsonl-linux-x64 \
     -o /usr/local/bin/pfc_jsonl && chmod +x /usr/local/bin/pfc_jsonl

# macOS (Apple Silicon / M1–M4):
curl -L https://github.com/ImpossibleForge/pfc-jsonl/releases/latest/download/pfc_jsonl-macos-arm64 \
     -o /usr/local/bin/pfc_jsonl && chmod +x /usr/local/bin/pfc_jsonl

# 2. Install pfc-gateway
git clone https://github.com/ImpossibleForge/pfc-gateway
cd pfc-gateway
pip install fastapi uvicorn boto3 python-dateutil

# 3. Start the server
PFC_API_KEY=your-secret-key uvicorn pfc_gateway:app --host 0.0.0.0 --port 8765
```

> **License note:** This tool requires the `pfc_jsonl` binary. `pfc_jsonl` is free for personal and open-source use — commercial use requires a separate license. See [pfc-jsonl](https://github.com/ImpossibleForge/pfc-jsonl) for details.

AWS credentials are read from the standard locations (`~/.aws/credentials`, environment variables, IAM role). No extra config needed.

---

## Ingest — receive data from any HTTP source

Enable ingest by setting `PFC_INGEST_DIR`. The gateway appends rows to a buffer file
and rotates it to a compressed `.pfc` file when a size or time threshold is reached.

```bash
# Start gateway with ingest enabled
PFC_API_KEY=secret PFC_INGEST_DIR=/data/pfc \
  uvicorn pfc_gateway:app --host 0.0.0.0 --port 8765
```

### Send rows with curl

```bash
# JSON array
curl -s -X POST http://localhost:8765/ingest \
  -H "X-API-Key: secret" \
  -H "Content-Type: application/json" \
  -d '[{"ts":"2026-04-21T10:00:00Z","level":"INFO","msg":"server started"}]'

# NDJSON (Fluent Bit json_stream / Vector ndjson)
printf '{"ts":"2026-04-21T10:00:01Z","level":"WARN","msg":"high cpu"}\n' | \
  curl -s -X POST http://localhost:8765/ingest \
  -H "X-API-Key: secret" \
  -H "Content-Type: application/x-ndjson" \
  --data-binary @-
```

### Fluent Bit HTTP output

```ini
[OUTPUT]
    Name              http
    Match             *
    Host              your-server
    Port              8765
    URI               /ingest
    Format            json          # sends JSON array — pfc-gateway auto-detects
    Header            X-API-Key secret
```

### Vector HTTP sink

```toml
[sinks.pfc_gateway]
type     = "http"
inputs   = ["your_source"]
uri      = "http://your-server:8765/ingest"
encoding.codec = "ndjson"

[sinks.pfc_gateway.request.headers]
X-API-Key = "secret"
```

### Force-flush the buffer

```bash
curl -s -X POST http://localhost:8765/ingest/flush \
  -H "X-API-Key: secret"
# → {"flushed": true, "rows": 4821, "file": "/data/pfc/ingest_20260421T103045.pfc"}
```

### Check buffer status

```bash
curl -s http://localhost:8765/ingest/status -H "X-API-Key: secret"
# → {"enabled": true, "buffer_rows": 142, "buffer_mb": 0.021,
#    "last_flush_age_sec": 312.4, "rotate_mb": 64, "rotate_sec": 3600, ...}
```

---

## Query a PFC archive

### curl (S3 file)

```bash
curl -s \
  -H "X-API-Key: your-secret-key" \
  -H "Content-Type: application/json" \
  -X POST http://localhost:8765/query \
  -d '{
    "file":    "s3://my-archive/pfc/logs_2026-03.pfc",
    "from_ts": "2026-03-05T10:00:00Z",
    "to_ts":   "2026-03-05T12:00:00Z",
    "filter":  {"level": "ERROR"}
  }'
```

**Response: NDJSON stream**
```json
{"ts":"2026-03-05T10:14:32Z","level":"ERROR","message":"connection refused","host":"web-03"}
{"ts":"2026-03-05T11:02:19Z","level":"ERROR","message":"disk full","host":"db-01"}
```

### curl (local file)

```bash
curl -s \
  -H "X-API-Key: your-secret-key" \
  -X POST http://localhost:8765/query \
  -H "Content-Type: application/json" \
  -d '{"file":"/data/archive/logs_march.pfc","from_ts":"2026-03-01","to_ts":"2026-04-01"}'
```

### Python

```python
import requests, json

resp = requests.post(
    "http://localhost:8765/query",
    headers={"X-API-Key": "your-secret-key"},
    json={
        "file":    "s3://my-archive/pfc/logs_2026-03.pfc",
        "from_ts": "2026-03-05T10:00Z",
        "to_ts":   "2026-03-05T12:00Z",
    },
    stream=True,
)
for line in resp.iter_lines():
    row = json.loads(line)
    print(row["ts"], row.get("level"), row.get("message"))
```

---

## Query multiple files (multi-month)

```bash
curl -s \
  -H "X-API-Key: your-secret-key" \
  -X POST http://localhost:8765/query/batch \
  -H "Content-Type: application/json" \
  -d '{
    "files": [
      "s3://my-archive/pfc/logs_2026-01.pfc",
      "s3://my-archive/pfc/logs_2026-02.pfc",
      "s3://my-archive/pfc/logs_2026-03.pfc"
    ],
    "from_ts": "2026-01-15T00:00Z",
    "to_ts":   "2026-03-15T00:00Z"
  }'
```

Files are queried in order. Results stream back as a single combined NDJSON response.

---


## SQL Query via DuckDB (optional)

If DuckDB with the [pfc extension](https://github.com/ImpossibleForge/pfc-duckdb) is installed
on the gateway server, you can run full SQL queries against `.pfc` archives:

```bash
curl -X POST http://localhost:8765/query/sql \
  -H "x-api-key: secret" \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT json_extract_string(line, '"'"'$.level'"'"') AS level, COUNT(*) AS cnt FROM pfc_scan('"'"'/var/lib/pfc/logs.pfc'"'"') GROUP BY level ORDER BY cnt DESC"
  }'
```

Supports any DuckDB SQL — `GROUP BY`, `AVG`, `JOIN` across multiple files, window functions:

```sql
-- Avg latency per service, last hour
SELECT json_extract_string(line, '$.service') AS service,
       ROUND(AVG(json_extract(line, '$.latency_ms')::FLOAT), 1) AS avg_ms
FROM pfc_scan('/var/lib/pfc/logs.pfc')
GROUP BY service ORDER BY avg_ms DESC;
```

Check if SQL mode is available on your gateway instance:
```bash
curl http://localhost:8765/ -H "x-api-key: secret"
# {"status":"ok","version":"0.3.0","binary":"...","sql_mode":true}
```

`sql_mode: false` means DuckDB is not installed — standard `/query` still works normally.

**Setup:**
```bash
# Install DuckDB
curl -L https://github.com/duckdb/duckdb/releases/latest/download/duckdb_cli-linux-amd64.gz \
  | gunzip > /usr/local/bin/duckdb && chmod +x /usr/local/bin/duckdb
# Install pfc extension
duckdb -c "INSTALL pfc FROM community;"
```
## Grafana Integration

pfc-gateway implements the [Grafana SimpleJSON data source](https://grafana.com/grafana/plugins/grafana-simple-json-datasource/) protocol.

**Setup (takes 2 minutes):**

1. In Grafana → Settings → Data Sources → Add data source
2. Search for **SimpleJSON** (install plugin if needed)
3. URL: `http://your-server:8765/grafana`
4. Custom HTTP Header: `X-API-Key` = your secret key
5. Save & Test → should show "Data source is working"

**In a dashboard panel:**
- Target: `s3://my-archive/pfc/logs_2026-03.pfc`
- Optional filter: `s3://my-archive/logs.pfc|{"level":"ERROR"}`

Grafana's time range picker controls `from_ts` and `to_ts` automatically.

---

## Live DB + cold archives in one dashboard

**Without pfc-gateway:**  Only the last 30 days (hot live data) visible in Grafana.

**With pfc-gateway:**
- Panel 1: Live DB data source (last 30 days)
- Panel 2: pfc-gateway data source (months/years of cold PFC archives)

Both panels in the same Grafana dashboard. No re-import. No DuckDB.

---

## API Reference

### Query endpoints

#### `POST /query`

| Field | Type | Description |
|-------|------|-------------|
| `file` | string | Local path or `s3://bucket/key.pfc` |
| `from_ts` | string | ISO 8601 start time (inclusive) |
| `to_ts` | string | ISO 8601 end time (exclusive) |
| `filter` | object | Optional equality filter `{"level": "ERROR"}` |
| `aws_profile` | string | Optional AWS profile name |

Returns: `application/x-ndjson` stream.

#### `POST /query/batch`

Same as `/query` but with `files: [...]` array instead of single `file`.

#### `GET /`

Health check. Returns `{"status": "ok", "version": "0.2.0"}`.

### Ingest endpoints

#### `POST /ingest`

Accepts rows in three formats (auto-detected):
- JSON array: `[{...}, {...}]`
- Object with rows key: `{"rows": [{...}, ...]}`
- Raw NDJSON: `{...}\n{...}\n`

Returns: `{"accepted": N}`

Requires `PFC_INGEST_DIR` to be set (returns 503 otherwise).

#### `POST /ingest/flush`

Force-compresses the current buffer to a `.pfc` file immediately.

Returns: `{"flushed": true, "rows": N, "file": "/path/to/ingest_<ts>.pfc"}` or `{"flushed": false, "reason": "empty"}`.

#### `GET /ingest/status`

Returns buffer statistics: row count, byte size, age since last flush, last output file, rotation thresholds.

---

## Environment Variables

### Query / Auth

| Variable | Default | Description |
|----------|---------|-------------|
| `PFC_API_KEY` | *(none — auth off)* | API key for `X-API-Key` header |
| `PFC_JSONL_BINARY` | `/usr/local/bin/pfc_jsonl` | Path to pfc_jsonl binary |
| `PFC_HOST` | `0.0.0.0` | Bind address |
| `PFC_PORT` | `8765` | Port |
| `PFC_PRESIGN_TTL` | `3600` | Pre-signed URL TTL in seconds |
| `AWS_DEFAULT_REGION` | `eu-central-1` | AWS region for S3 |

### Ingest

| Variable | Default | Description |
|----------|---------|-------------|
| `PFC_INGEST_DIR` | *(none — ingest off)* | Directory for buffer + output `.pfc` files |
| `PFC_INGEST_ROTATE_MB` | `64` | Rotate when buffer reaches this size (MB) |
| `PFC_INGEST_ROTATE_SEC` | `3600` | Rotate when buffer is older than this (seconds) |
| `PFC_INGEST_PREFIX` | `ingest` | Output filename prefix: `ingest_<ts>.pfc` |

Standard AWS variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_PROFILE`) are respected automatically.

---

## Run as systemd service

```ini
# /etc/systemd/system/pfc-gateway.service
[Unit]
Description=pfc-gateway — PFC cold archive bidirectional gateway
After=network.target

[Service]
Type=simple
User=pfc
WorkingDirectory=/opt/pfc-gateway
ExecStart=/usr/bin/uvicorn pfc_gateway:app --host 0.0.0.0 --port 8765
Restart=on-failure
Environment=PFC_API_KEY=your-secret-key
Environment=AWS_DEFAULT_REGION=eu-central-1
Environment=PFC_INGEST_DIR=/data/pfc          # omit to disable ingest
Environment=PFC_INGEST_ROTATE_MB=64
Environment=PFC_INGEST_ROTATE_SEC=3600

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl enable --now pfc-gateway
```

---

## Run as Docker container

```bash
docker run -d \
  -p 8765:8765 \
  -e PFC_API_KEY=your-secret-key \
  -e AWS_ACCESS_KEY_ID=... \
  -e AWS_SECRET_ACCESS_KEY=... \
  --name pfc-gateway \
  impossibleforge/pfc-gateway:latest
```

---

## Architecture in the full PFC ecosystem

```
Your data sources
    │
    ├── pfc-migrate     (one-shot export)
    ├── pfc-archiver-*  (autonomous daemon)
    ├── pfc-fluentbit   (live pipeline)
    └── pfc-gateway     (POST /ingest ← NEW)  ← this repo
              │
              ▼
    .pfc archives (local / S3 / Azure / GCS)
              │
    ┌─────────┴──────────┐
    │                    │
    ▼                    ▼
pfc-duckdb          pfc-gateway  ← this repo
SQL queries         HTTP REST
(DuckDB needed)     (no DuckDB)
    │                    │
    ▼                    ▼
Python / CLI        Grafana / PowerBI / curl / own tools
                    Fluent Bit / Vector / Telegraf (ingest)
```

| Tool | What | DuckDB needed |
|------|------|---------------|
| `pfc-migrate` | One-shot export to `.pfc` | No |
| `pfc-archiver-*` | Autonomous archive daemon | No |
| `pfc-fluentbit` | Live pipeline → `.pfc` | No |
| `pfc-duckdb` | SQL queries on PFC files | Yes |
| **`pfc-gateway`** | **HTTP REST — any tool** | **No** |

---

## Related repos

- [pfc-jsonl](https://github.com/ImpossibleForge/pfc-jsonl) — core binary (compress/decompress/query)
- [pfc-migrate](https://github.com/ImpossibleForge/pfc-migrate) — one-shot export and archive conversion
- [pfc-fluentbit](https://github.com/ImpossibleForge/pfc-fluentbit) — live Fluent Bit → PFC pipeline
- [pfc-duckdb](https://github.com/ImpossibleForge/pfc-duckdb) — DuckDB extension for SQL queries on PFC files
- [pfc-vector](https://github.com/ImpossibleForge/pfc-vector) — high-performance Rust ingest daemon for Vector.dev and Telegraf
- [pfc-otel-collector](https://github.com/ImpossibleForge/pfc-otel-collector) — OpenTelemetry OTLP/HTTP log exporter
- [pfc-kafka-consumer](https://github.com/ImpossibleForge/pfc-kafka-consumer) — Kafka / Redpanda consumer → PFC
- [pfc-telegraf](https://github.com/ImpossibleForge/pfc-telegraf) — Telegraf HTTP output plugin → PFC
- [pfc-grafana](https://github.com/ImpossibleForge/pfc-grafana) — Grafana data source plugin for PFC archives

---

*ImpossibleForge — [github.com/ImpossibleForge](https://github.com/ImpossibleForge)*
*Contact: info@impossibleforge.com*

---

## License

pfc-gateway (this repository) is released under the MIT License — see [LICENSE](LICENSE).

The PFC-JSONL binary () is proprietary software — free for personal and open-source use. Commercial use requires a license: [info@impossibleforge.com](mailto:info@impossibleforge.com)
