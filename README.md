# pfc-gateway

**HTTP REST query server for PFC cold archives — no DuckDB required.**

pfc-gateway makes PFC archives on S3 (or local storage) queryable by **any tool** — Grafana, Python, curl, PowerBI — through a simple HTTP API. No client library, no DuckDB, no special setup on the query side.

Part of the [PFC Ecosystem](https://github.com/ImpossibleForge).

---

## What it does

```
[Grafana / Python / curl / PowerBI / your own tools]
          │
          ▼  HTTP REST — no client library needed
     pfc-gateway  (this server)
          │
          ▼  pfc_jsonl s3-fetch — HTTP Range requests
     .pfc archives on S3
          │
          ▼  only ~4% of the archive is read per query
     NDJSON stream back to client
```

**One query → cold S3 data → back in seconds.** No re-import. No DuckDB.

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

AWS credentials are read from the standard locations (`~/.aws/credentials`, environment variables, IAM role). No extra config needed.

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

### `POST /query`

| Field | Type | Description |
|-------|------|-------------|
| `file` | string | Local path or `s3://bucket/key.pfc` |
| `from_ts` | string | ISO 8601 start time (inclusive) |
| `to_ts` | string | ISO 8601 end time (exclusive) |
| `filter` | object | Optional equality filter `{"level": "ERROR"}` |
| `aws_profile` | string | Optional AWS profile name |

Returns: `application/x-ndjson` stream.

### `POST /query/batch`

Same as `/query` but with `files: [...]` array instead of single `file`.

### `GET /`

Health check. Returns `{"status": "ok", "version": "0.1.0"}`.

---

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PFC_API_KEY` | *(none — auth off)* | API key for `X-API-Key` header |
| `PFC_JSONL_BINARY` | `/usr/local/bin/pfc_jsonl` | Path to pfc_jsonl binary |
| `PFC_HOST` | `0.0.0.0` | Bind address |
| `PFC_PORT` | `8765` | Port |
| `PFC_PRESIGN_TTL` | `3600` | Pre-signed URL TTL in seconds |
| `AWS_DEFAULT_REGION` | `eu-central-1` | AWS region for S3 |

Standard AWS variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_PROFILE`) are respected automatically.

---

## Run as systemd service

```ini
# /etc/systemd/system/pfc-gateway.service
[Unit]
Description=pfc-gateway — PFC cold archive query server
After=network.target

[Service]
Type=simple
User=pfc
WorkingDirectory=/opt/pfc-gateway
ExecStart=/usr/bin/uvicorn pfc_gateway:app --host 0.0.0.0 --port 8765
Restart=on-failure
Environment=PFC_API_KEY=your-secret-key
Environment=AWS_DEFAULT_REGION=eu-central-1

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
    └── pfc-fluentbit   (live pipeline)
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

---

*ImpossibleForge — [github.com/ImpossibleForge](https://github.com/ImpossibleForge)*
*Contact: info@impossibleforge.com*
