# Changelog — pfc-gateway

## v0.3.0 (2026-04-25)

### Added — SQL query mode via DuckDB (`POST /query/sql`)

pfc-gateway can now execute full SQL queries against `.pfc` archives using DuckDB
with the pfc community extension — for users who already have DuckDB installed.

```bash
curl -X POST http://localhost:8765/query/sql \
  -H "x-api-key: secret" \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT json_extract_string(line, '\''$.level'\'') AS level, COUNT(*) AS cnt FROM pfc_scan('\''/var/lib/pfc/logs.pfc'\'') GROUP BY level ORDER BY cnt DESC"}'
```

**Two query paths — choose what fits your stack:**

| Path | Endpoint | Requires | Use case |
|------|----------|----------|----------|
| Standard | `POST /query` | pfc_jsonl binary | Filter + time range, no DuckDB |
| SQL | `POST /query/sql` | DuckDB + pfc extension | Full SQL, aggregations, JOINs |

**New endpoint:**
- `POST /query/sql` — execute SQL via DuckDB with pfc extension loaded. Returns NDJSON (same format as `/query`).
  - `{"sql": "SELECT ..."}` — any valid DuckDB SQL with `pfc_scan()` or `read_pfc_jsonl()` as table source
  - Returns HTTP 503 with clear message if DuckDB is not installed
  - Returns HTTP 400 with SQL error details on syntax/runtime errors
  - Timeout: 120s

**Health response now includes `sql_mode`:**
```json
{"status": "ok", "version": "0.3.0", "binary": "/usr/local/bin/pfc_jsonl", "sql_mode": true}
```
`sql_mode: true` means DuckDB + pfc extension are available on this gateway instance.

**Setup SQL mode:**
```bash
# Install DuckDB
curl -L https://github.com/duckdb/duckdb/releases/latest/download/duckdb_cli-linux-amd64.gz | gunzip > /usr/local/bin/duckdb && chmod +x /usr/local/bin/duckdb

# Install pfc extension
duckdb -c "INSTALL pfc FROM community; LOAD pfc; SELECT 'ok';"
```

**Environment variable:**

| Variable | Default | Description |
|----------|---------|-------------|
| `DUCKDB_BINARY` | `duckdb` | Path to DuckDB binary |

---

## v0.2.0 (2026-04-21)

### Added — Bidirectional ingest (`POST /ingest`)

pfc-gateway now **receives** data in addition to serving it.
Any tool with an HTTP output — Fluent Bit, Vector, Telegraf, curl — can push NDJSON
directly to the gateway. Rows are buffered and compressed to `.pfc` files automatically.

```bash
# Send a batch of log rows
curl -X POST http://localhost:8765/ingest \
  -H "X-API-Key: secret" \
  -H "Content-Type: application/json" \
  -d '[{"ts":"2026-04-21T10:00:00Z","level":"INFO","msg":"hello"}]'
```

**Three new endpoints:**
- `POST /ingest` — accept rows (JSON array, `{"rows":[...]}`, or raw NDJSON)
- `POST /ingest/flush` — force-compress current buffer to a `.pfc` file immediately
- `GET  /ingest/status` — buffer stats (rows, bytes, age, last output file)

**Auto-rotation (configurable):**
- Size threshold: flush when buffer reaches `PFC_INGEST_ROTATE_MB` (default 64 MB)
- Time threshold: flush when buffer age exceeds `PFC_INGEST_ROTATE_SEC` (default 3600 s)
- Background watchdog task checks every 60 s

**Ingest environment variables:**

| Variable | Default | Description |
|----------|---------|-------------|
| `PFC_INGEST_DIR` | *(none — ingest off)* | Directory for buffer and output `.pfc` files |
| `PFC_INGEST_ROTATE_MB` | `64` | Size rotation threshold (MB) |
| `PFC_INGEST_ROTATE_SEC` | `3600` | Time rotation threshold (seconds) |
| `PFC_INGEST_PREFIX` | `ingest` | Output filename prefix (`ingest_<ts>.pfc`) |

**Error safety:** if `pfc_jsonl compress` fails, the buffer is restored — no data lost.

### Added — test suite for ingest

`tests/test_ingest.py` — 38 tests covering all ingest scenarios:
- Disabled mode (503), body format auto-detection (JSON array / `rows` object / NDJSON),
  buffer writes, flush, status, size-based rotation, time-based watchdog, auth.

**Total test count: 125 tests (87 + 38), all passing.**

### Added — `--ingest-dir` CLI flag

```bash
python pfc_gateway.py --ingest-dir /data/pfc --api-key secret
```

---

## v0.1.1 (2026-04-20)

### Fixed
- **Eager error-checking in query generators** — `_run_query_local` and `_run_query_s3`
  were generator functions, so `_locate_binary()` and S3 pre-signing ran lazily during
  `StreamingResponse` iteration. A missing binary or S3 auth failure was not caught by
  the endpoint's `except RuntimeError` handler — the gateway silently returned HTTP 200
  with an empty body instead of 500.  
  Fixed by making both functions regular functions that perform eager checks upfront, then
  return a nested `_gen()` generator for the actual streaming.

### Added
- Full test suite: `tests/test_gateway.py` (59 tests) and `tests/test_resilience.py`
  (28 resilience / edge-case tests). 87 tests total, all passing.

---

## v0.1.0 (2026-04-16)

Initial release.

### Features
- `POST /query` — query a local or S3 PFC file by time range, stream NDJSON
- `POST /query/batch` — query multiple PFC files in one request (multi-month archives)
- `GET /` — health check
- S3 mode: generates pre-signed URLs, delegates HTTP Range fetching to `pfc_jsonl s3-fetch`
  — only the needed blocks (~4% of archive) are downloaded
- Row-level timestamp filtering after block decompress
- Optional equality filter (`{"level": "ERROR"}`)
- API key authentication via `X-API-Key` header (disabled when `PFC_API_KEY` not set)
- **Grafana SimpleJSON data source** interface:
  - `GET  /grafana` — health check
  - `POST /grafana/search` — list targets
  - `POST /grafana/query` — table response, Grafana time range → from_ts/to_ts
  - `POST /grafana/annotations` — stub (returns empty)
- systemd service example in README
- Docker run example in README
- No DuckDB dependency — works with any HTTP client
