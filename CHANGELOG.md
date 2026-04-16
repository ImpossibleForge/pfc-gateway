# Changelog — pfc-gateway

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
