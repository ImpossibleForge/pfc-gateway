# Changelog — pfc-gateway

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
