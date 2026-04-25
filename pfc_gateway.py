"""
pfc-gateway v0.3.0 — Bidirectional HTTP gateway for PFC cold archives.

Query side  — stream NDJSON out of PFC files (local or S3)
Ingest side — receive NDJSON / JSON from any HTTP source and compress to PFC

Query endpoints
---------------
  POST /query        — query a single PFC file by time range, stream NDJSON
  POST /query/batch  — query multiple PFC files in one request
  POST /query/sql    — execute SQL via DuckDB + pfc extension (optional, requires DuckDB)

Ingest endpoints
----------------
  POST /ingest        — receive rows (JSON array, NDJSON, or {"rows":[…]})
  POST /ingest/flush  — force-compress the current buffer to a .pfc file
  GET  /ingest/status — buffer stats (rows, bytes, age, last file)

Supported query paths
---------------------
  Local  : PFC file on the server's local filesystem or NFS
  S3     : PFC archive on AWS S3 / GCS / Azure Blob (S3-compatible)

Architecture
------------
  Ingest: Fluent Bit / Vector / Telegraf / curl
          → POST /ingest  → append to .pfc_buffer.jsonl
          → rotate (size or time) → pfc_jsonl compress → ingest_<ts>.pfc

  Query:  Client → POST /query → pfc_jsonl s3-fetch / query → NDJSON stream

Grafana SimpleJSON Data Source is also supported:
  GET  /grafana           → health check
  POST /grafana/search    → list targets
  POST /grafana/query     → table response
  POST /grafana/annotations → stub

Usage
-----
  pip install fastapi uvicorn boto3 python-dateutil
  PFC_API_KEY=secret PFC_INGEST_DIR=/data/ingest \\
    uvicorn pfc_gateway:app --host 0.0.0.0 --port 8765

  # Ingest NDJSON from Fluent Bit / Vector / curl
  curl -H "X-API-Key: secret" -X POST http://localhost:8765/ingest \\
    -H "Content-Type: application/json" \\
    -d '[{"ts":"2026-04-21T10:00:00Z","level":"INFO","msg":"hello"}]'

  # Query a local file
  curl -H "X-API-Key: secret" -X POST http://localhost:8765/query \\
    -H "Content-Type: application/json" \\
    -d '{"file":"path/to/logs.pfc","from_ts":"2026-03-01T00:00Z","to_ts":"2026-03-02T00:00Z"}'
"""

from __future__ import annotations

__version__ = "0.3.0"

import asyncio
import json
import logging
import os
import shutil
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator, Optional

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from dateutil import parser as dateparser
from fastapi import Depends, FastAPI, HTTPException, Request, status
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.security.api_key import APIKeyHeader
from pydantic import BaseModel, field_validator

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DUCKDB_BINARY: str = os.environ.get("DUCKDB_BINARY", "duckdb")

PFC_JSONL_BINARY: str = (
    os.environ.get("PFC_JSONL_BINARY")
    or shutil.which("pfc_jsonl")
    or "/usr/local/bin/pfc_jsonl"
)

API_KEY: str | None = os.environ.get("PFC_API_KEY")  # None = auth disabled (dev mode)

PRESIGN_TTL: int = int(os.environ.get("PFC_PRESIGN_TTL", "3600"))  # seconds

AWS_REGION: str = os.environ.get("AWS_DEFAULT_REGION", "eu-central-1")

# Ingest config — all disabled when PFC_INGEST_DIR is not set
INGEST_DIR: str | None = os.environ.get("PFC_INGEST_DIR")
INGEST_ROTATE_MB: int  = int(os.environ.get("PFC_INGEST_ROTATE_MB", "64"))
INGEST_ROTATE_SEC: int = int(os.environ.get("PFC_INGEST_ROTATE_SEC", "3600"))
INGEST_PREFIX: str     = os.environ.get("PFC_INGEST_PREFIX", "ingest")

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("pfc-gateway")

# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

app = FastAPI(
    title="pfc-gateway",
    version=__version__,
    description="Bidirectional HTTP gateway for PFC cold archives. "
                "Ingest NDJSON from any HTTP source; query PFC files on S3 or local storage.",
)

# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

_api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def require_api_key(key: str | None = Depends(_api_key_header)) -> None:
    if API_KEY is None:
        return  # auth disabled — dev mode
    if key != API_KEY:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key (X-API-Key header).",
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _locate_binary() -> str:
    if not Path(PFC_JSONL_BINARY).exists():
        raise RuntimeError(
            f"pfc_jsonl binary not found at: {PFC_JSONL_BINARY}\n"
            "Install: https://github.com/ImpossibleForge/pfc-jsonl/releases\n"
            "Or set PFC_JSONL_BINARY env var."
        )
    return PFC_JSONL_BINARY


def _parse_ts(ts: str | None) -> datetime | None:
    if not ts:
        return None
    dt = dateparser.parse(ts)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _fmt_ts(dt: datetime) -> str:
    """Format for pfc_jsonl CLI: YYYY-MM-DDTHH:MM"""
    return dt.strftime("%Y-%m-%dT%H:%M")


def _is_s3(path: str) -> bool:
    return path.startswith("s3://") or path.startswith("s3a://")


def _parse_s3_path(path: str) -> tuple[str, str]:
    """Returns (bucket, key)."""
    path = path.replace("s3a://", "s3://")
    parts = path[5:].split("/", 1)
    if len(parts) < 2:
        raise ValueError(f"Invalid S3 path: {path}")
    return parts[0], parts[1]


def _presign(s3_client, bucket: str, key: str, ttl: int = PRESIGN_TTL) -> str:
    return s3_client.generate_presigned_url(
        "get_object",
        Params={"Bucket": bucket, "Key": key},
        ExpiresIn=ttl,
    )


def _row_in_range(row: dict, from_dt: datetime | None, to_dt: datetime | None) -> bool:
    """Row-level timestamp filter applied after block decompress."""
    ts_raw = row.get("timestamp") or row.get("@timestamp")
    if ts_raw is None or (from_dt is None and to_dt is None):
        return True
    try:
        if isinstance(ts_raw, (int, float)):
            ts = datetime.fromtimestamp(ts_raw, tz=timezone.utc)
        else:
            ts = dateparser.parse(str(ts_raw))
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            ts = ts.astimezone(timezone.utc)
    except Exception:
        return True  # unparseable → include row

    if from_dt and ts < from_dt:
        return False
    if to_dt and ts >= to_dt:
        return False
    return True


def _apply_filter(row: dict, filter_expr: dict | None) -> bool:
    """Simple equality filter: {"level": "ERROR", "host": "web-01"}"""
    if not filter_expr:
        return True
    for key, val in filter_expr.items():
        if row.get(key) != val:
            return False
    return True


# ---------------------------------------------------------------------------
# Query execution
# ---------------------------------------------------------------------------

def _run_query_local(
    pfc_path: str,
    from_dt: datetime | None,
    to_dt: datetime | None,
    filter_expr: dict | None,
) -> Generator[str, None, None]:
    """Query a local PFC file. Streams NDJSON lines.

    Uses `pfc_jsonl query --from --to` when a time range is given (block-level
    index → only needed blocks decompressed).  Falls back to
    `pfc_jsonl decompress` when no time range is given — full file decompressed,
    Python-level filter handles field filtering.

    Binary existence is checked eagerly (before the generator runs) so that a
    missing binary raises RuntimeError at call time, not during streaming.
    """
    binary = _locate_binary()  # ← eager: RuntimeError here if binary is missing

    def _gen() -> Generator[str, None, None]:
        if from_dt is not None and to_dt is not None:
            # Block-level time range query (efficient — only matching blocks read)
            cmd = [binary, "query",
                   "--from", _fmt_ts(from_dt),
                   "--to",   _fmt_ts(to_dt),
                   "--out",  "-", pfc_path]
        else:
            # No time range — full decompress; Python filter handles the rest
            cmd = [binary, "decompress", pfc_path, "-"]

        log.info("Local query: %s", " ".join(cmd))

        proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )

        try:
            for line in proc.stdout:
                line = line.rstrip("\n")
                if not line:
                    continue
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if _row_in_range(row, from_dt, to_dt) and _apply_filter(row, filter_expr):
                    yield json.dumps(row, ensure_ascii=False) + "\n"
        finally:
            proc.stdout.close()
            proc.wait()

        if proc.returncode not in (0, None):
            stderr = proc.stderr.read()
            log.warning("pfc_jsonl exited %d: %s", proc.returncode, stderr[:500])

    return _gen()


def _run_query_s3(
    pfc_s3_path: str,
    from_dt: datetime | None,
    to_dt: datetime | None,
    filter_expr: dict | None,
    aws_profile: str | None = None,
) -> Generator[str, None, None]:
    """Query a PFC file on S3 using pfc_jsonl s3-fetch (HTTP Range only).

    Binary check and S3 pre-signing happen eagerly (before the generator runs)
    so that auth failures or a missing binary raise RuntimeError at call time,
    not silently mid-stream.
    """
    binary = _locate_binary()  # ← eager: RuntimeError if binary is missing
    bucket, key = _parse_s3_path(pfc_s3_path)
    idx_key = key + ".idx"

    # Generate pre-signed URLs eagerly — failure raises RuntimeError here,
    # which the /query endpoint catches and converts to HTTP 500.
    session = boto3.Session(profile_name=aws_profile, region_name=AWS_REGION)
    s3 = session.client("s3")

    try:
        pfc_url = _presign(s3, bucket, key)
        idx_url = _presign(s3, bucket, idx_key)
    except (BotoCoreError, ClientError) as exc:
        raise RuntimeError(f"Failed to generate pre-signed URLs: {exc}") from exc

    log.info("S3 query: bucket=%s key=%s from=%s to=%s", bucket, key, from_dt, to_dt)

    def _gen() -> Generator[str, None, None]:
        cmd = [binary, "s3-fetch"]
        if from_dt:
            cmd += ["--from", _fmt_ts(from_dt), "--to", _fmt_ts(to_dt or from_dt)]
            cmd += ["--idx-url", idx_url]
        else:
            cmd += ["--all"]
        cmd += ["--out", "-", pfc_url]

        proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )

        try:
            for line in proc.stdout:
                line = line.rstrip("\n")
                if not line:
                    continue
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if _row_in_range(row, from_dt, to_dt) and _apply_filter(row, filter_expr):
                    yield json.dumps(row, ensure_ascii=False) + "\n"
        finally:
            proc.stdout.close()
            proc.wait()

    return _gen()


# ---------------------------------------------------------------------------
# Ingest — global state
# ---------------------------------------------------------------------------

_ingest_lock: asyncio.Lock | None = None
_ingest_buffer: Path | None = None       # .pfc_buffer.jsonl
_ingest_row_count: int = 0               # rows in current buffer
_ingest_last_flush: float = 0.0          # time.time() of last successful flush
_ingest_last_file: str = ""              # path of last compressed .pfc output


# ---------------------------------------------------------------------------
# Ingest — background helpers
# ---------------------------------------------------------------------------

async def _flush_buffer_locked(reason: str = "manual") -> dict:
    """Compress the current buffer to a .pfc file.

    MUST be called with *_ingest_lock held*.
    Renames buffer → .compressing, runs `pfc_jsonl compress`, removes temp.
    On error the buffer is restored so no data is lost.
    """
    global _ingest_row_count, _ingest_last_flush, _ingest_last_file

    if _ingest_buffer is None or not _ingest_buffer.exists() or _ingest_row_count == 0:
        return {"flushed": False, "reason": "empty"}

    ts_str   = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    tmp_path = _ingest_buffer.with_suffix(".compressing")
    out_path = Path(INGEST_DIR) / f"{INGEST_PREFIX}_{ts_str}.pfc"

    rows_snapshot     = _ingest_row_count
    _ingest_buffer.rename(tmp_path)
    _ingest_row_count = 0
    _ingest_last_flush = time.time()

    # Check binary eagerly before spawning subprocess
    try:
        binary = _locate_binary()
    except RuntimeError as exc:
        tmp_path.rename(_ingest_buffer)
        _ingest_row_count = rows_snapshot
        log.error("Ingest flush aborted — binary missing: %s", exc)
        return {"flushed": False, "reason": str(exc)}

    proc = await asyncio.create_subprocess_exec(
        binary, "compress", str(tmp_path), str(out_path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    _, stderr_bytes = await proc.communicate()

    if proc.returncode != 0:
        err_msg = stderr_bytes.decode("utf-8", errors="replace")[:300]
        log.error("pfc_jsonl compress failed (rc=%d): %s", proc.returncode, err_msg)
        # Restore buffer — data not lost
        tmp_path.rename(_ingest_buffer)
        _ingest_row_count = rows_snapshot
        return {"flushed": False, "reason": "compress_error", "detail": err_msg}

    # Success — clean up temp file
    try:
        tmp_path.unlink()
    except Exception:
        pass

    _ingest_last_file = str(out_path)
    log.info("Ingest flush (%s): %d rows → %s", reason, rows_snapshot, out_path.name)
    return {"flushed": True, "rows": rows_snapshot, "file": str(out_path)}


async def _maybe_flush() -> None:
    """Check size threshold and flush if needed.

    MUST be called with *_ingest_lock held*.
    """
    if _ingest_buffer is None or not _ingest_buffer.exists():
        return
    size_mb = _ingest_buffer.stat().st_size / (1024 * 1024)
    if size_mb >= INGEST_ROTATE_MB:
        log.info("Ingest: size threshold %.1f MB >= %d MB, rotating", size_mb, INGEST_ROTATE_MB)
        await _flush_buffer_locked("size")


async def _rotation_watchdog() -> None:
    """Background coroutine: flush buffer when time threshold is reached."""
    while True:
        await asyncio.sleep(60)
        if _ingest_lock is None:
            return
        async with _ingest_lock:
            if _ingest_row_count == 0:
                continue
            age = time.time() - _ingest_last_flush
            if age >= INGEST_ROTATE_SEC:
                log.info("Ingest watchdog: age %.0fs >= %ds, rotating", age, INGEST_ROTATE_SEC)
                await _flush_buffer_locked("time")


# ---------------------------------------------------------------------------
# Startup
# ---------------------------------------------------------------------------

@app.on_event("startup")
async def _startup() -> None:
    global _ingest_lock, _ingest_buffer, _ingest_row_count, _ingest_last_flush

    if INGEST_DIR is None:
        log.info("Ingest disabled (set PFC_INGEST_DIR to enable)")
        return

    Path(INGEST_DIR).mkdir(parents=True, exist_ok=True)
    _ingest_lock       = asyncio.Lock()
    _ingest_buffer     = Path(INGEST_DIR) / ".pfc_buffer.jsonl"
    _ingest_row_count  = 0
    _ingest_last_flush = time.time()

    asyncio.create_task(_rotation_watchdog())

    log.info(
        "Ingest enabled: dir=%s  rotate_mb=%d  rotate_sec=%d",
        INGEST_DIR, INGEST_ROTATE_MB, INGEST_ROTATE_SEC,
    )


# ---------------------------------------------------------------------------
# REST models
# ---------------------------------------------------------------------------

class QueryRequest(BaseModel):
    file: str
    from_ts: Optional[str] = None
    to_ts: Optional[str] = None
    filter: Optional[dict[str, Any]] = None
    aws_profile: Optional[str] = None

    @field_validator("file")
    @classmethod
    def file_must_be_pfc(cls, v: str) -> str:
        if not v.endswith(".pfc"):
            raise ValueError("file must end with .pfc")
        return v


class BatchQueryRequest(BaseModel):
    files: list[str]
    from_ts: Optional[str] = None
    to_ts: Optional[str] = None
    filter: Optional[dict[str, Any]] = None
    aws_profile: Optional[str] = None


# ---------------------------------------------------------------------------
# Endpoints — query
# ---------------------------------------------------------------------------

def _duckdb_sql_available() -> bool:
    """Check if DuckDB with pfc extension is usable for /query/sql."""
    try:
        r = subprocess.run(
            [DUCKDB_BINARY, "-c", "LOAD pfc; SELECT 1;"],
            capture_output=True, timeout=5
        )
        return r.returncode == 0
    except Exception:
        return False


@app.get("/", dependencies=[Depends(require_api_key)])
async def health():
    return {
        "status": "ok",
        "version": __version__,
        "binary": PFC_JSONL_BINARY,
        "sql_mode": _duckdb_sql_available(),
    }


@app.post("/query", dependencies=[Depends(require_api_key)])
async def query(req: QueryRequest):
    """
    Query a PFC archive for a time range.

    Returns an NDJSON stream — one JSON object per line.

    Body:
      file        : local path or s3://bucket/key.pfc
      from_ts     : ISO 8601 start time (inclusive)
      to_ts       : ISO 8601 end time (exclusive)
      filter      : optional equality filter {"level": "ERROR"}
      aws_profile : optional AWS profile name (S3 mode)
    """
    try:
        from_dt = _parse_ts(req.from_ts)
        to_dt   = _parse_ts(req.to_ts)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Invalid timestamp: {exc}")

    try:
        if _is_s3(req.file):
            gen = _run_query_s3(req.file, from_dt, to_dt, req.filter, req.aws_profile)
        else:
            if not Path(req.file).exists():
                raise HTTPException(status_code=404, detail=f"File not found: {req.file}")
            gen = _run_query_local(req.file, from_dt, to_dt, req.filter)
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc))

    return StreamingResponse(gen, media_type="application/x-ndjson")


@app.post("/query/batch", dependencies=[Depends(require_api_key)])
async def query_batch(req: BatchQueryRequest):
    """
    Query multiple PFC files in one request and stream combined NDJSON.
    Files are queried in order — useful for multi-month archives.

    Body:
      files       : list of local paths or s3:// URIs ending in .pfc
      from_ts     : ISO 8601 start (inclusive)
      to_ts       : ISO 8601 end (exclusive)
      filter      : optional equality filter
      aws_profile : optional AWS profile name
    """
    try:
        from_dt = _parse_ts(req.from_ts)
        to_dt   = _parse_ts(req.to_ts)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Invalid timestamp: {exc}")

    def _combined_gen():
        for f in req.files:
            if not f.endswith(".pfc"):
                continue
            try:
                gen = (
                    _run_query_s3(f, from_dt, to_dt, req.filter, req.aws_profile)
                    if _is_s3(f)
                    else _run_query_local(f, from_dt, to_dt, req.filter)
                )
                yield from gen
            except Exception as exc:
                log.warning("Skipping %s: %s", f, exc)

    return StreamingResponse(_combined_gen(), media_type="application/x-ndjson")


# ---------------------------------------------------------------------------
# Endpoints — SQL query (DuckDB + pfc extension)
# ---------------------------------------------------------------------------

class SqlQueryRequest(BaseModel):
    sql: str


@app.post("/query/sql", dependencies=[Depends(require_api_key)])
async def query_sql(req: SqlQueryRequest):
    """
    Execute a SQL query via DuckDB with the pfc extension loaded.
    Returns NDJSON — same format as /query for plugin compatibility.

    Requirements: DuckDB binary in PATH (or DUCKDB_BINARY env) +
                  pfc extension installed (INSTALL pfc FROM community).

    Body:
      sql : SQL query string, e.g.
            "SELECT * FROM pfc_scan('/var/lib/pfc/logs.pfc')
             WHERE json_extract_string(line, '$.level') = 'ERROR'"

    Example:
      curl -X POST http://localhost:8765/query/sql \\
        -H "x-api-key: secret" -H "Content-Type: application/json" \\
        -d '{"sql": "SELECT COUNT(*) FROM pfc_scan('\\''logs.pfc'\\'')"}'
    """
    if not req.sql or not req.sql.strip():
        raise HTTPException(status_code=400, detail="sql field is required")

    # Wrap in LOAD pfc to ensure extension is available
    full_sql = f"LOAD pfc;\n{req.sql.strip()}"

    try:
        result = await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: subprocess.run(
                [DUCKDB_BINARY, "-json", "-c", full_sql],
                capture_output=True, text=True, timeout=120
            )
        )
    except FileNotFoundError:
        raise HTTPException(
            status_code=503,
            detail=f"DuckDB binary not found: '{DUCKDB_BINARY}'. "
                   "Install DuckDB and set DUCKDB_BINARY env var to enable SQL mode."
        )
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=504, detail="SQL query timed out (120s)")

    if result.returncode != 0:
        stderr = result.stderr.strip()
        # DuckDB not having pfc extension is a specific, clear error
        if "pfc" in stderr.lower() and ("not found" in stderr.lower() or "catalog" in stderr.lower()):
            raise HTTPException(
                status_code=503,
                detail="pfc DuckDB extension not installed. "
                       "Run: INSTALL pfc FROM community; in DuckDB."
            )
        raise HTTPException(status_code=400, detail=f"SQL error: {stderr[:500]}")

    # DuckDB -json returns a JSON array — convert to NDJSON for consistency
    try:
        rows = json.loads(result.stdout) if result.stdout.strip() else []
    except json.JSONDecodeError:
        raise HTTPException(status_code=500, detail="Failed to parse DuckDB output")

    if not isinstance(rows, list):
        rows = [rows]

    def _ndjson_gen():
        for row in rows:
            yield json.dumps(row, ensure_ascii=False) + "\n"

    return StreamingResponse(_ndjson_gen(), media_type="application/x-ndjson")


# ---------------------------------------------------------------------------
# Endpoints — ingest
# ---------------------------------------------------------------------------

@app.post("/ingest", dependencies=[Depends(require_api_key)])
async def ingest(request: Request):
    """
    Receive rows and append to the ingest buffer.

    Accepts three body formats (auto-detected):
      - JSON array:               [{...}, {...}, ...]
      - Object with "rows" key:   {"rows": [{...}, ...]}
      - Raw NDJSON:               {...}\\n{...}\\n...

    Compatible with Fluent Bit HTTP output, Vector HTTP sink,
    Telegraf HTTP output, and plain curl.

    Returns: {"accepted": N}

    Requires PFC_INGEST_DIR to be set (otherwise 503).
    """
    if _ingest_lock is None:
        raise HTTPException(
            status_code=503,
            detail="Ingest not enabled. Set PFC_INGEST_DIR environment variable.",
        )

    body = await request.body()
    if not body or not body.strip():
        return {"accepted": 0}

    body_text = body.decode("utf-8", errors="replace")
    rows: list[dict] = []
    parse_succeeded = False   # True once we confirm the body is valid JSON/NDJSON

    # Try structured JSON first (array or {"rows": [...]})
    stripped = body_text.lstrip()
    if stripped.startswith(("{", "[")):
        try:
            parsed = json.loads(body_text)
            parse_succeeded = True            # valid JSON — even if zero dict rows
            if isinstance(parsed, list):
                rows = [r for r in parsed if isinstance(r, dict)]
            elif isinstance(parsed, dict):
                if "rows" in parsed and isinstance(parsed["rows"], list):
                    rows = [r for r in parsed["rows"] if isinstance(r, dict)]
                else:
                    rows = [parsed]  # single object body
        except json.JSONDecodeError:
            pass

    # Fall back to NDJSON (line-by-line)
    if not parse_succeeded:
        for line in body_text.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                if isinstance(obj, dict):
                    rows.append(obj)
                    parse_succeeded = True
            except json.JSONDecodeError:
                continue

    if not parse_succeeded:
        raise HTTPException(
            status_code=400,
            detail="No valid JSON objects found in request body. "
                   "Expected JSON array, {\"rows\":[...]}, or NDJSON.",
        )

    if not rows:
        return {"accepted": 0}

    async with _ingest_lock:
        global _ingest_row_count
        with _ingest_buffer.open("a", encoding="utf-8") as fh:
            for row in rows:
                fh.write(json.dumps(row, ensure_ascii=False) + "\n")
        _ingest_row_count += len(rows)
        await _maybe_flush()

    return {"accepted": len(rows)}


@app.post("/ingest/flush", dependencies=[Depends(require_api_key)])
async def ingest_flush():
    """
    Force-compress the current ingest buffer to a .pfc file immediately.

    Returns the flush result:
      {"flushed": true,  "rows": N, "file": "/path/to/ingest_<ts>.pfc"}
      {"flushed": false, "reason": "empty"}
    """
    if _ingest_lock is None:
        raise HTTPException(
            status_code=503,
            detail="Ingest not enabled. Set PFC_INGEST_DIR environment variable.",
        )
    async with _ingest_lock:
        result = await _flush_buffer_locked("manual")
    return result


@app.get("/ingest/status", dependencies=[Depends(require_api_key)])
async def ingest_status():
    """
    Return current ingest buffer statistics.

      enabled         : false when PFC_INGEST_DIR is not set
      buffer_rows     : rows currently in the buffer
      buffer_bytes    : buffer file size in bytes
      buffer_mb       : buffer file size in MB
      last_flush_age_sec : seconds since last successful flush
      last_file       : path of the last compressed .pfc file
      rotate_mb       : size rotation threshold (MB)
      rotate_sec      : time rotation threshold (seconds)
      ingest_dir      : configured ingest directory
    """
    if _ingest_lock is None:
        return {"enabled": False}

    buf_bytes = 0
    if _ingest_buffer and _ingest_buffer.exists():
        buf_bytes = _ingest_buffer.stat().st_size

    return {
        "enabled":            True,
        "buffer_rows":        _ingest_row_count,
        "buffer_bytes":       buf_bytes,
        "buffer_mb":          round(buf_bytes / (1024 * 1024), 3),
        "last_flush_age_sec": round(time.time() - _ingest_last_flush, 1),
        "last_file":          _ingest_last_file,
        "rotate_mb":          INGEST_ROTATE_MB,
        "rotate_sec":         INGEST_ROTATE_SEC,
        "ingest_dir":         INGEST_DIR,
    }


# ---------------------------------------------------------------------------
# Grafana SimpleJSON Data Source
# (https://grafana.com/grafana/plugins/grafana-simple-json-datasource/)
# ---------------------------------------------------------------------------

@app.get("/grafana", dependencies=[Depends(require_api_key)])
async def grafana_health():
    """Grafana datasource health check — must return 200."""
    return JSONResponse(content="ok")


class GrafanaSearchRequest(BaseModel):
    target: str = ""


class GrafanaQueryTarget(BaseModel):
    target: str = ""       # PFC file path + optional filter JSON
    type: str = "table"    # "timeseries" or "table"
    refId: str = "A"


class GrafanaQueryRequest(BaseModel):
    range: dict
    targets: list[GrafanaQueryTarget]
    maxDataPoints: int = 1000


@app.post("/grafana/search", dependencies=[Depends(require_api_key)])
async def grafana_search(req: GrafanaSearchRequest):
    """
    Return available targets.
    Clients can type any S3/local path — we just echo it back.
    Future: could list objects in a configured S3 prefix.
    """
    return JSONResponse(content=[req.target] if req.target else ["s3://your-bucket/logs.pfc"])


@app.post("/grafana/query", dependencies=[Depends(require_api_key)])
async def grafana_query(req: GrafanaQueryRequest):
    """
    Grafana SimpleJSON query endpoint.

    Target format:  s3://bucket/logs_march.pfc
    Optional filter appended as JSON:  s3://bucket/logs.pfc|{"level":"ERROR"}

    Returns table format with all columns from the PFC rows.
    """
    from_str = req.range.get("from", "")
    to_str   = req.range.get("to", "")
    try:
        from_dt = _parse_ts(from_str)
        to_dt   = _parse_ts(to_str)
    except Exception:
        from_dt = to_dt = None

    results = []
    for tgt in req.targets:
        # Parse target: "s3://bucket/file.pfc" or "s3://bucket/file.pfc|{json filter}"
        parts = tgt.target.split("|", 1)
        pfc_file = parts[0].strip()
        filter_expr: dict | None = None
        if len(parts) == 2:
            try:
                filter_expr = json.loads(parts[1])
            except json.JSONDecodeError:
                pass

        if not pfc_file.endswith(".pfc"):
            continue

        # Collect rows (capped at maxDataPoints)
        rows: list[dict] = []
        try:
            gen = (
                _run_query_s3(pfc_file, from_dt, to_dt, filter_expr)
                if _is_s3(pfc_file)
                else _run_query_local(pfc_file, from_dt, to_dt, filter_expr)
            )
            for line in gen:
                if len(rows) >= req.maxDataPoints:
                    break
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
        except Exception as exc:
            log.warning("Query error for %s: %s", pfc_file, exc)
            continue

        if not rows:
            continue

        # Build Grafana table response
        all_cols = list(dict.fromkeys(k for r in rows for k in r))
        columns = [{"text": c, "type": "string"} for c in all_cols]
        table_rows = [[r.get(c, "") for c in all_cols] for r in rows]

        results.append({
            "type": "table",
            "refId": tgt.refId,
            "columns": columns,
            "rows": table_rows,
        })

    return JSONResponse(content=results)


@app.post("/grafana/annotations", dependencies=[Depends(require_api_key)])
async def grafana_annotations(request: Request):
    return JSONResponse(content=[])


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    import uvicorn

    parser = argparse.ArgumentParser(
        prog="pfc-gateway",
        description="PFC cold archive HTTP REST gateway (bidirectional: query + ingest)",
    )
    parser.add_argument("--host",       default=os.environ.get("PFC_HOST", "0.0.0.0"))
    parser.add_argument("--port",       type=int, default=int(os.environ.get("PFC_PORT", "8765")))
    parser.add_argument("--api-key",    default=None,
                        help="API key (overrides PFC_API_KEY env var)")
    parser.add_argument("--binary",     default=None,
                        help="Path to pfc_jsonl binary (overrides PFC_JSONL_BINARY env var)")
    parser.add_argument("--ingest-dir", default=None,
                        help="Enable ingest and write buffers here (overrides PFC_INGEST_DIR)")
    parser.add_argument("--version",    action="version", version=f"pfc-gateway {__version__}")
    args = parser.parse_args()

    import sys

    if args.api_key:
        os.environ["PFC_API_KEY"] = args.api_key
        sys.modules[__name__].API_KEY = args.api_key
    if args.binary:
        os.environ["PFC_JSONL_BINARY"] = args.binary
        sys.modules[__name__].PFC_JSONL_BINARY = args.binary
    if args.ingest_dir:
        os.environ["PFC_INGEST_DIR"] = args.ingest_dir
        sys.modules[__name__].INGEST_DIR = args.ingest_dir

    print(f"pfc-gateway {__version__} — listening on {args.host}:{args.port}")
    print(f"  Binary     : {PFC_JSONL_BINARY}")
    print(f"  Auth       : {'enabled' if API_KEY else 'DISABLED (set PFC_API_KEY or --api-key)'}")
    print(f"  Ingest     : {INGEST_DIR or 'disabled (set PFC_INGEST_DIR or --ingest-dir)'}")

    uvicorn.run(
        "pfc_gateway:app",
        host=args.host,
        port=args.port,
        reload=False,
        log_level="info",
    )
