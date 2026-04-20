"""
pfc-gateway v0.1.0 — HTTP REST query server for PFC cold archives.

Makes PFC archives queryable by ANY tool — Grafana, Python, curl, PowerBI —
without requiring DuckDB or any client library beyond basic HTTP.

Supported query paths
---------------------
  Local  : PFC file on the server's local filesystem or NFS
  S3     : PFC archive on AWS S3 / GCS / Azure Blob (S3-compatible)

Architecture
------------
  Client  →  POST /query (JSON body)
          →  pfc-gateway generates pre-signed URLs (S3 mode)
          →  pfc_jsonl s3-fetch --from --to (downloads only needed blocks)
          →  pfc-gateway row-filters + streams NDJSON back

Grafana SimpleJSON Data Source is also supported:
  GET  /            → health check
  POST /search      → list available metrics (file paths)
  POST /query       → time series or table data
  POST /annotations → empty (not implemented)

Usage
-----
  pip install fastapi uvicorn boto3 python-dateutil
  PFC_API_KEY=secret uvicorn pfc_gateway:app --host 0.0.0.0 --port 8765

  # Query a local file
  curl -H "X-API-Key: secret" -X POST http://localhost:8765/query \\
    -H "Content-Type: application/json" \\
    -d '{"file":"path/to/logs.pfc","from_ts":"2026-03-01T00:00Z","to_ts":"2026-03-02T00:00Z"}'

  # Query an S3 file
  curl -H "X-API-Key: secret" -X POST http://localhost:8765/query \\
    -H "Content-Type: application/json" \\
    -d '{"file":"s3://my-bucket/logs_march.pfc","from_ts":"2026-03-05T10:00Z","to_ts":"2026-03-05T12:00Z"}'
"""

from __future__ import annotations

__version__ = "0.1.1"

import json
import logging
import os
import shutil
import subprocess
import tempfile
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

PFC_JSONL_BINARY: str = (
    os.environ.get("PFC_JSONL_BINARY")
    or shutil.which("pfc_jsonl")
    or "/usr/local/bin/pfc_jsonl"
)

API_KEY: str | None = os.environ.get("PFC_API_KEY")  # None = auth disabled (dev mode)

PRESIGN_TTL: int = int(os.environ.get("PFC_PRESIGN_TTL", "3600"))  # seconds

AWS_REGION: str = os.environ.get("AWS_DEFAULT_REGION", "eu-central-1")

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
    description="HTTP REST query server for PFC cold archives. "
                "Query PFC files on S3 or local storage without DuckDB.",
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


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get("/", dependencies=[Depends(require_api_key)])
async def health():
    return {"status": "ok", "version": __version__, "binary": PFC_JSONL_BINARY}


@app.post("/query", dependencies=[Depends(require_api_key)])
async def query(req: QueryRequest):
    """
    Query a PFC archive for a time range.

    Returns an NDJSON stream — one JSON object per line.

    Body:
      file      : local path or s3://bucket/key.pfc
      from_ts   : ISO 8601 start time (inclusive)
      to_ts     : ISO 8601 end time (exclusive)
      filter    : optional equality filter {"level": "ERROR"}
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
# Multi-file batch query
# ---------------------------------------------------------------------------

class BatchQueryRequest(BaseModel):
    files: list[str]
    from_ts: Optional[str] = None
    to_ts: Optional[str] = None
    filter: Optional[dict[str, Any]] = None
    aws_profile: Optional[str] = None


@app.post("/query/batch", dependencies=[Depends(require_api_key)])
async def query_batch(req: BatchQueryRequest):
    """
    Query multiple PFC files in one request and stream combined NDJSON.
    Files are queried in order — useful for multi-month archives.

    Body:
      files     : list of local paths or s3:// URIs ending in .pfc
      from_ts   : ISO 8601 start (inclusive)
      to_ts     : ISO 8601 end (exclusive)
      filter    : optional equality filter
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
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    import uvicorn

    parser = argparse.ArgumentParser(
        prog="pfc-gateway",
        description="PFC cold archive HTTP REST query server",
    )
    parser.add_argument("--host",    default=os.environ.get("PFC_HOST", "0.0.0.0"))
    parser.add_argument("--port",    type=int, default=int(os.environ.get("PFC_PORT", "8765")))
    parser.add_argument("--api-key", default=None,
                        help="API key (overrides PFC_API_KEY env var)")
    parser.add_argument("--binary",  default=None,
                        help="Path to pfc_jsonl binary (overrides PFC_JSONL_BINARY env var)")
    parser.add_argument("--version", action="version", version=f"pfc-gateway {__version__}")
    args = parser.parse_args()

    # Apply CLI overrides to module-level globals BEFORE uvicorn starts
    if args.api_key:
        os.environ["PFC_API_KEY"] = args.api_key
        # Reload module-level constant
        import sys
        sys.modules[__name__].API_KEY = args.api_key
    if args.binary:
        os.environ["PFC_JSONL_BINARY"] = args.binary
        sys.modules[__name__].PFC_JSONL_BINARY = args.binary

    print(f"pfc-gateway {__version__} — listening on {args.host}:{args.port}")
    print(f"  Binary : {PFC_JSONL_BINARY}")
    print(f"  Auth   : {'enabled' if API_KEY else 'DISABLED (set PFC_API_KEY or --api-key)'}")

    uvicorn.run(
        "pfc_gateway:app",
        host=args.host,
        port=args.port,
        reload=False,
        log_level="info",
    )
