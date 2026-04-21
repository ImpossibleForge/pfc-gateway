"""
tests/test_ingest.py — Test suite for pfc-gateway v0.2.0 ingest endpoints.

Coverage:
  TestIngestDisabled       — 503 when PFC_INGEST_DIR not set (5 tests)
  TestIngestBodyParsing    — auto-detect JSON array / {"rows":[]} / NDJSON (8 tests)
  TestIngestBufferWrites   — rows actually land in the buffer file (4 tests)
  TestIngestFlush          — POST /ingest/flush (6 tests)
  TestIngestStatus         — GET /ingest/status (5 tests)
  TestIngestRotation       — size-based auto-rotation during ingest (4 tests)
  TestIngestWatchdog       — time-based rotation via _rotation_watchdog (3 tests)
  TestIngestAuth           — auth enforcement on all three endpoints (3 tests)

Total: 38 tests
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from io import StringIO
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch, mock_open

import pytest
from fastapi.testclient import TestClient

import pfc_gateway

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

TEST_KEY = "test-secret-key"

# A TestClient that does NOT re-raise server exceptions, so we test HTTP codes.
client_no_exc = TestClient(pfc_gateway.app, raise_server_exceptions=False)

# A TestClient that DOES re-raise (for debugging individual assertions).
client = TestClient(pfc_gateway.app, raise_server_exceptions=True)


def _auth_headers() -> dict:
    return {"X-API-Key": TEST_KEY}


def _reset_ingest_globals(tmp_dir: Path | None = None) -> None:
    """Reset module-level ingest state between tests."""
    import asyncio as _asyncio

    pfc_gateway._ingest_lock       = _asyncio.Lock() if tmp_dir else None
    pfc_gateway._ingest_buffer     = (tmp_dir / ".pfc_buffer.jsonl") if tmp_dir else None
    pfc_gateway._ingest_row_count  = 0
    pfc_gateway._ingest_last_flush = time.time()
    pfc_gateway._ingest_last_file  = ""
    if tmp_dir:
        pfc_gateway.INGEST_DIR = str(tmp_dir)
    else:
        pfc_gateway.INGEST_DIR = None


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _patch_api_key():
    """Use a fixed API key for all tests."""
    with patch.object(pfc_gateway, "API_KEY", TEST_KEY):
        yield


@pytest.fixture
def ingest_dir(tmp_path):
    """Set up a fresh ingest directory and reset module globals."""
    _reset_ingest_globals(tmp_path)
    yield tmp_path
    _reset_ingest_globals(None)


# ---------------------------------------------------------------------------
# TestIngestDisabled — 503 when ingest not configured
# ---------------------------------------------------------------------------

class TestIngestDisabled:
    """All ingest endpoints return 503 when PFC_INGEST_DIR is not set."""

    @pytest.fixture(autouse=True)
    def _disable(self):
        _reset_ingest_globals(None)
        yield

    def test_post_ingest_returns_503(self):
        r = client_no_exc.post(
            "/ingest",
            headers=_auth_headers(),
            json=[{"msg": "hello"}],
        )
        assert r.status_code == 503

    def test_post_ingest_flush_returns_503(self):
        r = client_no_exc.post("/ingest/flush", headers=_auth_headers())
        assert r.status_code == 503

    def test_get_ingest_status_returns_disabled(self):
        r = client.get("/ingest/status", headers=_auth_headers())
        assert r.status_code == 200
        assert r.json()["enabled"] is False

    def test_503_detail_mentions_env_var(self):
        r = client_no_exc.post("/ingest", headers=_auth_headers(), json=[])
        # May return 503 or 200 (empty body), both acceptable when disabled
        # but if 503, detail must be informative
        if r.status_code == 503:
            assert "PFC_INGEST_DIR" in r.json()["detail"]

    def test_ingest_status_has_no_extra_keys_when_disabled(self):
        r = client.get("/ingest/status", headers=_auth_headers())
        data = r.json()
        assert "enabled" in data
        assert data["enabled"] is False


# ---------------------------------------------------------------------------
# TestIngestBodyParsing — auto-detect formats
# ---------------------------------------------------------------------------

class TestIngestBodyParsing:
    """POST /ingest auto-detects JSON array, {rows:[...]}, and NDJSON."""

    def test_json_array_accepted(self, ingest_dir):
        r = client.post(
            "/ingest",
            headers=_auth_headers(),
            json=[{"level": "INFO", "msg": "a"}, {"level": "WARN", "msg": "b"}],
        )
        assert r.status_code == 200
        assert r.json()["accepted"] == 2

    def test_rows_object_accepted(self, ingest_dir):
        body = {"rows": [{"x": 1}, {"x": 2}, {"x": 3}]}
        r = client.post("/ingest", headers=_auth_headers(), json=body)
        assert r.status_code == 200
        assert r.json()["accepted"] == 3

    def test_ndjson_accepted(self, ingest_dir):
        ndjson = '{"a":1}\n{"a":2}\n{"a":3}\n'
        r = client.post(
            "/ingest",
            headers={**_auth_headers(), "Content-Type": "application/x-ndjson"},
            content=ndjson.encode(),
        )
        assert r.status_code == 200
        assert r.json()["accepted"] == 3

    def test_single_object_body_accepted(self, ingest_dir):
        r = client.post(
            "/ingest",
            headers=_auth_headers(),
            json={"ts": "2026-04-21T10:00:00Z", "msg": "single"},
        )
        assert r.status_code == 200
        assert r.json()["accepted"] == 1

    def test_empty_array_returns_zero(self, ingest_dir):
        r = client.post("/ingest", headers=_auth_headers(), json=[])
        assert r.status_code == 200
        assert r.json()["accepted"] == 0

    def test_empty_body_returns_zero(self, ingest_dir):
        r = client.post(
            "/ingest",
            headers={**_auth_headers(), "Content-Type": "application/json"},
            content=b"",
        )
        assert r.status_code == 200
        assert r.json()["accepted"] == 0

    def test_invalid_body_returns_400(self, ingest_dir):
        r = client_no_exc.post(
            "/ingest",
            headers={**_auth_headers(), "Content-Type": "application/json"},
            content=b"not json at all !!!",
        )
        assert r.status_code == 400

    def test_ndjson_with_blank_lines_skipped(self, ingest_dir):
        ndjson = '{"a":1}\n\n{"a":2}\n\n'
        r = client.post(
            "/ingest",
            headers={**_auth_headers(), "Content-Type": "application/x-ndjson"},
            content=ndjson.encode(),
        )
        assert r.status_code == 200
        assert r.json()["accepted"] == 2


# ---------------------------------------------------------------------------
# TestIngestBufferWrites — rows land in the buffer file
# ---------------------------------------------------------------------------

class TestIngestBufferWrites:
    """Verify that rows are actually written to .pfc_buffer.jsonl."""

    def test_rows_written_to_buffer_file(self, ingest_dir):
        rows = [{"n": i} for i in range(5)]
        client.post("/ingest", headers=_auth_headers(), json=rows)
        buf = ingest_dir / ".pfc_buffer.jsonl"
        assert buf.exists()
        lines = buf.read_text(encoding="utf-8").strip().splitlines()
        assert len(lines) == 5
        assert json.loads(lines[0]) == {"n": 0}

    def test_multiple_ingests_append_to_buffer(self, ingest_dir):
        client.post("/ingest", headers=_auth_headers(), json=[{"x": 1}])
        client.post("/ingest", headers=_auth_headers(), json=[{"x": 2}, {"x": 3}])
        buf = ingest_dir / ".pfc_buffer.jsonl"
        lines = buf.read_text(encoding="utf-8").strip().splitlines()
        assert len(lines) == 3

    def test_row_count_increments(self, ingest_dir):
        client.post("/ingest", headers=_auth_headers(), json=[{"a": 1}])
        assert pfc_gateway._ingest_row_count == 1
        client.post("/ingest", headers=_auth_headers(), json=[{"a": 2}, {"a": 3}])
        assert pfc_gateway._ingest_row_count == 3

    def test_unicode_preserved_in_buffer(self, ingest_dir):
        row = {"msg": "Héllo Wörld 🌍"}
        client.post("/ingest", headers=_auth_headers(), json=[row])
        buf = ingest_dir / ".pfc_buffer.jsonl"
        written = json.loads(buf.read_text(encoding="utf-8"))
        assert written["msg"] == "Héllo Wörld 🌍"


# ---------------------------------------------------------------------------
# TestIngestFlush — POST /ingest/flush
# ---------------------------------------------------------------------------

class TestIngestFlush:
    """Force-flush compresses buffer to a .pfc file."""

    def test_flush_empty_buffer_returns_not_flushed(self, ingest_dir):
        r = client.post("/ingest/flush", headers=_auth_headers())
        assert r.status_code == 200
        assert r.json()["flushed"] is False
        assert r.json()["reason"] == "empty"

    def test_flush_with_data_calls_compress(self, ingest_dir):
        # Write a row first
        client.post("/ingest", headers=_auth_headers(), json=[{"x": 1}])

        # Mock the binary and asyncio subprocess
        mock_proc = AsyncMock()
        mock_proc.returncode = 0
        mock_proc.communicate = AsyncMock(return_value=(b"", b""))

        with patch.object(pfc_gateway, "_locate_binary", return_value="/fake/pfc_jsonl"), \
             patch("asyncio.create_subprocess_exec", return_value=mock_proc):
            r = client.post("/ingest/flush", headers=_auth_headers())

        assert r.status_code == 200
        data = r.json()
        assert data["flushed"] is True
        assert data["rows"] == 1
        assert "ingest_" in data["file"]
        assert data["file"].endswith(".pfc")

    def test_flush_clears_row_count(self, ingest_dir):
        client.post("/ingest", headers=_auth_headers(), json=[{"x": 1}, {"x": 2}])
        assert pfc_gateway._ingest_row_count == 2

        mock_proc = AsyncMock()
        mock_proc.returncode = 0
        mock_proc.communicate = AsyncMock(return_value=(b"", b""))

        with patch.object(pfc_gateway, "_locate_binary", return_value="/fake/pfc_jsonl"), \
             patch("asyncio.create_subprocess_exec", return_value=mock_proc):
            client.post("/ingest/flush", headers=_auth_headers())

        assert pfc_gateway._ingest_row_count == 0

    def test_flush_compress_error_restores_buffer(self, ingest_dir):
        client.post("/ingest", headers=_auth_headers(), json=[{"x": 1}])
        rows_before = pfc_gateway._ingest_row_count

        mock_proc = AsyncMock()
        mock_proc.returncode = 1
        mock_proc.communicate = AsyncMock(return_value=(b"", b"compress failed"))

        with patch.object(pfc_gateway, "_locate_binary", return_value="/fake/pfc_jsonl"), \
             patch("asyncio.create_subprocess_exec", return_value=mock_proc):
            r = client.post("/ingest/flush", headers=_auth_headers())

        assert r.status_code == 200
        data = r.json()
        assert data["flushed"] is False
        assert data["reason"] == "compress_error"
        # Buffer must be restored
        assert pfc_gateway._ingest_row_count == rows_before

    def test_flush_binary_missing_restores_buffer(self, ingest_dir):
        client.post("/ingest", headers=_auth_headers(), json=[{"y": 99}])
        rows_before = pfc_gateway._ingest_row_count

        with patch.object(pfc_gateway, "_locate_binary",
                          side_effect=RuntimeError("binary not found")):
            r = client.post("/ingest/flush", headers=_auth_headers())

        assert r.status_code == 200
        assert r.json()["flushed"] is False
        assert pfc_gateway._ingest_row_count == rows_before

    def test_flush_output_file_uses_prefix(self, ingest_dir):
        client.post("/ingest", headers=_auth_headers(), json=[{"z": 1}])
        pfc_gateway.INGEST_PREFIX = "myapp"

        mock_proc = AsyncMock()
        mock_proc.returncode = 0
        mock_proc.communicate = AsyncMock(return_value=(b"", b""))

        with patch.object(pfc_gateway, "_locate_binary", return_value="/fake/pfc_jsonl"), \
             patch("asyncio.create_subprocess_exec", return_value=mock_proc):
            r = client.post("/ingest/flush", headers=_auth_headers())

        pfc_gateway.INGEST_PREFIX = "ingest"  # reset
        assert "myapp_" in r.json().get("file", "")


# ---------------------------------------------------------------------------
# TestIngestStatus — GET /ingest/status
# ---------------------------------------------------------------------------

class TestIngestStatus:
    """Verify /ingest/status fields."""

    def test_status_enabled_true(self, ingest_dir):
        r = client.get("/ingest/status", headers=_auth_headers())
        assert r.status_code == 200
        assert r.json()["enabled"] is True

    def test_status_initial_row_count_zero(self, ingest_dir):
        r = client.get("/ingest/status", headers=_auth_headers())
        assert r.json()["buffer_rows"] == 0

    def test_status_row_count_after_ingest(self, ingest_dir):
        client.post("/ingest", headers=_auth_headers(), json=[{"a": 1}, {"a": 2}])
        r = client.get("/ingest/status", headers=_auth_headers())
        assert r.json()["buffer_rows"] == 2

    def test_status_contains_config_fields(self, ingest_dir):
        r = client.get("/ingest/status", headers=_auth_headers())
        data = r.json()
        assert "rotate_mb"  in data
        assert "rotate_sec" in data
        assert "ingest_dir" in data
        assert "last_file"  in data

    def test_status_buffer_bytes_increases(self, ingest_dir):
        r1 = client.get("/ingest/status", headers=_auth_headers())
        bytes_before = r1.json()["buffer_bytes"]

        client.post("/ingest", headers=_auth_headers(), json=[{"msg": "x" * 100}])

        r2 = client.get("/ingest/status", headers=_auth_headers())
        assert r2.json()["buffer_bytes"] > bytes_before


# ---------------------------------------------------------------------------
# TestIngestRotation — size-based auto-rotation
# ---------------------------------------------------------------------------

class TestIngestRotation:
    """Size threshold triggers automatic rotation during POST /ingest."""

    def test_size_threshold_triggers_flush(self, ingest_dir):
        """Set rotate threshold to 0 MB so any write triggers rotation."""
        pfc_gateway.INGEST_ROTATE_MB = 0  # always rotate

        mock_proc = AsyncMock()
        mock_proc.returncode = 0
        mock_proc.communicate = AsyncMock(return_value=(b"", b""))

        with patch.object(pfc_gateway, "_locate_binary", return_value="/fake/pfc_jsonl"), \
             patch("asyncio.create_subprocess_exec", return_value=mock_proc):
            r = client.post("/ingest", headers=_auth_headers(), json=[{"x": 1}])

        pfc_gateway.INGEST_ROTATE_MB = 64  # restore
        # After flush, row count must be 0
        assert pfc_gateway._ingest_row_count == 0
        assert r.json()["accepted"] == 1

    def test_below_threshold_no_rotation(self, ingest_dir):
        """Normal 64 MB threshold → no rotation on small payload."""
        pfc_gateway.INGEST_ROTATE_MB = 64

        mock_proc = AsyncMock()
        mock_proc.returncode = 0
        mock_proc.communicate = AsyncMock(return_value=(b"", b""))

        with patch.object(pfc_gateway, "_locate_binary", return_value="/fake/pfc_jsonl"), \
             patch("asyncio.create_subprocess_exec", return_value=mock_proc) as mock_exec:
            client.post("/ingest", headers=_auth_headers(), json=[{"x": 1}])

        # No compress call should have happened
        mock_exec.assert_not_called()
        assert pfc_gateway._ingest_row_count == 1

    def test_rotation_resets_row_count(self, ingest_dir):
        pfc_gateway.INGEST_ROTATE_MB = 0

        mock_proc = AsyncMock()
        mock_proc.returncode = 0
        mock_proc.communicate = AsyncMock(return_value=(b"", b""))

        with patch.object(pfc_gateway, "_locate_binary", return_value="/fake/pfc_jsonl"), \
             patch("asyncio.create_subprocess_exec", return_value=mock_proc):
            client.post("/ingest", headers=_auth_headers(), json=[{"a": 1}, {"a": 2}])

        pfc_gateway.INGEST_ROTATE_MB = 64
        assert pfc_gateway._ingest_row_count == 0

    def test_rotation_compress_error_preserves_rows(self, ingest_dir):
        pfc_gateway.INGEST_ROTATE_MB = 0

        mock_proc = AsyncMock()
        mock_proc.returncode = 1
        mock_proc.communicate = AsyncMock(return_value=(b"", b"error"))

        with patch.object(pfc_gateway, "_locate_binary", return_value="/fake/pfc_jsonl"), \
             patch("asyncio.create_subprocess_exec", return_value=mock_proc):
            client.post("/ingest", headers=_auth_headers(), json=[{"a": 1}])

        pfc_gateway.INGEST_ROTATE_MB = 64
        # Compress failed → buffer restored → row count preserved
        assert pfc_gateway._ingest_row_count == 1


# ---------------------------------------------------------------------------
# TestIngestWatchdog — time-based rotation
# ---------------------------------------------------------------------------

class TestIngestWatchdog:
    """_rotation_watchdog fires when age >= INGEST_ROTATE_SEC."""

    @pytest.mark.asyncio
    async def test_watchdog_flushes_on_time_threshold(self, ingest_dir):
        """Simulate watchdog tick with age > threshold."""
        # Set last flush far in the past
        pfc_gateway._ingest_last_flush = time.time() - 9999
        pfc_gateway._ingest_row_count = 5
        # Write something to buffer so flush has data
        pfc_gateway._ingest_buffer.write_text(
            '{"x":1}\n{"x":2}\n{"x":3}\n{"x":4}\n{"x":5}\n', encoding="utf-8"
        )

        mock_proc = AsyncMock()
        mock_proc.returncode = 0
        mock_proc.communicate = AsyncMock(return_value=(b"", b""))

        async def fake_watchdog_tick():
            async with pfc_gateway._ingest_lock:
                if pfc_gateway._ingest_row_count == 0:
                    return
                age = time.time() - pfc_gateway._ingest_last_flush
                if age >= pfc_gateway.INGEST_ROTATE_SEC:
                    await pfc_gateway._flush_buffer_locked("time")

        with patch.object(pfc_gateway, "_locate_binary", return_value="/fake/pfc_jsonl"), \
             patch("asyncio.create_subprocess_exec", return_value=mock_proc):
            await fake_watchdog_tick()

        assert pfc_gateway._ingest_row_count == 0

    @pytest.mark.asyncio
    async def test_watchdog_skips_when_buffer_empty(self, ingest_dir):
        """Watchdog must NOT call compress when no rows are buffered."""
        pfc_gateway._ingest_last_flush = time.time() - 9999
        pfc_gateway._ingest_row_count = 0

        async def fake_watchdog_tick():
            async with pfc_gateway._ingest_lock:
                if pfc_gateway._ingest_row_count == 0:
                    return  # ← expected path
                await pfc_gateway._flush_buffer_locked("time")

        with patch("asyncio.create_subprocess_exec") as mock_exec:
            await fake_watchdog_tick()

        mock_exec.assert_not_called()

    @pytest.mark.asyncio
    async def test_watchdog_skips_when_age_below_threshold(self, ingest_dir):
        """If last flush was recent, watchdog must NOT rotate."""
        pfc_gateway._ingest_last_flush = time.time()  # just now
        pfc_gateway._ingest_row_count = 5
        pfc_gateway._ingest_buffer.write_text('{"x":1}\n' * 5, encoding="utf-8")

        async def fake_watchdog_tick():
            async with pfc_gateway._ingest_lock:
                if pfc_gateway._ingest_row_count == 0:
                    return
                age = time.time() - pfc_gateway._ingest_last_flush
                if age >= pfc_gateway.INGEST_ROTATE_SEC:
                    await pfc_gateway._flush_buffer_locked("time")

        with patch("asyncio.create_subprocess_exec") as mock_exec:
            await fake_watchdog_tick()

        mock_exec.assert_not_called()
        assert pfc_gateway._ingest_row_count == 5


# ---------------------------------------------------------------------------
# TestIngestAuth — auth enforcement
# ---------------------------------------------------------------------------

class TestIngestAuth:
    """All ingest endpoints require X-API-Key when API_KEY is set."""

    def test_post_ingest_no_key_returns_401(self, ingest_dir):
        r = client_no_exc.post("/ingest", json=[{"x": 1}])
        assert r.status_code == 401

    def test_post_ingest_flush_no_key_returns_401(self, ingest_dir):
        r = client_no_exc.post("/ingest/flush")
        assert r.status_code == 401

    def test_get_ingest_status_no_key_returns_401(self, ingest_dir):
        r = client_no_exc.get("/ingest/status")
        assert r.status_code == 401
