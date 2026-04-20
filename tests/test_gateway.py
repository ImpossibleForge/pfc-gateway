"""
Unit / integration tests for pfc-gateway
==========================================
Uses FastAPI TestClient — no live server needed.
subprocess.Popen and boto3 are mocked throughout.

Run with:  python -m pytest tests/ -v
"""

import io
import json
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent))

from starlette.testclient import TestClient
import pfc_gateway
from pfc_gateway import (
    app,
    _parse_ts,
    _is_s3,
    _parse_s3_path,
    _row_in_range,
    _apply_filter,
    _fmt_ts,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

TEST_KEY = "test-secret-key"


def _make_client(api_key=TEST_KEY):
    """TestClient with optional API key pre-set."""
    return TestClient(app, raise_server_exceptions=False)


def _auth(key=TEST_KEY):
    return {"X-API-Key": key}


def _make_popen(lines: list[str], returncode: int = 0):
    """Build a mock Popen whose stdout yields the given lines."""
    content = "".join(line if line.endswith("\n") else line + "\n" for line in lines)
    mock_proc         = MagicMock()
    mock_proc.stdout  = io.StringIO(content)
    mock_proc.stderr  = MagicMock()
    mock_proc.stderr.read.return_value = "some stderr"
    mock_proc.returncode = returncode
    mock_proc.wait.return_value = returncode
    return mock_proc


# ===========================================================================
# 1. Auth
# ===========================================================================

class TestAuth(unittest.TestCase):

    def setUp(self):
        self.client = _make_client()

    @patch.object(pfc_gateway, "API_KEY", TEST_KEY)
    def test_missing_key_returns_401(self):
        r = self.client.get("/")
        self.assertEqual(r.status_code, 401)

    @patch.object(pfc_gateway, "API_KEY", TEST_KEY)
    def test_wrong_key_returns_401(self):
        r = self.client.get("/", headers={"X-API-Key": "wrong"})
        self.assertEqual(r.status_code, 401)

    @patch.object(pfc_gateway, "API_KEY", TEST_KEY)
    @patch("pfc_gateway.Path")
    def test_correct_key_returns_200(self, mock_path):
        mock_path.return_value.exists.return_value = True
        r = self.client.get("/", headers=_auth())
        self.assertEqual(r.status_code, 200)

    @patch.object(pfc_gateway, "API_KEY", None)
    @patch("pfc_gateway.Path")
    def test_no_api_key_configured_allows_all(self, mock_path):
        """Dev mode: no PFC_API_KEY set → all requests pass through."""
        mock_path.return_value.exists.return_value = True
        r = self.client.get("/")
        self.assertEqual(r.status_code, 200)

    @patch.object(pfc_gateway, "API_KEY", TEST_KEY)
    def test_401_body_contains_helpful_message(self):
        r = self.client.get("/")
        body = r.json()
        self.assertIn("detail", body)
        self.assertIn("API key", body["detail"])


# ===========================================================================
# 2. _parse_ts
# ===========================================================================

class TestParseTs(unittest.TestCase):

    def test_none_returns_none(self):
        self.assertIsNone(_parse_ts(None))

    def test_empty_string_returns_none(self):
        self.assertIsNone(_parse_ts(""))

    def test_iso_with_z_suffix(self):
        dt = _parse_ts("2026-03-01T14:00:00Z")
        self.assertEqual(dt.year, 2026)
        self.assertEqual(dt.tzinfo, timezone.utc)

    def test_iso_with_offset(self):
        dt = _parse_ts("2026-03-01T14:00:00+02:00")
        self.assertEqual(dt.tzinfo, timezone.utc)
        self.assertEqual(dt.hour, 12)  # 14:00+02 = 12:00 UTC

    def test_naive_datetime_treated_as_utc(self):
        dt = _parse_ts("2026-03-01T14:00:00")
        self.assertIsNotNone(dt.tzinfo)
        self.assertEqual(dt.tzinfo, timezone.utc)

    def test_date_only(self):
        dt = _parse_ts("2026-03-01")
        self.assertEqual(dt.year, 2026)
        self.assertEqual(dt.month, 3)


# ===========================================================================
# 3. _is_s3
# ===========================================================================

class TestIsS3(unittest.TestCase):

    def test_s3_prefix(self):
        self.assertTrue(_is_s3("s3://my-bucket/logs.pfc"))

    def test_s3a_prefix(self):
        self.assertTrue(_is_s3("s3a://my-bucket/logs.pfc"))

    def test_local_path_is_not_s3(self):
        self.assertFalse(_is_s3("/var/log/pfc/logs.pfc"))

    def test_http_is_not_s3(self):
        self.assertFalse(_is_s3("http://example.com/logs.pfc"))


# ===========================================================================
# 4. _parse_s3_path
# ===========================================================================

class TestParseS3Path(unittest.TestCase):

    def test_simple_path(self):
        bucket, key = _parse_s3_path("s3://my-bucket/logs.pfc")
        self.assertEqual(bucket, "my-bucket")
        self.assertEqual(key, "logs.pfc")

    def test_deep_nested_key(self):
        bucket, key = _parse_s3_path("s3://my-bucket/2026/03/01/logs.pfc")
        self.assertEqual(bucket, "my-bucket")
        self.assertEqual(key, "2026/03/01/logs.pfc")

    def test_s3a_normalized(self):
        bucket, key = _parse_s3_path("s3a://my-bucket/logs.pfc")
        self.assertEqual(bucket, "my-bucket")
        self.assertEqual(key, "logs.pfc")

    def test_missing_key_raises_value_error(self):
        with self.assertRaises(ValueError):
            _parse_s3_path("s3://bucket-only")


# ===========================================================================
# 5. _row_in_range
# ===========================================================================

class TestRowInRange(unittest.TestCase):

    def setUp(self):
        self.from_dt = datetime(2026, 3, 1, 10, 0, tzinfo=timezone.utc)
        self.to_dt   = datetime(2026, 3, 1, 12, 0, tzinfo=timezone.utc)

    def test_row_in_range_included(self):
        row = {"timestamp": "2026-03-01T11:00:00Z", "level": "INFO"}
        self.assertTrue(_row_in_range(row, self.from_dt, self.to_dt))

    def test_row_before_from_excluded(self):
        row = {"timestamp": "2026-03-01T09:00:00Z", "level": "INFO"}
        self.assertFalse(_row_in_range(row, self.from_dt, self.to_dt))

    def test_row_at_or_after_to_excluded(self):
        row = {"timestamp": "2026-03-01T12:00:00Z", "level": "INFO"}
        self.assertFalse(_row_in_range(row, self.from_dt, self.to_dt))

    def test_no_timestamp_field_included(self):
        row = {"level": "INFO", "message": "no ts"}
        self.assertTrue(_row_in_range(row, self.from_dt, self.to_dt))

    def test_no_range_always_included(self):
        row = {"timestamp": "2026-03-01T11:00:00Z"}
        self.assertTrue(_row_in_range(row, None, None))

    def test_epoch_int_timestamp(self):
        """Epoch-second integer timestamp must be handled."""
        # 2026-03-01 11:00:00 UTC
        epoch = int(datetime(2026, 3, 1, 11, 0, tzinfo=timezone.utc).timestamp())
        row = {"timestamp": epoch}
        self.assertTrue(_row_in_range(row, self.from_dt, self.to_dt))

    def test_at_timestamp_key_also_checked(self):
        """@timestamp (Elastic/Grafana convention) must also be recognized."""
        row = {"@timestamp": "2026-03-01T11:00:00Z"}
        self.assertTrue(_row_in_range(row, self.from_dt, self.to_dt))

    def test_unparseable_timestamp_included(self):
        """Rows with unparseable timestamp must not be dropped."""
        row = {"timestamp": "not-a-date"}
        self.assertTrue(_row_in_range(row, self.from_dt, self.to_dt))


# ===========================================================================
# 6. _apply_filter
# ===========================================================================

class TestApplyFilter(unittest.TestCase):

    def test_none_filter_always_matches(self):
        self.assertTrue(_apply_filter({"level": "INFO"}, None))

    def test_empty_filter_always_matches(self):
        self.assertTrue(_apply_filter({"level": "INFO"}, {}))

    def test_single_key_match(self):
        self.assertTrue(_apply_filter({"level": "ERROR", "host": "web-01"}, {"level": "ERROR"}))

    def test_single_key_no_match(self):
        self.assertFalse(_apply_filter({"level": "INFO"}, {"level": "ERROR"}))

    def test_multi_key_all_match(self):
        row = {"level": "ERROR", "host": "web-01", "service": "api"}
        self.assertTrue(_apply_filter(row, {"level": "ERROR", "host": "web-01"}))

    def test_multi_key_one_mismatch(self):
        row = {"level": "ERROR", "host": "web-02"}
        self.assertFalse(_apply_filter(row, {"level": "ERROR", "host": "web-01"}))


# ===========================================================================
# 7. _fmt_ts
# ===========================================================================

class TestFmtTs(unittest.TestCase):

    def test_format_matches_pfc_jsonl_expectation(self):
        dt = datetime(2026, 3, 1, 14, 30, 0, tzinfo=timezone.utc)
        result = _fmt_ts(dt)
        self.assertEqual(result, "2026-03-01T14:30")

    def test_midnight(self):
        dt = datetime(2026, 3, 1, 0, 0, 0, tzinfo=timezone.utc)
        self.assertEqual(_fmt_ts(dt), "2026-03-01T00:00")


# ===========================================================================
# 8. Health endpoint
# ===========================================================================

class TestHealthEndpoint(unittest.TestCase):

    def setUp(self):
        self.client = _make_client()

    @patch.object(pfc_gateway, "API_KEY", TEST_KEY)
    @patch("pfc_gateway.Path")
    def test_health_returns_ok(self, mock_path):
        mock_path.return_value.exists.return_value = True
        r = self.client.get("/", headers=_auth())
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()["status"], "ok")

    @patch.object(pfc_gateway, "API_KEY", TEST_KEY)
    @patch("pfc_gateway.Path")
    def test_health_returns_version(self, mock_path):
        mock_path.return_value.exists.return_value = True
        r = self.client.get("/", headers=_auth())
        self.assertIn("version", r.json())

    @patch.object(pfc_gateway, "API_KEY", TEST_KEY)
    @patch("pfc_gateway.Path")
    def test_health_returns_binary_path(self, mock_path):
        mock_path.return_value.exists.return_value = True
        r = self.client.get("/", headers=_auth())
        self.assertIn("binary", r.json())


# ===========================================================================
# 9. POST /query — local file
# ===========================================================================

class TestQueryLocal(unittest.TestCase):

    def setUp(self):
        self.client = _make_client()

    @patch.object(pfc_gateway, "API_KEY", TEST_KEY)
    @patch("pfc_gateway.subprocess.Popen")
    @patch("pfc_gateway._locate_binary", return_value="/usr/bin/pfc_jsonl")
    @patch("pfc_gateway.Path")
    def test_local_query_returns_ndjson(self, mock_path, mock_binary, mock_popen):
        mock_path.return_value.exists.return_value = True
        rows = ['{"timestamp":"2026-03-01T11:00:00Z","level":"INFO","msg":"hello"}']
        mock_popen.return_value = _make_popen(rows)

        r = self.client.post("/query", headers=_auth(), json={
            "file": "/data/logs.pfc",
            "from_ts": "2026-03-01T10:00:00Z",
            "to_ts":   "2026-03-01T12:00:00Z",
        })
        self.assertEqual(r.status_code, 200)
        self.assertIn("ndjson", r.headers["content-type"])
        parsed = json.loads(r.text.strip())
        self.assertEqual(parsed["level"], "INFO")

    @patch.object(pfc_gateway, "API_KEY", TEST_KEY)
    @patch("pfc_gateway.Path")
    def test_file_not_found_returns_404(self, mock_path):
        mock_path.return_value.exists.return_value = False
        r = self.client.post("/query", headers=_auth(), json={
            "file": "/nonexistent/logs.pfc",
        })
        self.assertEqual(r.status_code, 404)

    @patch.object(pfc_gateway, "API_KEY", TEST_KEY)
    def test_non_pfc_extension_returns_422(self):
        r = self.client.post("/query", headers=_auth(), json={
            "file": "/data/logs.jsonl",
        })
        self.assertEqual(r.status_code, 422)

    @patch.object(pfc_gateway, "API_KEY", TEST_KEY)
    @patch("pfc_gateway.subprocess.Popen")
    @patch("pfc_gateway._locate_binary", return_value="/usr/bin/pfc_jsonl")
    @patch("pfc_gateway.Path")
    def test_filter_applied_to_results(self, mock_path, mock_binary, mock_popen):
        mock_path.return_value.exists.return_value = True
        rows = [
            '{"timestamp":"2026-03-01T11:00:00Z","level":"ERROR","msg":"fail"}',
            '{"timestamp":"2026-03-01T11:01:00Z","level":"INFO","msg":"ok"}',
        ]
        mock_popen.return_value = _make_popen(rows)

        r = self.client.post("/query", headers=_auth(), json={
            "file":    "/data/logs.pfc",
            "from_ts": "2026-03-01T10:00:00Z",
            "to_ts":   "2026-03-01T12:00:00Z",
            "filter":  {"level": "ERROR"},
        })
        self.assertEqual(r.status_code, 200)
        lines = [l for l in r.text.strip().splitlines() if l]
        self.assertEqual(len(lines), 1)
        self.assertEqual(json.loads(lines[0])["level"], "ERROR")

    @patch.object(pfc_gateway, "API_KEY", TEST_KEY)
    @patch("pfc_gateway.subprocess.Popen")
    @patch("pfc_gateway._locate_binary", return_value="/usr/bin/pfc_jsonl")
    @patch("pfc_gateway.Path")
    def test_no_time_range_uses_decompress(self, mock_path, mock_binary, mock_popen):
        """No from_ts/to_ts → pfc_jsonl decompress (not query)."""
        mock_path.return_value.exists.return_value = True
        mock_popen.return_value = _make_popen(['{"level":"INFO"}'])

        self.client.post("/query", headers=_auth(), json={"file": "/data/logs.pfc"})

        cmd = mock_popen.call_args[0][0]
        self.assertIn("decompress", cmd)
        self.assertNotIn("query", cmd)

    @patch.object(pfc_gateway, "API_KEY", TEST_KEY)
    @patch("pfc_gateway.subprocess.Popen")
    @patch("pfc_gateway._locate_binary", return_value="/usr/bin/pfc_jsonl")
    @patch("pfc_gateway.Path")
    def test_with_time_range_uses_query_command(self, mock_path, mock_binary, mock_popen):
        """With from_ts + to_ts → pfc_jsonl query (block-level index)."""
        mock_path.return_value.exists.return_value = True
        mock_popen.return_value = _make_popen(['{"level":"INFO","timestamp":"2026-03-01T11:00:00Z"}'])

        self.client.post("/query", headers=_auth(), json={
            "file":    "/data/logs.pfc",
            "from_ts": "2026-03-01T10:00:00Z",
            "to_ts":   "2026-03-01T12:00:00Z",
        })

        cmd = mock_popen.call_args[0][0]
        self.assertIn("query", cmd)
        self.assertIn("--from", cmd)
        self.assertIn("--to", cmd)


# ===========================================================================
# 10. POST /query — S3
# ===========================================================================

class TestQueryS3(unittest.TestCase):

    def setUp(self):
        self.client = _make_client()

    @patch.object(pfc_gateway, "API_KEY", TEST_KEY)
    @patch("pfc_gateway.subprocess.Popen")
    @patch("pfc_gateway._locate_binary", return_value="/usr/bin/pfc_jsonl")
    @patch("pfc_gateway.boto3.Session")
    def test_s3_query_generates_presigned_urls(self, mock_session, mock_binary, mock_popen):
        mock_s3 = MagicMock()
        mock_s3.generate_presigned_url.return_value = "https://s3.example.com/presigned"
        mock_session.return_value.client.return_value = mock_s3
        mock_popen.return_value = _make_popen(['{"timestamp":"2026-03-01T11:00:00Z","level":"INFO"}'])

        r = self.client.post("/query", headers=_auth(), json={
            "file":    "s3://my-bucket/logs.pfc",
            "from_ts": "2026-03-01T10:00:00Z",
            "to_ts":   "2026-03-01T12:00:00Z",
        })
        self.assertEqual(r.status_code, 200)
        mock_s3.generate_presigned_url.assert_called()

    @patch.object(pfc_gateway, "API_KEY", TEST_KEY)
    @patch("pfc_gateway.subprocess.Popen")
    @patch("pfc_gateway._locate_binary", return_value="/usr/bin/pfc_jsonl")
    @patch("pfc_gateway.boto3.Session")
    def test_s3a_path_normalized(self, mock_session, mock_binary, mock_popen):
        """s3a:// must be treated as s3://."""
        mock_s3 = MagicMock()
        mock_s3.generate_presigned_url.return_value = "https://presigned"
        mock_session.return_value.client.return_value = mock_s3
        mock_popen.return_value = _make_popen(['{"timestamp":"2026-03-01T11:00:00Z"}'])

        r = self.client.post("/query", headers=_auth(), json={
            "file":    "s3a://my-bucket/logs.pfc",
            "from_ts": "2026-03-01T10:00:00Z",
            "to_ts":   "2026-03-01T12:00:00Z",
        })
        self.assertEqual(r.status_code, 200)

    @patch.object(pfc_gateway, "API_KEY", TEST_KEY)
    @patch("pfc_gateway._locate_binary", return_value="/usr/bin/pfc_jsonl")
    @patch("pfc_gateway.boto3.Session")
    def test_s3_presign_failure_returns_500(self, mock_session, mock_binary):
        from botocore.exceptions import ClientError
        mock_s3 = MagicMock()
        mock_s3.generate_presigned_url.side_effect = ClientError(
            {"Error": {"Code": "403", "Message": "Forbidden"}}, "GeneratePresignedUrl"
        )
        mock_session.return_value.client.return_value = mock_s3

        r = self.client.post("/query", headers=_auth(), json={
            "file":    "s3://my-bucket/logs.pfc",
            "from_ts": "2026-03-01T10:00:00Z",
            "to_ts":   "2026-03-01T12:00:00Z",
        })
        self.assertEqual(r.status_code, 500)


# ===========================================================================
# 11. POST /query/batch
# ===========================================================================

class TestQueryBatch(unittest.TestCase):

    def setUp(self):
        self.client = _make_client()

    @patch.object(pfc_gateway, "API_KEY", TEST_KEY)
    @patch("pfc_gateway.subprocess.Popen")
    @patch("pfc_gateway._locate_binary", return_value="/usr/bin/pfc_jsonl")
    @patch("pfc_gateway.Path")
    def test_batch_combines_multiple_files(self, mock_path, mock_binary, mock_popen):
        mock_path.return_value.exists.return_value = True
        mock_popen.side_effect = [
            _make_popen(['{"timestamp":"2026-03-01T11:00:00Z","level":"INFO","f":"1"}',
                         '{"timestamp":"2026-03-01T11:01:00Z","level":"INFO","f":"1"}']),
            _make_popen(['{"timestamp":"2026-03-02T11:00:00Z","level":"ERROR","f":"2"}']),
        ]

        r = self.client.post("/query/batch", headers=_auth(), json={
            "files":   ["/data/logs_march01.pfc", "/data/logs_march02.pfc"],
            "from_ts": "2026-03-01T00:00:00Z",
            "to_ts":   "2026-03-03T00:00:00Z",
        })
        self.assertEqual(r.status_code, 200)
        lines = [l for l in r.text.strip().splitlines() if l]
        self.assertEqual(len(lines), 3)

    @patch.object(pfc_gateway, "API_KEY", TEST_KEY)
    @patch("pfc_gateway.subprocess.Popen")
    @patch("pfc_gateway._locate_binary", return_value="/usr/bin/pfc_jsonl")
    @patch("pfc_gateway.Path")
    def test_batch_skips_non_pfc_files(self, mock_path, mock_binary, mock_popen):
        mock_path.return_value.exists.return_value = True
        mock_popen.return_value = _make_popen(['{"level":"INFO","timestamp":"2026-03-01T11:00Z"}'])

        r = self.client.post("/query/batch", headers=_auth(), json={
            "files": ["/data/logs.pfc", "/data/README.md", "/data/data.jsonl"],
        })
        self.assertEqual(r.status_code, 200)
        # Only the .pfc file should trigger a Popen call
        self.assertEqual(mock_popen.call_count, 1)

    @patch.object(pfc_gateway, "API_KEY", TEST_KEY)
    @patch("pfc_gateway.subprocess.Popen")
    @patch("pfc_gateway._locate_binary", return_value="/usr/bin/pfc_jsonl")
    @patch("pfc_gateway.Path")
    def test_batch_one_file_fails_others_continue(self, mock_path, mock_binary, mock_popen):
        mock_path.return_value.exists.return_value = True

        def popen_side_effect(cmd, **kwargs):
            if "march01" in " ".join(cmd):
                raise RuntimeError("file not found")
            return _make_popen(['{"level":"INFO","timestamp":"2026-03-02T11:00Z"}'])

        mock_popen.side_effect = popen_side_effect

        r = self.client.post("/query/batch", headers=_auth(), json={
            "files": ["/data/march01.pfc", "/data/march02.pfc"],
        })
        self.assertEqual(r.status_code, 200)
        lines = [l for l in r.text.strip().splitlines() if l]
        self.assertEqual(len(lines), 1)

    @patch.object(pfc_gateway, "API_KEY", TEST_KEY)
    def test_batch_invalid_timestamp_returns_400(self):
        r = self.client.post("/query/batch", headers=_auth(), json={
            "files":   ["/data/logs.pfc"],
            "from_ts": "not-a-timestamp",
        })
        self.assertEqual(r.status_code, 400)


# ===========================================================================
# 12. Grafana endpoints
# ===========================================================================

class TestGrafana(unittest.TestCase):

    def setUp(self):
        self.client = _make_client()

    @patch.object(pfc_gateway, "API_KEY", TEST_KEY)
    def test_grafana_health_returns_200(self):
        r = self.client.get("/grafana", headers=_auth())
        self.assertEqual(r.status_code, 200)

    @patch.object(pfc_gateway, "API_KEY", TEST_KEY)
    def test_grafana_search_echoes_target(self):
        r = self.client.post("/grafana/search", headers=_auth(),
                             json={"target": "s3://my-bucket/logs.pfc"})
        self.assertEqual(r.status_code, 200)
        self.assertIn("s3://my-bucket/logs.pfc", r.json())

    @patch.object(pfc_gateway, "API_KEY", TEST_KEY)
    def test_grafana_search_empty_target_returns_placeholder(self):
        r = self.client.post("/grafana/search", headers=_auth(), json={"target": ""})
        self.assertEqual(r.status_code, 200)
        self.assertIsInstance(r.json(), list)
        self.assertGreater(len(r.json()), 0)

    @patch.object(pfc_gateway, "API_KEY", TEST_KEY)
    @patch("pfc_gateway.subprocess.Popen")
    @patch("pfc_gateway._locate_binary", return_value="/usr/bin/pfc_jsonl")
    @patch("pfc_gateway.Path")
    def test_grafana_query_returns_table_format(self, mock_path, mock_binary, mock_popen):
        mock_path.return_value.exists.return_value = True
        mock_popen.return_value = _make_popen([
            '{"timestamp":"2026-03-01T11:00:00Z","level":"INFO","msg":"ok"}',
        ])

        r = self.client.post("/grafana/query", headers=_auth(), json={
            "range": {"from": "2026-03-01T10:00:00Z", "to": "2026-03-01T12:00:00Z"},
            "targets": [{"target": "/data/logs.pfc", "type": "table", "refId": "A"}],
            "maxDataPoints": 100,
        })
        self.assertEqual(r.status_code, 200)
        results = r.json()
        self.assertIsInstance(results, list)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["type"], "table")
        self.assertIn("columns", results[0])
        self.assertIn("rows", results[0])

    @patch.object(pfc_gateway, "API_KEY", TEST_KEY)
    def test_grafana_annotations_returns_empty_list(self):
        r = self.client.post("/grafana/annotations", headers=_auth(), json={})
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json(), [])

    @patch.object(pfc_gateway, "API_KEY", TEST_KEY)
    @patch("pfc_gateway.subprocess.Popen")
    @patch("pfc_gateway._locate_binary", return_value="/usr/bin/pfc_jsonl")
    @patch("pfc_gateway.Path")
    def test_grafana_query_filter_via_pipe_syntax(self, mock_path, mock_binary, mock_popen):
        """Target format: 'path.pfc|{"level":"ERROR"}' must apply filter."""
        mock_path.return_value.exists.return_value = True
        mock_popen.return_value = _make_popen([
            '{"timestamp":"2026-03-01T11:00:00Z","level":"ERROR","msg":"bad"}',
            '{"timestamp":"2026-03-01T11:01:00Z","level":"INFO","msg":"ok"}',
        ])

        r = self.client.post("/grafana/query", headers=_auth(), json={
            "range":   {"from": "2026-03-01T10:00:00Z", "to": "2026-03-01T12:00:00Z"},
            "targets": [{"target": '/data/logs.pfc|{"level":"ERROR"}', "refId": "A"}],
            "maxDataPoints": 100,
        })
        results = r.json()
        # Only ERROR row should be in table rows
        self.assertEqual(len(results[0]["rows"]), 1)


# ===========================================================================
# 13. Streaming response
# ===========================================================================

class TestStreamingResponse(unittest.TestCase):

    def setUp(self):
        self.client = _make_client()

    @patch.object(pfc_gateway, "API_KEY", TEST_KEY)
    @patch("pfc_gateway.subprocess.Popen")
    @patch("pfc_gateway._locate_binary", return_value="/usr/bin/pfc_jsonl")
    @patch("pfc_gateway.Path")
    def test_content_type_is_ndjson(self, mock_path, mock_binary, mock_popen):
        mock_path.return_value.exists.return_value = True
        mock_popen.return_value = _make_popen(['{"level":"INFO","timestamp":"2026-03-01T11:00:00Z"}'])

        r = self.client.post("/query", headers=_auth(), json={
            "file":    "/data/logs.pfc",
            "from_ts": "2026-03-01T10:00:00Z",
            "to_ts":   "2026-03-01T12:00:00Z",
        })
        self.assertIn("ndjson", r.headers.get("content-type", ""))

    @patch.object(pfc_gateway, "API_KEY", TEST_KEY)
    @patch("pfc_gateway.subprocess.Popen")
    @patch("pfc_gateway._locate_binary", return_value="/usr/bin/pfc_jsonl")
    @patch("pfc_gateway.Path")
    def test_each_output_line_is_valid_json(self, mock_path, mock_binary, mock_popen):
        mock_path.return_value.exists.return_value = True
        rows = [
            '{"timestamp":"2026-03-01T11:00:00Z","level":"INFO","msg":"a"}',
            '{"timestamp":"2026-03-01T11:01:00Z","level":"ERROR","msg":"b"}',
            '{"timestamp":"2026-03-01T11:02:00Z","level":"WARN","msg":"c"}',
        ]
        mock_popen.return_value = _make_popen(rows)

        r = self.client.post("/query", headers=_auth(), json={
            "file":    "/data/logs.pfc",
            "from_ts": "2026-03-01T10:00:00Z",
            "to_ts":   "2026-03-01T12:00:00Z",
        })
        for line in r.text.strip().splitlines():
            if line:
                json.loads(line)  # must not raise


if __name__ == "__main__":
    unittest.main(verbosity=2)
