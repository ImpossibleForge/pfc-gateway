"""
Resilience / edge-case tests for pfc-gateway
=============================================
Focuses on error conditions, malformed inputs, process failures, and boundary
behavior.  Complements test_gateway.py which covers the happy paths.

Run with:  python -m pytest tests/ -v
"""

import io
import json
import sys
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
    _parse_s3_path,
    _row_in_range,
    _apply_filter,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

TEST_KEY = "test-secret-key"


def _make_client():
    return TestClient(app, raise_server_exceptions=False)


def _auth(key=TEST_KEY):
    return {"X-API-Key": key}


def _make_popen(lines, returncode=0):
    content = "".join(line if line.endswith("\n") else line + "\n" for line in lines)
    mock_proc = MagicMock()
    mock_proc.stdout = io.StringIO(content)
    mock_proc.stderr = MagicMock()
    mock_proc.stderr.read.return_value = ""
    mock_proc.returncode = returncode
    mock_proc.wait.return_value = returncode
    return mock_proc


# ===========================================================================
# R1. Binary not found
# ===========================================================================

class TestBinaryMissing(unittest.TestCase):
    """pfc_jsonl binary missing → must not crash the test suite."""

    def setUp(self):
        self.client = _make_client()

    @patch.object(pfc_gateway, "API_KEY", TEST_KEY)
    @patch("pfc_gateway._locate_binary", side_effect=RuntimeError("pfc_jsonl not found"))
    @patch("pfc_gateway.Path")
    def test_binary_missing_local_query_returns_error(self, mock_path, mock_binary):
        """RuntimeError from missing binary must not propagate to the test runner."""
        mock_path.return_value.exists.return_value = True
        # raise_server_exceptions=False → returns 500, not a Python exception
        r = self.client.post("/query", headers=_auth(), json={
            "file":    "/data/logs.pfc",
            "from_ts": "2026-03-01T10:00:00Z",
            "to_ts":   "2026-03-01T12:00:00Z",
        })
        # RuntimeError from missing binary is caught by the endpoint's
        # except RuntimeError block → HTTPException(500).
        self.assertEqual(r.status_code, 500)

    @patch.object(pfc_gateway, "API_KEY", TEST_KEY)
    @patch("pfc_gateway._locate_binary", side_effect=RuntimeError("pfc_jsonl not found"))
    @patch("pfc_gateway.Path")
    def test_binary_missing_batch_caught_per_file_returns_200_empty(self, mock_path, mock_binary):
        """Batch catches exceptions per-file → binary error → 200 empty body."""
        mock_path.return_value.exists.return_value = True
        r = self.client.post("/query/batch", headers=_auth(), json={
            "files": ["/data/logs.pfc"],
        })
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.text.strip(), "")


# ===========================================================================
# R2. Process crash / non-zero exit
# ===========================================================================

class TestProcessCrash(unittest.TestCase):
    """pfc_jsonl exits non-zero — rows yielded before exit are preserved."""

    def setUp(self):
        self.client = _make_client()

    @patch.object(pfc_gateway, "API_KEY", TEST_KEY)
    @patch("pfc_gateway.subprocess.Popen")
    @patch("pfc_gateway._locate_binary", return_value="/usr/bin/pfc_jsonl")
    @patch("pfc_gateway.Path")
    def test_nonzero_exit_rows_yielded_are_preserved(self, mock_path, mock_binary, mock_popen):
        """Rows output before non-zero exit must still be returned."""
        mock_path.return_value.exists.return_value = True
        mock_popen.return_value = _make_popen(
            ['{"timestamp":"2026-03-01T11:00:00Z","level":"INFO","msg":"ok"}'],
            returncode=1,
        )
        r = self.client.post("/query", headers=_auth(), json={
            "file":    "/data/logs.pfc",
            "from_ts": "2026-03-01T10:00:00Z",
            "to_ts":   "2026-03-01T12:00:00Z",
        })
        self.assertEqual(r.status_code, 200)
        lines = [l for l in r.text.strip().splitlines() if l]
        self.assertEqual(len(lines), 1)

    @patch.object(pfc_gateway, "API_KEY", TEST_KEY)
    @patch("pfc_gateway.subprocess.Popen")
    @patch("pfc_gateway._locate_binary", return_value="/usr/bin/pfc_jsonl")
    @patch("pfc_gateway.Path")
    def test_nonzero_exit_no_rows_returns_200_empty(self, mock_path, mock_binary, mock_popen):
        """Non-zero exit with no rows → 200 empty body."""
        mock_path.return_value.exists.return_value = True
        mock_popen.return_value = _make_popen([], returncode=1)
        r = self.client.post("/query", headers=_auth(), json={
            "file":    "/data/logs.pfc",
            "from_ts": "2026-03-01T10:00:00Z",
            "to_ts":   "2026-03-01T12:00:00Z",
        })
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.text.strip(), "")

    @patch.object(pfc_gateway, "API_KEY", TEST_KEY)
    @patch("pfc_gateway.subprocess.Popen")
    @patch("pfc_gateway._locate_binary", return_value="/usr/bin/pfc_jsonl")
    @patch("pfc_gateway.Path")
    def test_nonzero_exit_output_is_valid_ndjson(self, mock_path, mock_binary, mock_popen):
        """Each line returned after a failed process must be valid JSON."""
        mock_path.return_value.exists.return_value = True
        rows = [
            '{"timestamp":"2026-03-01T11:00:00Z","level":"INFO"}',
            '{"timestamp":"2026-03-01T11:01:00Z","level":"WARN"}',
        ]
        mock_popen.return_value = _make_popen(rows, returncode=1)
        r = self.client.post("/query", headers=_auth(), json={
            "file":    "/data/logs.pfc",
            "from_ts": "2026-03-01T10:00:00Z",
            "to_ts":   "2026-03-01T12:00:00Z",
        })
        for line in r.text.strip().splitlines():
            if line:
                json.loads(line)  # must not raise


# ===========================================================================
# R3. Malformed output from pfc_jsonl
# ===========================================================================

class TestMalformedOutput(unittest.TestCase):
    """Non-JSON or blank lines in pfc_jsonl output must be silently skipped."""

    def setUp(self):
        self.client = _make_client()

    @patch.object(pfc_gateway, "API_KEY", TEST_KEY)
    @patch("pfc_gateway.subprocess.Popen")
    @patch("pfc_gateway._locate_binary", return_value="/usr/bin/pfc_jsonl")
    @patch("pfc_gateway.Path")
    def test_non_json_lines_skipped(self, mock_path, mock_binary, mock_popen):
        mock_path.return_value.exists.return_value = True
        lines = [
            "ERROR: something went wrong",
            '{"timestamp":"2026-03-01T11:00:00Z","level":"INFO","msg":"good"}',
            "WARNING: block checksum mismatch",
        ]
        mock_popen.return_value = _make_popen(lines)
        r = self.client.post("/query", headers=_auth(), json={
            "file":    "/data/logs.pfc",
            "from_ts": "2026-03-01T10:00:00Z",
            "to_ts":   "2026-03-01T12:00:00Z",
        })
        self.assertEqual(r.status_code, 200)
        output_lines = [l for l in r.text.strip().splitlines() if l]
        self.assertEqual(len(output_lines), 1)
        self.assertEqual(json.loads(output_lines[0])["level"], "INFO")

    @patch.object(pfc_gateway, "API_KEY", TEST_KEY)
    @patch("pfc_gateway.subprocess.Popen")
    @patch("pfc_gateway._locate_binary", return_value="/usr/bin/pfc_jsonl")
    @patch("pfc_gateway.Path")
    def test_blank_lines_do_not_appear_in_output(self, mock_path, mock_binary, mock_popen):
        mock_path.return_value.exists.return_value = True
        lines = [
            "",
            '{"timestamp":"2026-03-01T11:00:00Z","level":"INFO"}',
            "",
            "",
        ]
        mock_popen.return_value = _make_popen(lines)
        r = self.client.post("/query", headers=_auth(), json={
            "file":    "/data/logs.pfc",
            "from_ts": "2026-03-01T10:00:00Z",
            "to_ts":   "2026-03-01T12:00:00Z",
        })
        output_lines = [l for l in r.text.strip().splitlines() if l]
        self.assertEqual(len(output_lines), 1)

    @patch.object(pfc_gateway, "API_KEY", TEST_KEY)
    @patch("pfc_gateway.subprocess.Popen")
    @patch("pfc_gateway._locate_binary", return_value="/usr/bin/pfc_jsonl")
    @patch("pfc_gateway.Path")
    def test_truncated_json_skipped(self, mock_path, mock_binary, mock_popen):
        mock_path.return_value.exists.return_value = True
        lines = [
            '{"timestamp":"2026-03-01T11:00:00Z","level":"INFO","msg":"ok"}',
            '{"truncated":',  # incomplete JSON
        ]
        mock_popen.return_value = _make_popen(lines)
        r = self.client.post("/query", headers=_auth(), json={
            "file":    "/data/logs.pfc",
            "from_ts": "2026-03-01T10:00:00Z",
            "to_ts":   "2026-03-01T12:00:00Z",
        })
        output_lines = [l for l in r.text.strip().splitlines() if l]
        self.assertEqual(len(output_lines), 1)


# ===========================================================================
# R4. Input validation edge cases
# ===========================================================================

class TestInputValidation(unittest.TestCase):
    """Invalid timestamps and missing required fields must be rejected cleanly."""

    def setUp(self):
        self.client = _make_client()

    @patch.object(pfc_gateway, "API_KEY", TEST_KEY)
    @patch("pfc_gateway.Path")
    def test_query_invalid_from_ts_returns_400(self, mock_path):
        mock_path.return_value.exists.return_value = True
        r = self.client.post("/query", headers=_auth(), json={
            "file":    "/data/logs.pfc",
            "from_ts": "this-is-not-a-date",
        })
        self.assertEqual(r.status_code, 400)

    @patch.object(pfc_gateway, "API_KEY", TEST_KEY)
    @patch("pfc_gateway.Path")
    def test_query_invalid_to_ts_returns_400(self, mock_path):
        mock_path.return_value.exists.return_value = True
        r = self.client.post("/query", headers=_auth(), json={
            "file":  "/data/logs.pfc",
            "to_ts": "definitely-not-a-date",
        })
        self.assertEqual(r.status_code, 400)

    @patch.object(pfc_gateway, "API_KEY", TEST_KEY)
    def test_query_missing_file_field_returns_422(self):
        """Pydantic: 'file' is required → 422 Unprocessable Entity."""
        r = self.client.post("/query", headers=_auth(), json={
            "from_ts": "2026-03-01T10:00:00Z",
        })
        self.assertEqual(r.status_code, 422)

    @patch.object(pfc_gateway, "API_KEY", TEST_KEY)
    def test_batch_invalid_to_ts_returns_400(self):
        r = self.client.post("/query/batch", headers=_auth(), json={
            "files":  ["/data/logs.pfc"],
            "to_ts":  "not-a-date-at-all",
        })
        self.assertEqual(r.status_code, 400)


# ===========================================================================
# R5. S3 edge cases
# ===========================================================================

class TestS3EdgeCases(unittest.TestCase):

    def setUp(self):
        self.client = _make_client()

    @patch.object(pfc_gateway, "API_KEY", TEST_KEY)
    @patch("pfc_gateway.subprocess.Popen")
    @patch("pfc_gateway._locate_binary", return_value="/usr/bin/pfc_jsonl")
    @patch("pfc_gateway.boto3.Session")
    def test_s3_query_without_time_range_uses_all_flag(self, mock_session, mock_binary, mock_popen):
        """No from_ts/to_ts for S3 → pfc_jsonl s3-fetch --all."""
        mock_s3 = MagicMock()
        mock_s3.generate_presigned_url.return_value = "https://presigned"
        mock_session.return_value.client.return_value = mock_s3
        mock_popen.return_value = _make_popen(
            ['{"level":"INFO","timestamp":"2026-03-01T11:00:00Z"}']
        )
        self.client.post("/query", headers=_auth(), json={
            "file": "s3://my-bucket/logs.pfc",
        })
        cmd = mock_popen.call_args[0][0]
        self.assertIn("--all", cmd)
        self.assertNotIn("--from", cmd)

    @patch.object(pfc_gateway, "API_KEY", TEST_KEY)
    @patch("pfc_gateway.subprocess.Popen")
    @patch("pfc_gateway._locate_binary", return_value="/usr/bin/pfc_jsonl")
    @patch("pfc_gateway.boto3.Session")
    def test_s3_query_with_time_range_presigns_idx_url(self, mock_session, mock_binary, mock_popen):
        """With time range, the .pfc.idx file must also be pre-signed."""
        mock_s3 = MagicMock()
        mock_s3.generate_presigned_url.return_value = "https://presigned"
        mock_session.return_value.client.return_value = mock_s3
        mock_popen.return_value = _make_popen(
            ['{"timestamp":"2026-03-01T11:00:00Z","level":"INFO"}']
        )
        self.client.post("/query", headers=_auth(), json={
            "file":    "s3://my-bucket/logs.pfc",
            "from_ts": "2026-03-01T10:00:00Z",
            "to_ts":   "2026-03-01T12:00:00Z",
        })
        # Both .pfc and .pfc.idx must be presigned (≥ 2 calls)
        self.assertGreaterEqual(mock_s3.generate_presigned_url.call_count, 2)
        presigned_keys = [
            ca[1]["Params"]["Key"]
            for ca in mock_s3.generate_presigned_url.call_args_list
        ]
        self.assertTrue(any(k.endswith(".idx") for k in presigned_keys))

    @patch.object(pfc_gateway, "API_KEY", TEST_KEY)
    @patch("pfc_gateway.subprocess.Popen")
    @patch("pfc_gateway._locate_binary", return_value="/usr/bin/pfc_jsonl")
    @patch("pfc_gateway.boto3.Session")
    def test_s3_query_forwards_aws_profile_to_boto3_session(
        self, mock_session, mock_binary, mock_popen
    ):
        """aws_profile in request body must be forwarded to boto3.Session()."""
        mock_s3 = MagicMock()
        mock_s3.generate_presigned_url.return_value = "https://presigned"
        mock_session.return_value.client.return_value = mock_s3
        mock_popen.return_value = _make_popen(
            ['{"timestamp":"2026-03-01T11:00:00Z","level":"INFO"}']
        )
        self.client.post("/query", headers=_auth(), json={
            "file":        "s3://my-bucket/logs.pfc",
            "from_ts":     "2026-03-01T10:00:00Z",
            "to_ts":       "2026-03-01T12:00:00Z",
            "aws_profile": "my-named-profile",
        })
        mock_session.assert_called_with(
            profile_name="my-named-profile",
            region_name=pfc_gateway.AWS_REGION,
        )


# ===========================================================================
# R6. Batch edge cases
# ===========================================================================

class TestBatchEdgeCases(unittest.TestCase):

    def setUp(self):
        self.client = _make_client()

    @patch.object(pfc_gateway, "API_KEY", TEST_KEY)
    def test_batch_empty_files_list_returns_200_empty(self):
        r = self.client.post("/query/batch", headers=_auth(), json={"files": []})
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.text.strip(), "")

    @patch.object(pfc_gateway, "API_KEY", TEST_KEY)
    @patch("pfc_gateway.subprocess.Popen")
    @patch("pfc_gateway._locate_binary", return_value="/usr/bin/pfc_jsonl")
    @patch("pfc_gateway.boto3.Session")
    @patch("pfc_gateway.Path")
    def test_batch_s3_file_calls_presign(
        self, mock_path, mock_session, mock_binary, mock_popen
    ):
        """S3 file in batch → boto3 presign must be called."""
        mock_path.return_value.exists.return_value = True
        mock_s3 = MagicMock()
        mock_s3.generate_presigned_url.return_value = "https://presigned"
        mock_session.return_value.client.return_value = mock_s3
        mock_popen.return_value = _make_popen(
            ['{"level":"INFO","timestamp":"2026-03-01T11:00Z"}']
        )
        self.client.post("/query/batch", headers=_auth(), json={
            "files":   ["s3://my-bucket/logs.pfc"],
            "from_ts": "2026-03-01T00:00:00Z",
            "to_ts":   "2026-03-02T00:00:00Z",
        })
        mock_s3.generate_presigned_url.assert_called()

    @patch.object(pfc_gateway, "API_KEY", TEST_KEY)
    @patch("pfc_gateway.subprocess.Popen")
    @patch("pfc_gateway._locate_binary", return_value="/usr/bin/pfc_jsonl")
    @patch("pfc_gateway.Path")
    def test_batch_exception_on_one_file_others_continue(
        self, mock_path, mock_binary, mock_popen
    ):
        """RuntimeError on one batch file → rest of files still processed."""
        mock_path.return_value.exists.return_value = True

        def popen_side_effect(cmd, **kwargs):
            if "bad" in " ".join(cmd):
                raise RuntimeError("simulated read error")
            return _make_popen(
                ['{"level":"INFO","timestamp":"2026-03-02T11:00Z"}']
            )

        mock_popen.side_effect = popen_side_effect
        r = self.client.post("/query/batch", headers=_auth(), json={
            "files": ["/data/bad.pfc", "/data/good.pfc"],
        })
        self.assertEqual(r.status_code, 200)
        lines = [l for l in r.text.strip().splitlines() if l]
        self.assertEqual(len(lines), 1)


# ===========================================================================
# R7. Grafana edge cases
# ===========================================================================

class TestGrafanaEdgeCases(unittest.TestCase):

    def setUp(self):
        self.client = _make_client()

    @patch.object(pfc_gateway, "API_KEY", TEST_KEY)
    @patch("pfc_gateway._locate_binary", return_value="/usr/bin/pfc_jsonl")
    @patch("pfc_gateway.Path")
    def test_grafana_non_pfc_target_not_in_results(self, mock_path, mock_binary):
        """Grafana target without .pfc extension must be skipped."""
        mock_path.return_value.exists.return_value = True
        r = self.client.post("/grafana/query", headers=_auth(), json={
            "range":        {"from": "2026-03-01T10:00:00Z", "to": "2026-03-01T12:00:00Z"},
            "targets":      [{"target": "/data/logs.csv", "refId": "A"}],
            "maxDataPoints": 100,
        })
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json(), [])

    @patch.object(pfc_gateway, "API_KEY", TEST_KEY)
    @patch("pfc_gateway.subprocess.Popen")
    @patch("pfc_gateway._locate_binary", return_value="/usr/bin/pfc_jsonl")
    @patch("pfc_gateway.Path")
    def test_grafana_respects_max_data_points(self, mock_path, mock_binary, mock_popen):
        """maxDataPoints cap must limit rows in Grafana table response."""
        mock_path.return_value.exists.return_value = True
        rows_10 = [
            f'{{"timestamp":"2026-03-01T11:0{i}:00Z","level":"INFO","idx":{i}}}'
            for i in range(10)
        ]
        mock_popen.return_value = _make_popen(rows_10)
        r = self.client.post("/grafana/query", headers=_auth(), json={
            "range":        {"from": "2026-03-01T10:00:00Z", "to": "2026-03-01T12:00:00Z"},
            "targets":      [{"target": "/data/logs.pfc", "refId": "A"}],
            "maxDataPoints": 3,
        })
        results = r.json()
        self.assertEqual(len(results), 1)
        self.assertEqual(len(results[0]["rows"]), 3)

    @patch.object(pfc_gateway, "API_KEY", TEST_KEY)
    @patch("pfc_gateway.subprocess.Popen")
    @patch("pfc_gateway._locate_binary", return_value="/usr/bin/pfc_jsonl")
    @patch("pfc_gateway.Path")
    def test_grafana_malformed_pipe_filter_uses_no_filter(
        self, mock_path, mock_binary, mock_popen
    ):
        """Malformed JSON in pipe filter → query runs without filter, row returned."""
        mock_path.return_value.exists.return_value = True
        mock_popen.return_value = _make_popen([
            '{"timestamp":"2026-03-01T11:00:00Z","level":"INFO","msg":"hello"}',
        ])
        r = self.client.post("/grafana/query", headers=_auth(), json={
            "range":        {"from": "2026-03-01T10:00:00Z", "to": "2026-03-01T12:00:00Z"},
            "targets":      [{"target": "/data/logs.pfc|{not valid json}", "refId": "A"}],
            "maxDataPoints": 100,
        })
        results = r.json()
        # Query proceeds without filter — the one row is still returned
        self.assertEqual(len(results), 1)
        self.assertEqual(len(results[0]["rows"]), 1)

    @patch.object(pfc_gateway, "API_KEY", TEST_KEY)
    @patch("pfc_gateway._locate_binary", return_value="/usr/bin/pfc_jsonl")
    @patch("pfc_gateway.Path")
    def test_grafana_empty_targets_returns_empty_list(self, mock_path, mock_binary):
        r = self.client.post("/grafana/query", headers=_auth(), json={
            "range":        {"from": "2026-03-01T10:00:00Z", "to": "2026-03-01T12:00:00Z"},
            "targets":      [],
            "maxDataPoints": 100,
        })
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json(), [])


# ===========================================================================
# R8. Row filtering edge cases (unit tests)
# ===========================================================================

class TestRowFilteringEdgeCases(unittest.TestCase):
    """_row_in_range and _apply_filter boundary behavior."""

    def setUp(self):
        self.from_dt = datetime(2026, 3, 1, 10, 0, tzinfo=timezone.utc)
        self.to_dt   = datetime(2026, 3, 1, 12, 0, tzinfo=timezone.utc)

    def test_apply_filter_key_absent_from_row_excluded(self):
        """Filter key not present in row → row.get() returns None ≠ filter value → excluded."""
        row = {"msg": "hello"}  # no "level" key
        self.assertFalse(_apply_filter(row, {"level": "ERROR"}))

    def test_apply_filter_none_value_matches_null_field(self):
        """Filter value None must match a row field that is None."""
        row = {"level": None, "msg": "hello"}
        self.assertTrue(_apply_filter(row, {"level": None}))

    def test_apply_filter_extra_row_fields_do_not_affect_match(self):
        """Extra row fields (not in filter) must not prevent a match."""
        row = {"level": "ERROR", "host": "web-01", "extra": "ignored"}
        self.assertTrue(_apply_filter(row, {"level": "ERROR"}))

    def test_row_in_range_ts_at_from_boundary_included(self):
        """Timestamp exactly equal to from_dt must be included (inclusive start)."""
        row = {"timestamp": self.from_dt.isoformat()}
        self.assertTrue(_row_in_range(row, self.from_dt, self.to_dt))

    def test_row_in_range_only_to_dt_filters_correctly(self):
        """Only to_dt set → rows strictly before to_dt included, at/after excluded."""
        row_before = {"timestamp": "2026-03-01T09:00:00Z"}
        row_at     = {"timestamp": "2026-03-01T12:00:00Z"}
        self.assertTrue(_row_in_range(row_before, None, self.to_dt))
        self.assertFalse(_row_in_range(row_at,     None, self.to_dt))

    def test_row_in_range_only_from_dt_filters_correctly(self):
        """Only from_dt set → rows at/after from_dt included, before excluded."""
        row_before  = {"timestamp": "2026-03-01T09:59:00Z"}
        row_at      = {"timestamp": "2026-03-01T10:00:00Z"}
        row_after   = {"timestamp": "2026-03-01T23:00:00Z"}
        self.assertFalse(_row_in_range(row_before, self.from_dt, None))
        self.assertTrue(_row_in_range(row_at,      self.from_dt, None))
        self.assertTrue(_row_in_range(row_after,   self.from_dt, None))


if __name__ == "__main__":
    unittest.main(verbosity=2)
