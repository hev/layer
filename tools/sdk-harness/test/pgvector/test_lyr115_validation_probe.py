"""Deterministic HTTP sensitivity tests; no gateway or database is exercised."""

from contextlib import contextmanager, redirect_stdout
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import io
import json
import os
import threading
import unittest
from unittest.mock import patch

import lyr115_validation_probe as probe


@contextmanager
def server(mode="healthy"):
    state = {"exists": mode == "existing", "row": None, "requests": []}

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def respond(self, status, body):
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(body).encode())

        def do_GET(self):
            state["requests"].append(("GET", self.path, None))
            self.respond(200 if state["exists"] else 404, {})

        def do_DELETE(self):
            state["requests"].append(("DELETE", self.path, None))
            if mode == "cleanup_error":
                self.respond(500, {"error": "delete_failed"})
            else:
                if mode != "cleanup_lies":
                    state["exists"] = False
                self.respond(200, {})

        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            state["requests"].append(("POST", self.path, body))
            invalid = False
            if self.path.endswith("/query"):
                invalid = (body.get("top_k", 0) > 10000
                           or len(body["rank_by"][2]) != 2
                           or ("filters" in body and mode != "unknown_accepted"))
                if not invalid:
                    self.respond(200, {"rows": [state["row"]]})
                    return
            else:
                if "schema" in body:
                    state["exists"] = True
                    if mode == "seed_error":
                        self.respond(500, {})
                        return
                shape = "upsert_rows" if "upsert_rows" in body else "upsert_columns"
                if shape == "upsert_columns" and mode == "unsupported_columns":
                    self.respond(422, {"error": "UnsupportedByStore",
                                       "message": "pgvector upsert_columns"})
                    return
                row = (body[shape][0] if shape == "upsert_rows" else
                       {k: v[0] for k, v in body[shape].items()})
                reserved = any(k.startswith("_hevlayer_") for k in row)
                invalid = len(row["vector"]) != 2 or reserved
                if reserved and mode in ("reserved_stored", "reject_but_store"):
                    state["row"] = row
                    if mode == "reserved_stored":
                        self.respond(200, {})
                        return
                if not invalid:
                    state["row"] = row
                    self.respond(200, {})
                    return
            if invalid:
                status = 400 if mode == "historical_400" else 422
                error = "UnsupportedByStore" if mode == "wrong_error" else "validation_error"
                body = {"error": error, "message": "invalid input"}
                if mode == "text_422":
                    body = "validation_error"
                if mode == "missing_message":
                    body.pop("message")
                self.respond(status, body)

    httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    try:
        yield "http://127.0.0.1:{}".format(httpd.server_port), state
    finally:
        httpd.shutdown()
        httpd.server_close()
        thread.join()


class ProbeTests(unittest.TestCase):
    def execute(self, mode):
        with server(mode) as (url, state):
            output = io.StringIO()
            with patch.dict(os.environ, {"PGVECTOR_API_KEY": "offline-secret"}), redirect_stdout(output):
                code = probe.main(["--gateway-url", url])
            self.assertNotIn("offline-secret", output.getvalue())
            report = json.loads(output.getvalue())
            self.assertEqual(code, 0 if report["passed"] else 1)
            self.assertRegex(report["namespace"], r"^scratch-lyr115-\d{8}-[a-f0-9]{32}$")
            deletes = [r for r in state["requests"] if r[0] == "DELETE"]
            self.assertEqual(len(deletes), 0 if mode == "existing" else 1)
            if deletes:
                self.assertEqual(deletes[0][1], "/v2/namespaces/" + report["namespace"])
                self.assertEqual(state["requests"][-1][0], "GET")
            return report, state

    def test_healthy_script_validates_probe_only(self):
        report, state = self.execute("healthy")
        self.assertTrue(report["passed"], report)
        self.assertFalse(state["exists"])
        self.assertEqual(len(report["cases"]), 16)
        names = {c["case"] for c in report["cases"]}
        for shape in ("upsert_rows", "upsert_columns"):
            self.assertIn(shape + "_reserved_not_stored", names)

    def test_regression_sensitivity(self):
        for mode, failed_case in (
                ("historical_400", "top_k_above_10000"),
                ("wrong_error", "query_wrong_vector_dimension"),
                ("text_422", "unknown_schema_attribute"),
                ("missing_message", "unknown_schema_attribute"),
                ("unknown_accepted", "unknown_schema_attribute"),
                ("reserved_stored", "upsert_rows_reserved"),
                ("reject_but_store", "upsert_columns_reserved_not_stored"),
                ("cleanup_error", "cleanup_delete"),
                ("cleanup_lies", "cleanup_verify_absent"),
                ("unsupported_columns", "upsert_columns_control"),
                ("seed_error", "seed")):
            with self.subTest(mode=mode):
                report, _ = self.execute(mode)
                self.assertFalse(report["passed"])
                self.assertIn(failed_case, [c["case"] for c in report["cases"] if not c["passed"]])
                if mode == "unsupported_columns":
                    self.assertNotIn("upsert_columns_reserved", [c["case"] for c in report["cases"]])

    def test_existing_namespace_never_written_or_deleted(self):
        report, state = self.execute("existing")
        self.assertFalse(report["passed"])
        self.assertEqual([r[0] for r in state["requests"]], ["GET"])

    def test_explicit_url_required(self):
        with redirect_stdout(io.StringIO()), patch("sys.stderr", new=io.StringIO()):
            with self.assertRaises(SystemExit) as raised:
                probe.main([])
        self.assertEqual(raised.exception.code, 2)

    def test_seed_transport_failure_still_cleans_up(self):
        with server() as (url, state):
            client = probe.Client(url)
            original = client.request

            def fail_after_seed(method, path, body=None):
                response = original(method, path, body)
                if body and "schema" in body:
                    raise TimeoutError("response lost")
                return response

            client.request = fail_after_seed
            report = probe.run(client)
            self.assertFalse(report["passed"])
            self.assertFalse(state["exists"])
            self.assertEqual(report["cases"][-1]["case"], "cleanup_verify_absent")


if __name__ == "__main__":
    unittest.main()
