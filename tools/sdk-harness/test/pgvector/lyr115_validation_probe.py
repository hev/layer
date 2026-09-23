#!/usr/bin/env python3
"""Isolated LYR-115 regression probe (Python standard library only).

Run explicitly: python3 tools/sdk-harness/test/pgvector/lyr115_validation_probe.py
--gateway-url http://127.0.0.1:8080 (on one line). Optional bearer auth comes
from PGVECTOR_API_KEY, never a command argument. JSON stdout reports each
expected/observed status and body; exit 1 means a failed check or cleanup.

Only run against an authorized pgvector gateway. Creates a UUID scratch
namespace, checks absence first, and deletes that exact name in finally.
Cleanup verifies origin metadata absence; asynchronous cache/S3 purge is not
certified. No caller-supplied namespace or remote default is accepted.

Covers native upsert_rows and object-of-arrays upsert_columns. pgvector's
patch_rows/patch_columns are unsupported, and array-of-columns is not its
accepted representation; none count as reserved-key rejection coverage.
Unknown attributes are tested in query filters: writes infer new attributes.
Offline scripted success proves probe behavior, not Postgres acceptance.
Sources: pgvector/client.py, cases.json, gateway openapi.yaml, api/write.mdx.
"""

import argparse
import datetime
import json
import os
import urllib.error
import urllib.parse
import urllib.request
import uuid


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class Client:
    def __init__(self, url, token="", timeout=10):
        parsed = urllib.parse.urlsplit(url)
        if (parsed.scheme not in ("http", "https") or not parsed.hostname
                or parsed.username or parsed.password or parsed.query or parsed.fragment):
            raise ValueError("gateway URL must be HTTP(S), without credentials/query/fragment")
        self.url, self.token, self.timeout = url.rstrip("/"), token, timeout
        self.opener = urllib.request.build_opener(NoRedirect())

    def request(self, method, path, body=None):
        headers = {"Content-Type": "application/json"}
        if self.token:
            headers["Authorization"] = "Bearer " + self.token
        req = urllib.request.Request(self.url + path, method=method, headers=headers,
                                     data=None if body is None else json.dumps(body).encode())
        try:
            response = self.opener.open(req, timeout=self.timeout)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            raw = response.read().decode("utf-8", errors="replace")
            # Also redact an accidentally echoed credential in server responses.
            if self.token:
                raw = raw.replace(self.token, "[REDACTED]")
            try:
                payload = json.loads(raw)
            except ValueError:
                payload = raw
            return {"status": response.code, "body": payload}


def query(**extra):
    return {"rank_by": ["vector", "ANN", [1, 0]], "top_k": 10,
            "include_attributes": True, **extra}


def write(shape, row):
    return {shape: [row] if shape == "upsert_rows" else
            {key: [value] for key, value in row.items()}}


def run(client):
    namespace = "scratch-lyr115-{}-{}".format(
        datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d"), uuid.uuid4().hex)
    base = "/v2/namespaces/" + namespace
    metadata = "/v1/namespaces/" + namespace + "/metadata"
    results = []
    owned = False

    def check(name, method, path, body, status, predicate=lambda b: True,
              expectation=""):
        observed = client.request(method, path, body)
        passed = observed["status"] == status and predicate(observed["body"])
        results.append({"case": name, "expected": {"status": status,
                        "body": expectation}, "observed": observed, "passed": passed})
        return passed

    def validation(name, path, body):
        return check(name, "POST", path, body, 422,
                     lambda b: isinstance(b, dict) and b.get("error") == "validation_error"
                     and isinstance(b.get("message"), str) and bool(b["message"].strip()),
                     '{"error":"validation_error","message":<nonempty string>}')

    try:
        if not check("namespace_absent_before_create", "GET", metadata, None, 404):
            return {"namespace": namespace, "passed": False, "cases": results}
        # Mark before sending: a timeout can follow a committed seed write.
        owned = True
        seed = {"id": "seed", "vector": [1, 0], "tag": "baseline"}
        if not check("seed", "POST", base,
                     {**write("upsert_rows", seed), "schema": {
                         "vector": "[2]f32", "tag": "string"},
                      "distance_metric": "euclidean_squared"}, 200):
            return {"namespace": namespace, "passed": False, "cases": results}
        def baseline(b):
            return (isinstance(b, dict) and isinstance(b.get("rows"), list)
                    and len(b["rows"]) == 1 and isinstance(b["rows"][0], dict)
                    and b["rows"][0].get("id") == "seed"
                    and b["rows"][0].get("tag") == "baseline")
        if not check("seed_readback", "POST", base + "/query", query(), 200,
                     baseline, "one seed row with tag=baseline"):
            return {"namespace": namespace, "passed": False, "cases": results}
        validation("unknown_schema_attribute", base + "/query",
                   query(filters=["lyr115_unknown", "Eq", "value"]))
        validation("query_wrong_vector_dimension", base + "/query",
                   query(rank_by=["vector", "ANN", [1, 0, 0]]))
        validation("top_k_above_10000", base + "/query", query(top_k=10001))
        for shape in ("upsert_rows", "upsert_columns"):
            if not check(shape + "_control", "POST", base, write(shape, seed), 200,
                         expectation="valid representation succeeds; unsupported is a failure"):
                continue
            validation(shape + "_wrong_vector_dimension", base,
                       write(shape, {**seed, "vector": [1, 0, 0]}))
            key = "_hevlayer_lyr115_probe"
            validation(shape + "_reserved", base,
                       write(shape, {**seed, key: "caller-reserved-value"}))
            # Explicit projection avoids the normal suppression of reserved attrs.
            check(shape + "_reserved_not_stored", "POST", base + "/query",
                  query(include_attributes=["tag", key]), 200,
                  lambda b: baseline(b) and key not in b["rows"][0],
                  "seed remains; explicitly projected caller-reserved key absent")
    except (OSError, ValueError) as error:
        results.append({"case": "transport_or_decode", "passed": False,
                        "observed": {"error": type(error).__name__}})
    finally:
        if owned:
            # Independent attempts: failed DELETE must not prevent verification.
            for name, method, path, status in (
                    ("cleanup_delete", "DELETE", base, 200),
                    ("cleanup_verify_absent", "GET", metadata, 404)):
                try:
                    check(name, method, path, None, status)
                except (OSError, ValueError) as error:
                    results.append({"case": name, "passed": False,
                                    "observed": {"error": type(error).__name__}})
    return {"namespace": namespace, "passed": all(r["passed"] for r in results),
            "cases": results}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--gateway-url", required=True)
    args = parser.parse_args(argv)
    try:
        client = Client(args.gateway_url, os.environ.get("PGVECTOR_API_KEY", ""))
    except ValueError as error:
        parser.error(str(error))
    report = run(client)
    print(json.dumps(report, sort_keys=True))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
