"""LYR-112: runtime declarations and ordered/conditional wire behavior together."""
from concurrent.futures import ThreadPoolExecutor
import json
import os
import uuid
from urllib.error import HTTPError
from urllib.request import Request, urlopen

base = os.environ.get("PGVECTOR_GATEWAY_URL", "http://127.0.0.1:8080")
ns = f"scratch-lyr112-{uuid.uuid4().hex}"


def request(method, path, body=None, status=200):
    req = Request(base + path, method=method, headers={"Content-Type": "application/json"},
                  data=None if body is None else json.dumps(body).encode())
    try:
        response = urlopen(req, timeout=30)
    except HTTPError as error:
        response = error
    with response:
        raw = response.read()
        value = json.loads(raw) if raw else None
        assert response.code == status, (path, body, response.code, value)
        return value


wire = f"/v2/namespaces/{ns}"


def query(body, expected):
    rows = request("POST", wire + "/query", body)["rows"]
    assert [r["id"] for r in rows] == expected, (body, rows)
    assert all("$dist" not in r for r in rows), rows


created = False
try:
    report = request("GET", f"/v2/namespaces/{ns}/capabilities")
    assert report["declared"] is True and report["store"]["kind"] == "pgvector", report
    features = {f["id"]: f["support"] for f in report["features"]}
    assert features["ordered_scan"] == features["conditional_writes"] == "supported", report
    assert features["search_after"] == "unsupported", report
    request("POST", wire, {"upsert_rows": [
        {"id": "b", "version": 2}, {"id": "a", "version": 2},
        {"id": "c", "version": 3}, {"id": "d"}, {"id": 9, "version": 1}]})
    created = True
    query({"filters": ["version", "Gte", 2], "limit": 2}, ["a", "b"])
    query({"rank_by": ["version", "asc"], "top_k": 3}, ["d", 9, "a"])
    query({"rank_by": ["version", "desc"], "top_k": 3}, ["c", "a", "b"])
    query({"rank_by": ["id", "desc"], "filters": ["version", "Eq", 2], "limit": 1}, ["b"])
    query({"filters": ["version", "Gt", 99]}, [])
    for body in ({"top_k": 0}, {"limit": 10001}, {"top_k": 1, "limit": 1}):
        request("POST", wire + "/query", body, status=400)
    rejection = request("POST", wire + "/query", {"cursor": "x"}, status=422)
    assert rejection["feature"] == "search_after", rejection
    result = request("POST", wire, {"upsert_columns": {"id": ["a", "b", "new"], "version": [3, 1, 1]},
                     "upsert_condition": ["version", "Lt", {"$ref_new": "version"}]})
    assert result["rows_affected"] == 2, result
    query({"filters": ["version", "Eq", 3]}, ["a", "c"])
    query({"filters": ["version", "Eq", 2]}, ["b"])
    query({"filters": ["id", "Eq", "new"]}, ["new"])
    request("POST", wire, {"schema": {"added": "string"}, "upsert_rows": [{"id": "oops"}],
            "upsert_condition": ["version", "Lt", {"$ref_new": "added"}]}, status=400)
    query({"filters": ["id", "Eq", "oops"]}, [])
    schema = request("GET", f"/v1/namespaces/{ns}/schema")
    assert "added" not in schema, schema
    # Racing writers cannot let the older version overwrite the newer one.
    def advance(version):
        return request("POST", wire, {"upsert_rows": [{"id": "a", "version": version}],
                       "upsert_condition": ["version", "Lt", {"$ref_new": "version"}]})
    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(advance, [11, 10]))
    query({"filters": ["version", "Eq", 11]}, ["a"])
    print("PASS LYR-112: declared ordered_scan/conditional_writes, filters, asc/desc, id, limits, column conditions, atomic rejection")
finally:
    if created:
        request("DELETE", wire)
