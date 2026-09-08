"""Live phase-one suite using the committed, generated Python client."""

import asyncio
import datetime
import json
import os
from pathlib import Path
import uuid
from urllib.parse import quote
from hevlayer import AsyncHevlayer, HevlayerError


async def main():
    prefix = os.environ.get("PGVECTOR_TEST_PREFIX", "scratch-lyr20")
    ns = f"{prefix}-{datetime.datetime.now(datetime.timezone.utc):%Y%m%d}-python-{uuid.uuid4().hex[:8]}-'雪_%41"
    # The generated Python SDK interpolates path parameters directly (the TS
    # SDK encodes them). Encode here so both suites send the same logical name.
    path_ns = quote(ns, safe="")
    cases = json.loads(Path(__file__).with_name("cases.json").read_text())
    async with AsyncHevlayer(
        base_url=os.environ.get("PGVECTOR_GATEWAY_URL", "http://127.0.0.1:8080")
    ) as client:
        try:
            for case in cases:
                op = case["op"]
                body = case.get("body", {})
                methods = {
                    "write": lambda: client.write_namespace(path_ns, body),
                    "query": lambda: client.query_namespace(path_ns, body),
                    "schema": lambda: client.get_turbopuffer_namespace_schema(path_ns),
                    "metadata": lambda: client.get_namespace_metadata(path_ns),
                    "fetch": lambda: client.fetch_document(path_ns, body["id"]),
                    "fetch_many": lambda: client.fetch_documents(path_ns, body),
                    "warm": lambda: client.hint_cache_warm(path_ns),
                }
                try:
                    result = await methods[op]()
                except HevlayerError as e:
                    assert e.status_code == case.get("status", 200), (
                        case["name"],
                        e.status_code,
                        str(e),
                    )
                    if "feature" in case:
                        assert e.error == "UnsupportedByStore", (case["name"], e.error)
                        assert case["feature"] in str(e), (case["name"], str(e))
                else:
                    assert case.get("status", 200) == 200, (
                        case["name"],
                        "unexpected success",
                    )
                    if hasattr(result, "model_dump"):
                        result = result.model_dump(by_alias=True, exclude_none=True)
                    if op == "write":
                        assert result["rows_affected"] == case["count"], case["name"]
                    if op == "metadata":
                        assert result["approx_row_count"] == case["count"], case["name"]
                        assert "index" not in result, case["name"]
                    if op == "schema":
                        if "field" in case:
                            assert case["field"] in result, case["name"]
                        if "absent" in case:
                            assert case["absent"] not in result, case["name"]
                    if op == "query":
                        rows = result["rows"]
                        if "ids" in case:
                            assert {json.dumps(r["id"]) for r in rows} == {
                                json.dumps(i) for i in case["ids"]
                            }, (case["name"], rows)
                        if "first" in case:
                            assert rows[0]["id"] == case["first"], (case["name"], rows)
                        if "dist" in case:
                            assert abs(rows[0]["$dist"] - case["dist"]) < 1e-6, case[
                                "name"
                            ]
                        if "fields" in case:
                            assert set(rows[0]) == set(case["fields"]), (
                                case["name"],
                                rows,
                            )
                        if "n" in case:
                            assert rows[0]["n"] == case["n"], case["name"]
                        if case.get("hybrid"):
                            assert result.get("hybrid"), case["name"]
                    if op == "fetch":
                        assert result["attributes"]["text"] == case["text"], case[
                            "name"
                        ]
                        if "absent" in case:
                            assert case["absent"] not in result["attributes"], case[
                                "name"
                            ]
                    if op == "fetch_many":
                        assert {r["id"] for r in result["documents"]} == set(
                            case["ids"]
                        ), (case["name"], result)
                print("PASS", case["name"])
            listing = await client.list_turbopuffer_namespaces(prefix=ns)
            assert ns in [n.id for n in listing.namespaces]
        finally:
            await client.delete_namespace(path_ns)
            listing = await client.list_turbopuffer_namespaces(prefix=ns)
            assert not listing.namespaces, "namespace cleanup failed"
    print(f"Python: {len(cases)} phase-one cases plus namespace listing/cleanup passed")


asyncio.run(main())
