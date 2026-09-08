# Maintainer helper for the checked-in, shared Python/TypeScript wire cases.
import json
from pathlib import Path

cases = []


def add(name, op, body=None, **expected):
    cases.append(
        dict(
            name=name, op=op, **({"body": body} if body is not None else {}), **expected
        )
    )


rows = [
    {
        "id": "a",
        "vector": [1, 0],
        "text": "database database postgres",
        "n": 1,
        "tag": "x'; DROP TABLE nope;--",
        "active": True,
    },
    {
        "id": "b",
        "vector": [0, 1],
        "text": "database vector",
        "n": 2,
        "tag": None,
        "active": False,
    },
    {"id": "c", "vector": [-1, 0], "text": "forest trees", "n": None},
    {"id": "d", "vector": [0, -1], "text": "ocean water"},
    {"id": 7, "vector": [0.7, 0.3], "text": "numeric identity", "n": 7},
    {"id": "7", "vector": [0.6, 0.4], "text": "string identity", "n": 8},
]
add(
    "schema and full row upsert",
    "write",
    {
        "schema": {"text": {"type": "string", "full_text_search": True}},
        "distance_metric": "euclidean_squared",
        "upsert_rows": rows,
    },
    count=6,
)
add("schema read", "schema", field="text")
add("metadata has real count and no fabricated backlog", "metadata", count=6)
ann = {"rank_by": ["vector", "ANN", [1, 0]], "top_k": 20, "include_attributes": True}


def q(name, f=None, ids=None, **extra):
    b = dict(ann)
    if f is not None:
        b["filters"] = f
    b.update(extra.pop("query", {}))
    add(name, "query", b, **({"ids": ids} if ids is not None else {}), **extra)


q("dense distance uses squared euclidean", first="a", dist=0)
q("squared L2 score conversion", ["id", "Eq", "b"], ["b"], dist=2)
q("string ID remains distinct", ["id", "Eq", "7"], ["7"])
q("numeric ID remains distinct", ["id", "Eq", 7], [7])
q("scalar equality", ["n", "Eq", 2], ["b"])
q("null equals missing", ["n", "Eq", None], ["c", "d"])
q("not-null excludes missing", ["n", "NotEq", None], ["a", "b", 7, "7"])
q("not-equal includes missing", ["n", "NotEq", 2], ["a", "c", "d", 7, "7"])
q("Gt", ["n", "Gt", 2], [7, "7"])
q("Gte", ["n", "Gte", 2], ["b", 7, "7"])
q("Lt", ["n", "Lt", 2], ["a"])
q("Lte", ["n", "Lte", 2], ["a", "b"])
q("In including null", ["n", "In", [1, None]], ["a", "c", "d"])
q("NotIn including null", ["n", "NotIn", [1, None]], ["b", 7, "7"])
q("empty In", ["n", "In", []], [])
q("empty NotIn", ["n", "NotIn", []], ["a", "b", "c", "d", 7, "7"])
q("boolean filter", ["active", "Eq", False], ["b"])
q("SQL text is bound", ["tag", "Eq", "x'; DROP TABLE nope;--"], ["a"])
q(
    "composed predicates",
    [
        "And",
        [["Or", [["n", "Eq", 1], ["n", "Eq", 2]]], ["Not", ["active", "Eq", False]]],
    ],
    ["a"],
)
q("Not handles missing explicitly", ["Not", ["n", "Gt", 2]], ["a", "b", "c", "d"])
q(
    "explicit attributes projection",
    ["id", "Eq", "a"],
    ["a"],
    query={"include_attributes": ["text"]},
    fields=["id", "$dist", "text"],
)
q(
    "false projection",
    ["id", "Eq", "a"],
    ["a"],
    query={"include_attributes": False},
    fields=["id", "$dist"],
)
q(
    "explicit vector projection",
    ["id", "Eq", "a"],
    ["a"],
    query={"include_attributes": ["vector"]},
    fields=["id", "$dist", "vector"],
)
add(
    "native limit alias",
    "query",
    {"rank_by": ["vector", "ANN", [1, 0]], "limit": 1},
    ids=["a"],
)
q(
    "BM25 ranked leg",
    ids=["a", "b"],
    query={"rank_by": ["text", "BM25", "database"]},
    first="a",
)
q(
    "BM25 scalar filter",
    ["n", "Eq", 2],
    ["b"],
    query={"rank_by": ["text", "BM25", "database"]},
)
q(
    "BM25 input is text, not SQL",
    ids=[],
    query={"rank_by": ["text", "BM25", "zzzz'; DROP TABLE nope;--"]},
)
add("single fetch", "fetch", {"id": "a"}, text="database database postgres")
add("batch fetch", "fetch_many", {"ids": ["a", "b", "missing"]}, ids=["a", "b"])
add(
    "hybrid preserves numeric and string IDs",
    "query",
    {
        "rank_by": [
            "text",
            "Auto",
            "identity",
            {"route": "fused", "vector": [1, 0], "fuzziness": 0},
        ],
        "filters": ["n", "Gte", 7],
        "top_k": 10,
    },
    ids=[7, "7"],
    hybrid=True,
)
# Complete unsupported writes must leave the table, schema and contents untouched.
for feature, value in [
    ("patch_rows", [{"id": "a", "n": 99}]),
    ("patch_columns", {"id": ["a"], "n": [99]}),
    ("delete_by_filter", ["n", "Eq", 1]),
    ("upsert_condition", ["n", "Eq", 1]),
    ("copy_from_namespace", "other"),
    ("distance_metric", "dot_product"),
]:
    add(
        "reject mixed " + feature,
        "write",
        {"upsert_rows": [{"id": "rogue", "vector": [1, 0], "n": 999}], feature: value},
        status=422,
        feature=feature,
    )
add(
    "schema DDL rolled back with unsupported option",
    "write",
    {"schema": {"new_field": "string"}, "patch_rows": []},
    status=422,
    feature="patch_rows",
)
add("schema unchanged after rejection", "schema", absent="new_field")
add("rejected writes unchanged count", "metadata", count=6)
q("original row unchanged", ["id", "Eq", "a"], ["a"], n=1)
for feature, value in [
    ("searchAfter", "cursor"),
    ("cursor", "cursor"),
    ("delete_by_filter", True),
    ("queries", [ann, ann]),
    ("aggregate_by", {"n": ["Sum", "n"]}),
    ("group_by", ["n"]),
    ("exclude_attributes", ["text"]),
    ("consistency", {"level": "strong"}),
    ("vector_encoding", "base64"),
]:
    q("reject query " + feature, query={feature: value}, status=422, feature=feature)
for op in ["Contains", "ContainsAny", "Fuzzy"]:
    q(
        "reject " + op,
        query={"filters": ["text", op, "database"]},
        status=422,
        feature=op,
    )
q(
    "unsupported filter",
    query={"filters": ["text", "Regex", ".*"]},
    status=422,
    feature="Regex",
)
q(
    "schema mismatch is validation error",
    query={"filters": ["n", "Eq", "wrong"]},
    status=400,
)
q(
    "dimension mismatch is validation error",
    query={"rank_by": ["vector", "ANN", [1, 2, 3]]},
    status=400,
)
add("type change rejected", "write", {"schema": {"n": "string"}}, status=400)
add("dimension change rejected", "write", {"schema": {"vector": "[3]f32"}}, status=400)
add(
    "multiple text fields rejected",
    "write",
    {"schema": {"title": {"type": "string", "full_text_search": True}}},
    status=422,
    feature="multiple full_text_search",
)
add(
    "column upsert replaces full row",
    "write",
    {
        "upsert_columns": {
            "id": ["a"],
            "vector": [[1, 0]],
            "text": ["updated database"],
            "n": [10],
        }
    },
    count=1,
)
add(
    "fetch reflects replacement",
    "fetch",
    {"id": "a"},
    text="updated database",
    absent="tag",
)
add("delete missing ID counts zero", "write", {"deletes": ["missing"]}, count=0)
add("delete numeric id only", "write", {"deletes": [7]}, count=1)
q("string ID survives numeric deletion", ["id", "Eq", "7"], ["7"])
q("numeric ID deleted", ["id", "Eq", 7], [])
add(
    "malformed columns atomic",
    "write",
    {"upsert_columns": {"id": ["z", "y"], "n": [1]}},
    status=400,
)
add("new nullable schema attribute", "write", {"schema": {"added": "string"}}, count=0)
add("new schema attribute readable", "schema", field="added")
add("warm hint explicitly unsupported", "warm", status=422, feature="hint_cache_warm")
add(
    "gateway BM25+dense RRF",
    "query",
    {
        "rank_by": [
            "text",
            "Auto",
            "database",
            {"route": "fused", "vector": [1, 0], "fuzziness": 0},
        ],
        "top_k": 3,
        "include_attributes": ["text"],
    },
    first="a",
    hybrid=True,
)
add(
    "fuzzy hybrid rejected",
    "query",
    {"rank_by": ["text", "HybridText", "database"], "top_k": 3},
    status=422,
    feature="Fuzzy",
)
add(
    "hybrid unknown field rejected",
    "query",
    {
        "rank_by": [
            "text",
            "Auto",
            "database",
            {"route": "fused", "vector": [1, 0], "fuzziness": 0},
        ],
        "searchAfter": "later",
    },
    status=422,
    feature="searchAfter",
)
add(
    "disable scalar filtering",
    "write",
    {"schema": {"n": {"type": "int", "filterable": False}}},
    count=0,
)
q("nonfilterable scalar rejected", query={"filters": ["n", "Eq", 10]}, status=400)
Path(__file__).with_name("cases.json").write_text(json.dumps(cases, indent=2) + "\n")
