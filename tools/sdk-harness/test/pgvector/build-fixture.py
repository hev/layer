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


# Served by the bundled CPU sidecar in CE Compose (api/embed#cpu-models).
EMBED_MODEL = "BAAI/bge-small-en-v1.5"
EMBED_DIMS = 384

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
# Ordered scans (LYR-112): exact order, not just the id set. Unsigned ids sort
# before string ids; strings compare in byte order; no $dist is returned.
add(
    "filter-only query orders by id",
    "query",
    {"top_k": 10},
    order=[7, "7", "a", "b", "c", "d"],
    fields=["id"],
)
add(
    "filter-only query with filter",
    "query",
    {"filters": ["n", "Gte", 2], "top_k": 10},
    order=[7, "7", "b"],
)
add(
    "rank_by attribute asc sorts nulls first",
    "query",
    {"rank_by": ["n", "asc"], "top_k": 10, "include_attributes": ["n"]},
    order=["c", "d", "a", "b", 7, "7"],
)
add(
    "rank_by attribute desc sorts nulls last with limit",
    "query",
    {"rank_by": ["n", "desc"], "limit": 3, "include_attributes": ["n"]},
    order=["7", 7, "b"],
    fields=["id", "n"],
    n=8,
)
add(
    "rank_by attribute with filter",
    "query",
    {"rank_by": ["n", "desc"], "filters": ["n", "Lt", 8], "top_k": 10},
    order=[7, "b", "a"],
)
add(
    "rank_by multiple attributes",
    "query",
    {"rank_by": [["active", "desc"], ["id", "asc"]], "top_k": 10},
    order=["a", "b", 7, "7", "c", "d"],
)
add("rank_by id desc", "query", {"rank_by": ["id", "desc"], "top_k": 2}, order=["d", "c"])
add(
    "page by string id filter",
    "query",
    {"rank_by": ["id", "asc"], "filters": ["id", "Gt", "7"], "top_k": 2},
    order=["a", "b"],
)
add(
    "page by numeric id filter",
    "query",
    {"rank_by": ["id", "asc"], "filters": ["id", "Gt", 7], "top_k": 2},
    order=["7", "a"],
)
add(
    "unknown rank_by attribute is validation error",
    "query",
    {"rank_by": ["missing", "asc"]},
    status=400,
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
# `feature` is the typed field on the 422 body (RFC 0118): the wire-feature id
# where one owns the key, otherwise the key itself.
for key, value, feature in [
    ("copy_from_namespace", "other", "copy_from_namespace"),
    ("branch_from_namespace", "other", "branch_from_namespace"),
    ("distance_metric", "dot_product", "distance_metric"),
]:
    add(
        "reject mixed " + key,
        "write",
        {"upsert_rows": [{"id": "rogue", "vector": [1, 0], "n": 999}], key: value},
        status=422,
        feature=feature,
    )
# RFC 0118 step C: the gateway serves schema `embed` on Postgres, but this
# namespace already holds client vectors, and an embedded namespace holds only
# its embedding target (rule 7). Neither the schema nor the rows change.
add(
    "reject schema embed beside client vectors",
    "write",
    {
        "schema": {"summary": {"type": "string", "embed": {"model": EMBED_MODEL}}},
        "upsert_rows": [{"id": "rogue-embed", "summary": "embedded on write"}],
    },
    status=422,
    feature="max_vector_fields",
)
add("schema unchanged after embed rejection", "schema", absent="summary")
add(
    "schema DDL rolled back with unsupported option",
    "write",
    {"schema": {"new_field": "string"}, "return_affected_ids": True},
    status=422,
    feature="return_affected_ids",
)
add("schema unchanged after rejection", "schema", absent="new_field")
add("rejected writes unchanged count", "metadata", count=6)
q("original row unchanged", ["id", "Eq", "a"], ["a"], n=1)
for key, value, feature in [
    ("searchAfter", "cursor", "search_after"),
    ("cursor", "cursor", "search_after"),
    ("delete_by_filter", True, "delete_by_filter"),
    ("queries", [ann, ann], "multi_query"),
    ("aggregate_by", {"n": ["Sum", "n"]}, "aggregate_by"),
    ("group_by", ["n"], "aggregate_by"),
    ("consistency", {"level": "strong"}, "consistency"),
    ("vector_encoding", "base64", "vector_encoding"),
]:
    q("reject query " + key, query={key: value}, status=422, feature=feature)
q(
    "reject Fuzzy",
    query={"filters": ["text", "Fuzzy", "database"]},
    status=422,
    feature="Fuzzy",
)
# Array operators need an array attribute; on a scalar they are malformed.
for op, value in [("Contains", "database"), ("ContainsAny", ["database"])]:
    q(op + " on a scalar is validation error", query={"filters": ["text", op, value]}, status=400)
add(
    "exclude_attributes drops the listed names",
    "query",
    {
        "rank_by": ["vector", "ANN", [1, 0]],
        "filters": ["id", "Eq", "a"],
        "exclude_attributes": ["text", "tag"],
    },
    ids=["a"],
    fields=["id", "$dist", "n", "active"],
)
q(
    "include_attributes with exclude_attributes is validation error",
    query={"exclude_attributes": ["text"]},
    status=400,
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
# A second full_text_search field on an existing namespace rebuilds the single
# BM25 index in the same write; each field then ranks by its own text.
add(
    "second text field added to existing namespace",
    "write",
    {
        "schema": {"title": {"type": "string", "full_text_search": True}},
        "upsert_rows": [
            {"id": "e", "vector": [0.5, 0.5], "text": "release notes", "title": "database"},
        ],
    },
    count=1,
)
add("schema lists both text fields", "schema", field="title")
q(
    "BM25 on the added field scores that field only",
    ids=["e"],
    query={"rank_by": ["title", "BM25", "database"]},
    first="e",
)
q(
    "BM25 on the original field is unchanged",
    ids=["a", "b"],
    query={"rank_by": ["text", "BM25", "database"]},
    first="a",
)
q(
    "BM25 on a non-text field rejected",
    query={"rank_by": ["n", "BM25", "database"]},
    status=400,
)
add("removing full_text_search rejected", "write", {"schema": {"title": "string"}}, status=400)
add("added text field row removed", "write", {"deletes": ["e"]}, count=1)
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
# Conditional writes: the condition reads the stored row; $ref_new reads the
# row being written. A new id always inserts. Rows: a n=10, b n=2, "7" n=8.
add(
    "conditional upsert with $ref_new",
    "write",
    {
        "upsert_rows": [
            {"id": "b", "vector": [0, 1], "text": "database vector", "n": 3},
            {"id": "7", "vector": [0.6, 0.4], "text": "stale", "n": 1},
            {"id": "e", "vector": [0.5, 0.5], "text": "new row", "n": 1},
        ],
        "upsert_condition": ["n", "Lt", {"$ref_new": "n"}],
    },
    count=2,
)
add(
    "failed condition keeps stored row",
    "query",
    {
        "rank_by": ["id", "asc"],
        "filters": ["id", "In", ["7", "b", "e"]],
        "include_attributes": ["n"],
    },
    order=["7", "b", "e"],
    n=8,
)
add(
    "passed condition replaced row",
    "query",
    {"filters": ["id", "Eq", "b"], "include_attributes": ["n"]},
    order=["b"],
    n=3,
)
add(
    "insert-only condition skips existing id",
    "write",
    {
        "upsert_rows": [{"id": "b", "vector": [0, 1], "n": 99}],
        "upsert_condition": ["id", "Eq", None],
    },
    count=0,
)
add(
    "conditional delete",
    "write",
    {"deletes": ["e", "b", "missing"], "delete_condition": ["n", "Gte", 3]},
    count=1,
)
add(
    "conditional writes leave expected rows",
    "query",
    {"top_k": 10},
    order=["7", "a", "c", "d", "e"],
)
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
    feature="fuzzy",
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
    feature="search_after",
)
add(
    "disable scalar filtering",
    "write",
    {"schema": {"n": {"type": "int", "filterable": False}}},
    count=0,
)
q("nonfilterable scalar rejected", query={"filters": ["n", "Eq", 10]}, status=400)

# LYR-137 and LYR-138, in a namespace of its own (`"ns": "sessions"`) shaped
# like hev kit's sessions namespace: array attributes, the dashboard's
# exclude_attributes session list and its ContainsAny filter by tool.
def sessions_case(name, op, body=None, **expected):
    add(name, op, body, ns="sessions", **expected)


sessions_case(
    "array attributes declared and inferred",
    "write",
    {
        "schema": {"tool_names": "[]string", "prompt_ts": {"type": "[]uint"}},
        "upsert_rows": [
            {"id": "s1", "start": 30, "first_prompt": "one", "tool_names": ["Bash", "Read"], "prompt_ts": [1, 2], "vector": [1, 0]},
            {"id": "s2", "start": 20, "first_prompt": "two", "tool_names": ["Edit"], "prompt_ts": [3], "vector": [0, 1]},
            {"id": "s3", "start": 10, "first_prompt": "three", "tool_names": [], "vector": [1, 1], "scores": [0.5, 2]},
            {"id": "s4", "start": 0, "first_prompt": "four", "vector": [1, 2]},
        ],
    },
    count=4,
)
sessions_case("schema lists an inferred array", "schema", field="scores")
sessions_case(
    "kit session list with exclude_attributes",
    "query",
    {"rank_by": ["start", "desc"], "top_k": 1000, "exclude_attributes": ["first_prompt", "vector"]},
    order=["s1", "s2", "s3", "s4"],
    fields=["id", "start", "tool_names", "prompt_ts"],
)
sessions_case(
    "ANN with exclude_attributes",
    "query",
    {"rank_by": ["vector", "ANN", [1, 0]], "top_k": 1, "exclude_attributes": ["tool_names", "prompt_ts"]},
    ids=["s1"],
    fields=["id", "$dist", "start", "first_prompt"],
)
for name, f, order in [
    ("ContainsAny", ["tool_names", "ContainsAny", ["Bash"]], ["s1"]),
    ("ContainsAny several", ["tool_names", "ContainsAny", ["Edit", "Read", "Grep"]], ["s1", "s2"]),
    ("ContainsAny empty list matches nothing", ["tool_names", "ContainsAny", []], []),
    ("NotContainsAny includes empty and missing", ["tool_names", "NotContainsAny", ["Bash", "Edit"]], ["s3", "s4"]),
    ("NotContainsAny empty list matches all", ["tool_names", "NotContainsAny", []], ["s1", "s2", "s3", "s4"]),
    ("Contains", ["tool_names", "Contains", "Read"], ["s1"]),
    ("NotContains", ["tool_names", "NotContains", "Read"], ["s2", "s3", "s4"]),
    ("Contains on uint array", ["prompt_ts", "Contains", 3], ["s2"]),
    ("ContainsAny on inferred float array", ["scores", "ContainsAny", [2]], ["s3"]),
    ("array filter composes", ["And", [["tool_names", "ContainsAny", ["Bash", "Edit"]], ["start", "Lt", 30]]], ["s2"]),
]:
    sessions_case(
        name,
        "query",
        {"rank_by": ["start", "desc"], "top_k": 10, "filters": f},
        order=order,
    )
sessions_case(
    "array element type mismatch is validation error",
    "query",
    {"top_k": 10, "filters": ["tool_names", "ContainsAny", [1]]},
    status=400,
)
sessions_case(
    "Eq on an array attribute unsupported",
    "query",
    {"top_k": 10, "filters": ["tool_names", "Eq", ["Bash"]]},
    status=422,
    feature="Eq",
)
sessions_case(
    "conditional upsert on an array attribute",
    "write",
    {
        "upsert_rows": [
            {"id": "s1", "start": 31, "tool_names": ["Bash"]},
            {"id": "s2", "start": 21, "tool_names": ["Edit"]},
        ],
        "upsert_condition": ["tool_names", "Contains", "Edit"],
    },
    count=1,
)

# LYR-140 (RFC 0115 slice B item 7): patches and filtered writes, in a
# namespace of their own (`"ns": "patch"`). A patch writes only the named
# attributes; a patch to a missing id is ignored; the write is one transaction.
def patch_case(name, op, body=None, **expected):
    add(name, op, body, ns="patch", **expected)


def patch_rows_after(name, **values):
    patch_case(
        name,
        "query",
        {"rank_by": ["id", "asc"], "top_k": 10, "include_attributes": True},
        values=values,
    )


patch_case(
    "patch seed rows",
    "write",
    {
        "upsert_rows": [
            {"id": "p1", "vector": [1, 0], "first_prompt": "one", "n": 1, "status": "published"},
            {"id": "p2", "vector": [0, 1], "first_prompt": "two", "n": 2, "status": "published"},
            {"id": "p3", "vector": [1, 1], "first_prompt": "three", "n": 3, "status": "draft"},
        ],
    },
    count=3,
)
# hev kit's PatchSessionSummaries body, verbatim in shape.
patch_case(
    "kit patch_rows summary with schema",
    "write",
    {
        "patch_rows": [{"id": "p1", "summary": "first session"}],
        "schema": {"summary": {"type": "string"}},
    },
    count=1,
    rows_patched=1,
)
patch_rows_after("patch keeps unnamed attributes", summary="first session", first_prompt="one", n=1)
patch_case("patched attribute is declared", "schema", field="summary")
patch_case(
    "patch_rows on a missing id is ignored",
    "write",
    {"patch_rows": [{"id": "missing", "n": 9}]},
    count=0,
    rows_patched=0,
)
patch_case("patch never creates a row", "metadata", count=3)
patch_case(
    "patch_columns with null clearing",
    "write",
    {"patch_columns": {"id": ["p1", "p2"], "summary": [None, "second"], "n": [10, 20]}},
    count=2,
    rows_patched=2,
)
patch_rows_after("patch_columns applied", summary=None, n=10)
patch_case(
    "patch_condition with $ref_new",
    "write",
    {
        "patch_rows": [{"id": "p1", "n": 5}, {"id": "p2", "n": 25}],
        "patch_condition": ["n", "Lt", {"$ref_new": "n"}],
    },
    count=1,
    rows_patched=1,
)
patch_case(
    "failed patch_condition keeps the row",
    "query",
    {"rank_by": ["id", "asc"], "top_k": 2, "include_attributes": ["n"]},
    order=["p1", "p2"],
    n=10,
)
patch_case(
    "patch_by_filter",
    "write",
    {"patch_by_filter": {"filters": ["status", "Eq", "published"], "patch": {"status": "archived"}}},
    count=2,
    rows_patched=2,
)
patch_case(
    "patch_by_filter result is filterable",
    "query",
    {"filters": ["status", "Eq", "archived"], "top_k": 10},
    order=["p1", "p2"],
)
patch_case(
    "delete_by_filter",
    "write",
    {"delete_by_filter": ["status", "Eq", "draft"]},
    count=1,
    rows_deleted=1,
)
patch_case("delete_by_filter removed the row", "metadata", count=2)
# Turbopuffer's order: delete_by_filter, patch_by_filter, then the rest.
patch_case(
    "filtered writes combine with upsert, patch and delete",
    "write",
    {
        "delete_by_filter": ["id", "Eq", "p2"],
        "patch_by_filter": {"filters": ["status", "Eq", "archived"], "patch": {"summary": "by filter"}},
        "upsert_rows": [{"id": "p4", "vector": [0.5, 0.5], "n": 4}],
        "patch_rows": [{"id": "p4", "status": "new"}, {"id": "p2", "status": "gone"}],
        "deletes": ["p1"],
    },
    count=5,
    rows_patched=2,
    rows_deleted=2,
)
patch_rows_after("combined write result", status="new", n=4, summary=None)
patch_case("combined write leaves one row", "metadata", count=1)
# Rejected writes change nothing, including a schema declared beside them.
for key, value, status, feature in [
    ("patch_rows", [{"id": "p4", "n": "wrong"}], 400, None),
    ("patch_columns", {"id": ["p4", "p5"], "n": [1]}, 400, None),
    ("patch_by_filter", {"filters": ["n", "Regex", "x"], "patch": {"n": 1}}, 422, "Regex"),
    ("delete_by_filter", ["nope", "Eq", 1], 400, None),
    ("patch_condition", ["n", "Lt", {"$ref_new": "status"}], 400, None),
]:
    body = {
        "schema": {"rejected_field": "string"},
        "upsert_rows": [{"id": "rogue", "vector": [1, 0], "n": 999}],
        "deletes": ["p4"],
        key: value,
    }
    if key == "patch_condition":
        body["patch_rows"] = [{"id": "p4", "n": 5}]
    patch_case(
        "rejected " + key + " is atomic",
        "write",
        body,
        status=status,
        **({"feature": feature} if feature else {}),
    )
patch_case("schema unchanged after rejected patches", "schema", absent="rejected_field")
patch_case("rows unchanged after rejected patches", "metadata", count=1)
patch_rows_after("row unchanged after rejected patches", status="new", n=4)

# RFC 0118 steps C and E: schema `embed` on Postgres, in a namespace of its own
# (`"ns": "embed"`), since an embedded namespace holds no client vectors. The
# gateway embeds with the configured provider; no row carries a vector.
def embed_case(name, op, body=None, **expected):
    add(name, op, body, ns="embed", **expected)


embed_schema = {
    "text": {"type": "string", "full_text_search": True, "embed": {"model": EMBED_MODEL}}
}
embed_rows = [
    {"id": "planet", "text": "Jupiter is the biggest planet in the Solar System."},
    {"id": "plant", "text": "Plants turn sunlight, water, and carbon dioxide into food."},
    {"id": "rust", "text": "The Rust borrow checker rejects aliasing mutable references."},
]
embed_case("embed schema-only write", "write", {"schema": embed_schema}, count=0)
embed_case(
    "schema-only write declares no vector column yet",
    "schema",
    field="text",
    embedded="text",
    absent="embed_text",
)
embed_case(
    "embed write with text-only rows",
    "write",
    {"schema": embed_schema, "upsert_rows": embed_rows},
    count=3,
)
embed_case("schema shows the vector target", "schema", field="embed_text", embedded="text")
embed_case(
    "rows carry gateway vectors",
    "query",
    {"filters": ["id", "Eq", "planet"], "include_attributes": ["embed_text"]},
    ids=["planet"],
    vector=["embed_text", EMBED_DIMS],
)
embed_case(
    "ANN over Embed resolves on Postgres",
    "query",
    {"rank_by": ["text", "ANN", ["Embed", "largest planet in the solar system"]], "top_k": 3},
    first="planet",
)
embed_case(
    "Auto semantic route embeds",
    "query",
    {
        "rank_by": [
            "text",
            "Auto",
            "which one of these is about how green plants make their food",
            {"vector": ["Embed", "which one of these is about how green plants make their food"]},
        ],
        "top_k": 3,
    },
    first="plant",
)
embed_case(
    "Auto fused route runs BM25 and semantic legs",
    "query",
    {
        "rank_by": [
            "text",
            "Auto",
            "borrow checker references",
            {"vector": ["Embed", "borrow checker references"], "fuzziness": 0},
        ],
        "top_k": 3,
    },
    first="rust",
    hybrid=True,
)
embed_case(
    "rows-only write still embeds",
    "write",
    {"upsert_rows": [{"id": "ocean", "text": "The ocean covers most of the Earth."}]},
    count=1,
)
embed_case(
    "rows-only write stored a vector",
    "query",
    {"filters": ["id", "Eq", "ocean"], "include_attributes": ["embed_text"]},
    ids=["ocean"],
    vector=["embed_text", EMBED_DIMS],
)
embed_case(
    "reject a second embedded attribute",
    "write",
    {
        "schema": {"title": {"type": "string", "embed": {"model": EMBED_MODEL}}},
        "upsert_rows": [{"id": "second", "text": "x", "title": "y"}],
    },
    status=422,
    feature="schema.embed",
)
embed_case(
    "reject a model change on an embedded namespace",
    "write",
    {
        "schema": {
            "text": {
                "type": "string",
                "full_text_search": True,
                "embed": {"model": "sentence-transformers/all-MiniLM-L6-v2"},
            }
        },
        "upsert_rows": [{"id": "changed", "text": "x"}],
    },
    status=422,
    feature="schema.embed",
)
embed_case(
    "reject a client vector beside embed",
    "write",
    {"upsert_rows": [{"id": "client-vector", "text": "x", "vector": [1, 0]}]},
    status=422,
    feature="max_vector_fields",
)
embed_case(
    "reject chunked embedding",
    "write",
    {
        "schema": {
            "text": {
                "type": "string",
                "full_text_search": True,
                "embed": {"model": EMBED_MODEL, "chunk": {"strategy": "fixed", "size": 20}},
            }
        },
        "upsert_rows": [{"id": "chunked", "text": "x"}],
    },
    status=422,
    feature="embed.chunk",
)
embed_case("rejected embed writes leave the count", "metadata", count=4)
# A patch cannot recompute the embedding, so the gateway refuses to patch its
# source (a 422 validation_error, not UnsupportedByStore: no store takes it);
# an unrelated attribute still patches.
embed_case(
    "reject a patch to the embedded source",
    "write",
    {"patch_rows": [{"id": "planet", "text": "Saturn has rings."}]},
    status=422,
)
embed_case(
    "reject a filter patch to the embedded source",
    "write",
    {"patch_by_filter": {"filters": ["id", "Eq", "planet"], "patch": {"text": "Saturn"}}},
    status=422,
)
embed_case(
    "patch an attribute beside the embedded one",
    "write",
    {"patch_rows": [{"id": "planet", "topic": "astronomy"}]},
    count=1,
    rows_patched=1,
)
embed_case(
    "ANN over Embed after the patch",
    "query",
    {"rank_by": ["text", "ANN", ["Embed", "largest planet in the solar system"]], "top_k": 1, "include_attributes": ["topic"]},
    first="planet",
    values={"topic": "astronomy"},
)
Path(__file__).with_name("cases.json").write_text(json.dumps(cases, indent=2) + "\n")
