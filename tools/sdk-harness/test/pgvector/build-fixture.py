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
# where one owns the key, otherwise the key itself. `patch_condition` is owned
# by `conditional_writes`, which Postgres serves for upserts and deletes only.
for key, value, feature in [
    ("patch_rows", [{"id": "a", "n": 99}], "patch_rows"),
    ("patch_columns", {"id": ["a"], "n": [99]}, "patch_columns"),
    ("delete_by_filter", ["n", "Eq", 1], "delete_by_filter"),
    ("patch_condition", ["n", "Eq", 1], "conditional_writes"),
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
    {"schema": {"new_field": "string"}, "patch_rows": []},
    status=422,
    feature="patch_rows",
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
    ("exclude_attributes", ["text"], "exclude_attributes"),
    ("consistency", {"level": "strong"}, "consistency"),
    ("vector_encoding", "base64", "vector_encoding"),
]:
    q("reject query " + key, query={key: value}, status=422, feature=feature)
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
Path(__file__).with_name("cases.json").write_text(json.dumps(cases, indent=2) + "\n")
