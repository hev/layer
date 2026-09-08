import { Hevlayer, HevlayerError, type FetchLike, type LayerResponse } from "../src/index.js";

// @ts-ignore Node provides this built-in module at runtime; the generated package has no dev dependency on @types/node.
const { test } = await import("node:test");

test("core operations use the generated surface", async () => {
  const seen = new Set<string>();
  let scanPolls = 0;
  const fetch: FetchLike = async (input, init) => {
    const url = new URL(String(input));
    const method = init?.method ?? "GET";
    const headers = new Headers(init?.headers);
    const body = typeof init?.body === "string" ? JSON.parse(init.body) : undefined;
    assertEqual(headers.get("authorization"), "Bearer test-token");

    if (method === "GET" && url.pathname === "/v2/namespaces/ns/documents/doc-1") {
      seen.add("fetchDocument");
      assertEqual(url.searchParams.get("include_attributes"), "title,price");
      return jsonResponse({ id: "doc-1", attributes: { title: "Boot", price: 10 } }, { headers: { "x-layer-cache": "hit" } });
    }
    if (method === "POST" && url.pathname === "/v2/namespaces/ns/query") {
      seen.add("queryNamespace");
      assertEqual(headers.get("x-hevlayer-search-query"), "boots");
      assertEqual(headers.get("x-hevlayer-tags"), "app:shop,surface:store");
      return jsonResponse({ rows: [{ id: "doc-1", "$dist": 0.1, title: "Boot" }] }, { headers: { "x-layer-stable-as-of": "42" } });
    }
    if (method === "GET" && url.pathname === "/v2/namespaces/ns/search-history") {
      seen.add("listSearchHistory");
      assertEqual(url.searchParams.get("tag"), "app:shop,surface:store");
      assertEqual(url.searchParams.get("limit"), "10");
      return jsonResponse({ entries: [{ timestamp: "2026-05-22T08:00:00.000Z", timestamp_nanos: 1747900800000000000, namespace: "ns", trace_id: "trace", raw_query: "boots", stable_as_of: 42, query: { top_k: 1 }, top_result_ids: ["doc-1"], tags: ["app:shop"] }], next_cursor: null });
    }
    if (method === "POST" && url.pathname === "/v2/namespaces/ns" && url.search === "") {
      if (body.patch_columns) {
        seen.add("patchColumns");
        assertDeepEqual(body.patch_columns.id, ["doc-1", "doc-2"]);
      } else if (body.patch_rows) {
        seen.add("writeNamespace");
        assertEqual(body.patch_rows[0].id, "doc-3");
      } else {
        seen.add("writeNamespace");
        assertOk(Boolean(body.upsert_rows));
      }
      return jsonResponse({ status: "OK", message: "written", rows_affected: 1, billing: {} });
    }
    if (method === "POST" && url.pathname === "/v2/namespaces/ns-branch") {
      seen.add("branchNamespace");
      assertEqual(url.searchParams.get("stainless_overload"), "branchFrom");
      return jsonResponse({ status: "OK", message: "branched", rows_affected: 0, billing: {} });
    }
    if (method === "POST" && url.pathname === "/v1/namespaces/ns/_debug/recall") {
      seen.add("recall");
      return jsonResponse({ avg_recall: 1, avg_exhaustive_count: 10, avg_ann_count: 10 });
    }
    if (method === "POST" && url.pathname === "/v2/namespaces/ns/snapshots") {
      seen.add("createSnapshot");
      return jsonResponse({ id: "snap-1", namespace: "ns", field: "category", source: "origin", status: "running", progress: 0, documents_scanned: 0, created_at: "2026-06-07T18:21:04Z" }, { status: 202 });
    }
    if (method === "GET" && url.pathname === "/v2/namespaces/ns/snapshot-jobs/snap-1") {
      seen.add("getSnapshotJob");
      return jsonResponse({ id: "snap-1", namespace: "ns", field: "category", source: "origin", status: "completed", progress: 1, documents_scanned: 10, sha: "abc1234", created_at: "2026-06-07T18:21:04Z" });
    }
    if (method === "POST" && url.pathname === "/v2/pipelines") {
      seen.add("createPipelineConflict");
      return jsonResponse({ error: "conflict", message: "exists" }, { status: 409 });
    }
    if (method === "GET" && url.pathname === "/v2/pipelines") {
      seen.add("listPipelines");
      return jsonResponse({ pipelines: [{ id: "p1", target_namespace: "ns", distance_metric: "cosine", created_at: "2026-06-07T18:21:04Z" }] });
    }
    if (method === "POST" && url.pathname === "/v2/pipelines/p1/documents/stage") {
      if (body.stage === "indexed") {
        seen.add("completeDocuments");
        assertEqual(body.from_stage, "embedding");
        assertEqual(body.worker_id, "w1");
      } else if (body.stage === "pending") {
        seen.add("releaseDocuments");
      } else if (body.stage === "failed") {
        seen.add("failDocuments");
      } else {
        throw new Error("unexpected stage: " + body.stage);
      }
      return jsonResponse({ pipeline_id: "p1", stage: "indexed", updated: 2 });
    }
    if (method === "PUT" && url.pathname === "/v2/pipelines/p1/documents/doc-1/vectors") {
      if (body.vectors[0].id === "doc-1:chunk-1") {
        seen.add("writeSingleVector");
      } else if (body.vectors[0].id === "doc-1:multi-1") {
        seen.add("writeSingleMultivector");
        assertDeepEqual(body.vectors[0].vectors, [[0.1, 0.2], [0.3, 0.4]]);
        assertEqual(body.vectors[0].attributes.kind, "late");
      } else {
        throw new Error("unexpected vector id: " + body.vectors[0].id);
      }
      return jsonResponse({ status: "ok", message: "vector" });
    }
    if (method === "POST" && url.pathname === "/v2/namespaces/ns/warm") {
      seen.add("warmNamespace");
      assertEqual(url.searchParams.get("page_size"), "42");
      return jsonResponse({ id: "warm-1", namespace: "ns", status: "completed", progress: 1, documents_scanned: 10, created_at: "2026-06-07T18:21:04Z" }, { status: 201 });
    }
    if (method === "POST" && url.pathname === "/v2/namespaces/ns/scans") {
      seen.add("createScan");
      return jsonResponse({ id: "scan-1", namespace: "ns", source: "origin", status: "running", progress: 0, documents_scanned: 0, created_at: "2026-06-07T18:21:04Z" }, { status: 202 });
    }
    if (method === "GET" && url.pathname === "/v2/namespaces/ns/scans/scan-1") {
      seen.add("getScan");
      scanPolls += 1;
      return jsonResponse({ id: "scan-1", namespace: "ns", source: "origin", status: scanPolls === 1 ? "running" : "completed", progress: scanPolls === 1 ? 0.5 : 1, documents_scanned: 10, created_at: "2026-06-07T18:21:04Z" });
    }
    if (method === "GET" && url.pathname === "/v2/metrics/query") {
      seen.add("queryMetrics");
      assertEqual(url.searchParams.get("query"), "up");
      return jsonResponse({ status: "success", data: { result: [] } });
    }
    if (method === "GET" && url.pathname === "/v2/metrics/query_range") {
      seen.add("queryMetricsRange");
      assertEqual(url.searchParams.get("query"), "rate(up[5m])");
      return jsonResponse({ status: "success", data: { result: [] } });
    }
    if (method === "DELETE" && url.pathname === "/v2/namespaces/ns/scans/scan-1") {
      seen.add("deleteScan");
      return jsonResponse({ status: "OK", message: "deleted" });
    }
    if (method === "DELETE" && url.pathname === "/v2/pipelines/p1") {
      seen.add("deletePipeline");
      return jsonResponse({ status: "OK", message: "deleted" });
    }
    if (method === "DELETE" && url.pathname === "/v2/namespaces/old-products") {
      seen.add("deleteNamespace");
      return jsonResponse({ status: "OK", message: "deleted" });
    }
    throw new Error("unexpected request: " + method + " " + url.toString());
  };

  const client = new Hevlayer({ baseUrl: "https://unit.test", apiKey: "  test-token\n", fetch });
  const fetched = await client.fetchDocument("ns", "doc-1", { includeAttributes: ["title", "price"], withPerf: true }) as LayerResponse<any>;
  assertEqual(fetched.perf.cacheStatus, "hit");
  assertEqual(fetched.data.id, "doc-1");

  const query = await client.queryNamespace("ns", { vector: [0.1, 0.2], top_k: 1, include_attributes: ["title"] }, { searchQuery: " boots ", tags: ["surface:store", "app:shop", "surface:store"], withPerf: true }) as LayerResponse<any>;
  assertEqual(query.data.stable_as_of, 42);
  assertEqual(query.data.rows[0].id, "doc-1");

  await client.listSearchHistory("ns", { tags: ["surface:store", "app:shop"], limit: 10 });
  await assertRejects(() => client.listSearchHistory("ns", { tags: ["bad,tag"] }));

  const write = await client.writeNamespace("ns", { upsert_rows: [{ id: "doc-1", vector: [0.1, 0.2] }] });
  assertEqual(write.status, "OK");
  await client.writeNamespace("ns", { patch_rows: [{ id: "doc-3", category: "Audio" }] });
  await client.branchNamespace("ns-branch", { branch_from_namespace: { source_namespace: "ns" } });
  await client.evaluateTurbopufferRecall("ns", { num: 5, top_k: 10 });
  const snapshot = await client.createSnapshot("ns", { field: "category", source: "origin" });
  assertEqual(snapshot.status, "running");
  const snapshotDone = await client.getSnapshotJob("ns", "snap-1");
  assertEqual(snapshotDone.status, "completed");

  const pipeline = await client.ensurePipeline({ id: "p1", target_namespace: "ns" });
  assertEqual(pipeline.id, "p1");
  await client.releaseDocuments("p1", ["doc-1"]);
  await client.failDocuments("p1", ["doc-2"]);
  const completed = await client.completeDocuments("p1", ["doc-1", "doc-2"], { fromStage: "embedding", workerId: "w1" });
  assertEqual(completed.updated, 2);
  await client.writeSingleVector("p1", "doc-1", { id: "doc-1:chunk-1", vector: [0.3, 0.4], attributes: { kind: "review" } });
  await client.writeSingleMultivector("p1", "doc-1", "doc-1:multi-1", [[0.1, 0.2], [0.3, 0.4]], { kind: "late" });
  await client.patchColumns("ns", ["doc-1", "doc-2"], { tags: [["durable"], ["soft"]], tags_v: ["v1", "v1"] });
  await assertRejects(() => client.patchColumns("ns", [""], { tags: ["bad"] }));
  await assertRejects(() => client.patchColumns("ns", ["doc-1"], { id: ["bad"] }));
  await client.warmNamespace("ns", { pageSize: 42 });
  const scan = await client.scan("ns", { source: "origin" }, { initialDelayMs: 1, maxDelayMs: 1, timeoutMs: 100 });
  assertEqual(scan.status, "completed");
  await client.queryMetrics({ query: "up" });
  await client.queryMetricsRange({ query: "rate(up[5m])", start: "1", end: "2", step: "1m" });
  await client.deleteScan("ns", "scan-1");
  await client.deletePipeline("p1");
  await client.deleteNamespace("old-products");

  for (const key of ["fetchDocument", "queryNamespace", "listSearchHistory", "writeNamespace", "branchNamespace", "recall", "createSnapshot", "getSnapshotJob", "createPipelineConflict", "listPipelines", "releaseDocuments", "failDocuments", "completeDocuments", "writeSingleVector", "writeSingleMultivector", "patchColumns", "warmNamespace", "createScan", "getScan", "queryMetrics", "queryMetricsRange", "deleteScan", "deletePipeline", "deleteNamespace"]) {
    assertOk(seen.has(key), "operation was not exercised: " + key);
  }
});

function jsonResponse(body: unknown, options: { status?: number; headers?: Record<string, string> } = {}): Response {
  return new Response(JSON.stringify(body), {
    status: options.status ?? 200,
    headers: { "content-type": "application/json", ...(options.headers ?? {}) },
  });
}

function assertEqual(actual: unknown, expected: unknown, message?: string): void {
  if (actual !== expected) {
    throw new Error(message ?? "expected " + JSON.stringify(actual) + " to equal " + JSON.stringify(expected));
  }
}

function assertDeepEqual(actual: unknown, expected: unknown, message?: string): void {
  if (JSON.stringify(actual) !== JSON.stringify(expected)) {
    throw new Error(message ?? "expected " + JSON.stringify(actual) + " to deep equal " + JSON.stringify(expected));
  }
}

function assertOk(value: unknown, message?: string): void {
  if (!value) {
    throw new Error(message ?? "expected value to be truthy");
  }
}

async function assertRejects(fn: () => Promise<unknown>, predicate?: (error: unknown) => boolean): Promise<void> {
  try {
    await fn();
  } catch (error) {
    if (predicate && !predicate(error)) {
      throw new Error("rejection did not match predicate: " + String(error));
    }
    return;
  }
  throw new Error("expected promise to reject");
}
