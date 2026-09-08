// Live phase-one suite using the committed, generated TypeScript client.
import assert from "node:assert/strict";
import fs from "node:fs";
import { randomUUID } from "node:crypto";
import {
  Hevlayer,
  HevlayerError,
} from "../../../../clients/typescript/src/client.js";

const client = new Hevlayer({
  baseUrl: process.env.PGVECTOR_GATEWAY_URL ?? "http://127.0.0.1:8080",
});
const prefix = process.env.PGVECTOR_TEST_PREFIX ?? "scratch-lyr20";
const ns = `${prefix}-${new Date().toISOString().slice(0, 10).replaceAll("-", "")}-ts-${randomUUID().slice(0, 8)}-'雪_%41`;
const cases = JSON.parse(
  fs.readFileSync(new URL("./cases.json", import.meta.url), "utf8"),
);
try {
  for (const c of cases) {
    const body = c.body ?? {};
    const methods: Record<string, () => Promise<unknown>> = {
      write: () => client.writeNamespace(ns, body),
      query: () => client.queryNamespace(ns, body),
      schema: () => client.getTurbopufferNamespaceSchema(ns),
      metadata: () => client.getNamespaceMetadata(ns),
      fetch: () => client.fetchDocument(ns, body.id),
      fetch_many: () => client.fetchDocuments(ns, body),
      warm: () => client.hintCacheWarm(ns),
    };
    let result: any;
    try {
      result = await methods[c.op]();
    } catch (e) {
      assert(e instanceof HevlayerError, `${c.name}: ${e}`);
      assert.equal(e.statusCode, c.status ?? 200, `${c.name}: ${e}`);
      if (c.feature) {
        assert.equal(e.kind, "UnsupportedByStore", c.name);
        assert(e.message.includes(c.feature), `${c.name}: ${e}`);
      }
      console.log("PASS", c.name);
      continue;
    }
    assert.equal(c.status ?? 200, 200, `${c.name}: unexpected success`);
    if (c.op === "write") assert.equal(result.rows_affected, c.count, c.name);
    if (c.op === "metadata") {
      assert.equal(result.approx_row_count, c.count, c.name);
      assert(!result.index, c.name);
    }
    if (c.op === "schema") {
      if (c.field) assert(c.field in result, c.name);
      if (c.absent) assert(!(c.absent in result), c.name);
    }
    if (c.op === "query") {
      const rows = result.rows;
      if (c.ids)
        assert.deepEqual(
          new Set(rows.map((r: any) => JSON.stringify(r.id))),
          new Set(c.ids.map((id: unknown) => JSON.stringify(id))),
          c.name,
        );
      if (c.first) assert.equal(rows[0].id, c.first, c.name);
      if ("dist" in c) assert(Math.abs(rows[0].$dist - c.dist) < 1e-6, c.name);
      if (c.fields)
        assert.deepEqual(
          new Set(Object.keys(rows[0])),
          new Set(c.fields),
          c.name,
        );
      if ("n" in c) assert.equal(rows[0].n, c.n, c.name);
      if (c.hybrid) assert(result.hybrid, c.name);
    }
    if (c.op === "fetch") {
      assert.equal(result.attributes.text, c.text, c.name);
      if (c.absent) assert(!(c.absent in result.attributes), c.name);
    }
    if (c.op === "fetch_many")
      assert.deepEqual(
        new Set(result.documents.map((r: any) => r.id)),
        new Set(c.ids),
        c.name,
      );
    console.log("PASS", c.name);
  }
  const listing = await client.listTurbopufferNamespaces({ prefix: ns });
  assert(listing.namespaces.some((n) => n.id === ns));
} finally {
  await client.deleteNamespace(ns);
  const listing = await client.listTurbopufferNamespaces({ prefix: ns });
  assert.equal(listing.namespaces.length, 0, "namespace cleanup failed");
}
console.log(
  `TypeScript: ${cases.length} phase-one cases plus namespace listing/cleanup passed`,
);
