package cmd

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/hev/layer/apps/layer-cli/internal/output"
	hevlayer "github.com/hev/layer/clients/go"
)

func TestIndexListJSONIncludesLastSnapshot(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requireAuth(t, r)
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v2/namespaces":
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"namespaces":  []map[string]interface{}{{"name": "shop-products", "row_count": 10}},
				"next_cursor": "",
			})
		case r.Method == http.MethodGet && r.URL.Path == "/v2/activity/snapshots":
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"events": []map[string]interface{}{{"ts_ms": 1700000000000, "namespace": "shop-products", "sha": "abc123"}},
			})
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	stdout, stderr, code := runTestCLI(t, server.URL, []string{"index", "list", "-o", "json"})
	if code != ExitOK {
		t.Fatalf("code=%d stderr=%s", code, stderr)
	}
	if !strings.Contains(stdout, `"last_snapshot_ms": 1700000000000`) {
		t.Fatalf("last snapshot missing from json: %s", stdout)
	}
}

func TestIndexListBestEffortSnapshotFeed(t *testing.T) {
	// A failing activity feed must not fail the listing: the table still renders.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requireAuth(t, r)
		if r.URL.Path == "/v2/activity/snapshots" {
			http.Error(w, `{"error":"unavailable","message":"down"}`, http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"namespaces":  []map[string]interface{}{{"name": "shop-products", "row_count": 10}},
			"next_cursor": "",
		})
	}))
	defer server.Close()

	stdout, stderr, code := runTestCLI(t, server.URL, []string{"index", "list"})
	if code != ExitOK {
		t.Fatalf("code=%d stderr=%s", code, stderr)
	}
	if !strings.Contains(stdout, "LAST_SNAPSHOT_MS") || !strings.Contains(stdout, "shop-products") {
		t.Fatalf("table missing rows/column: %s", stdout)
	}
}

func TestIndexGetShowsSnapshotHistory(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requireAuth(t, r)
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v2/namespaces":
			if r.URL.Query().Get("prefix") != "shop-products" {
				t.Fatalf("missing prefix: %s", r.URL.RawQuery)
			}
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"namespaces": []map[string]interface{}{
					{"name": "shop-products", "row_count": 10, "size_bytes": 2048, "is_stable": true, "stable_as_of_ms": 1699999999000, "last_write_ms": 1700000000000},
					{"name": "shop-products-staging", "row_count": 1}, // prefix sibling must be ignored
				},
				"next_cursor": "",
			})
		case r.Method == http.MethodGet && r.URL.Path == "/v2/namespaces/shop-products/history":
			_ = json.NewEncoder(w).Encode([]map[string]interface{}{
				{"watermark_ms": 1700000000000, "sha": "abc1234deadbeef"},
				{"watermark_ms": 1699999000000, "sha": "ffff000011112222"},
			})
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	stdout, stderr, code := runTestCLI(t, server.URL, []string{"index", "get", "shop-products", "-o", "json"})
	if code != ExitOK {
		t.Fatalf("code=%d stderr=%s", code, stderr)
	}
	if !strings.Contains(stdout, `"snapshots": 2`) {
		t.Fatalf("snapshot count missing: %s", stdout)
	}
	if !strings.Contains(stdout, `"last_snapshot_ms": 1700000000000`) {
		t.Fatalf("last snapshot missing: %s", stdout)
	}

	stdout, stderr, code = runTestCLI(t, server.URL, []string{"index", "get", "shop-products"})
	if code != ExitOK {
		t.Fatalf("table code=%d stderr=%s", code, stderr)
	}
	if !strings.Contains(stdout, "LAST_SNAPSHOT_MS") || !strings.Contains(stdout, "1700000000000") {
		t.Fatalf("table detail missing last snapshot: %s", stdout)
	}
	if !strings.Contains(stdout, "RECENT SNAPSHOTS") || !strings.Contains(stdout, "abc1234deadbeef") {
		t.Fatalf("recent snapshots missing: %s", stdout)
	}
}

func TestIndexGetNotFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requireAuth(t, r)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"namespaces": []map[string]interface{}{}, "next_cursor": ""})
	}))
	defer server.Close()

	_, stderr, code := runTestCLI(t, server.URL, []string{"index", "get", "missing-index"})
	if code != ExitFailed {
		t.Fatalf("code=%d stderr=%s", code, stderr)
	}
	if !strings.Contains(stderr, `index "missing-index" not found`) {
		t.Fatalf("unexpected stderr: %q", stderr)
	}
}

func TestIndexDeleteByPrefix(t *testing.T) {
	var deleted []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requireAuth(t, r)
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v2/namespaces":
			if r.URL.Query().Get("prefix") != "shop-" {
				t.Fatalf("missing prefix: %s", r.URL.RawQuery)
			}
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"namespaces": []map[string]interface{}{
					{"name": "shop-products"},
					{"name": "shop-orders"},
				},
				"next_cursor": "",
			})
		case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/v2/namespaces/"):
			deleted = append(deleted, strings.TrimPrefix(r.URL.Path, "/v2/namespaces/"))
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"message": "namespace deleted"})
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	// Non-TTY requires --yes; the prefix expands to every matching index.
	stdout, stderr, code := runTestCLI(t, server.URL, []string{"index", "delete", "--prefix", "shop-", "--yes", "-o", "names"})
	if code != ExitOK {
		t.Fatalf("code=%d stderr=%s", code, stderr)
	}
	if len(deleted) != 2 || deleted[0] != "shop-products" || deleted[1] != "shop-orders" {
		t.Fatalf("unexpected deletions: %v", deleted)
	}
	if !strings.Contains(stdout, "shop-products") || !strings.Contains(stdout, "shop-orders") {
		t.Fatalf("output missing deleted names: %q", stdout)
	}
}

func TestIndexDeletePrefixNoMatch(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requireAuth(t, r)
		if r.Method == http.MethodDelete {
			t.Fatalf("must not delete when prefix matches nothing: %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"namespaces": []map[string]interface{}{}, "next_cursor": ""})
	}))
	defer server.Close()

	_, stderr, code := runTestCLI(t, server.URL, []string{"index", "delete", "--prefix", "absent-", "--yes"})
	if code != ExitOK {
		t.Fatalf("code=%d stderr=%s", code, stderr)
	}
	if !strings.Contains(stderr, `No indexes match prefix "absent-"`) {
		t.Fatalf("expected no-match notice: %q", stderr)
	}
}

func TestIndexDeletePrefixRejectsNamesAndPrefix(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("must not call gateway on usage error: %s %s", r.Method, r.URL.Path)
	}))
	defer server.Close()

	_, stderr, code := runTestCLI(t, server.URL, []string{"index", "delete", "shop-products", "--prefix", "shop-", "--yes"})
	if code != ExitUsage {
		t.Fatalf("code=%d stderr=%s", code, stderr)
	}
	if !strings.Contains(stderr, "either NAME arguments or --prefix") {
		t.Fatalf("expected mutual-exclusion error: %q", stderr)
	}
}

func TestVectorstoreCommandsUseGatewayAPI(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requireAuth(t, r)
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v2/vectorstores":
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"vectorstores": []map[string]interface{}{
					vectorstorePayload("prod-turbopuffer", true),
					vectorstorePayload("backup-turbopuffer", false),
				},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/v2/vectorstores/prod-turbopuffer":
			_ = json.NewEncoder(w).Encode(vectorstorePayload("prod-turbopuffer", true))
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	stdout, stderr, code := runTestCLI(t, server.URL, []string{"vectorstore", "list", "-o", "names"})
	if code != ExitOK {
		t.Fatalf("code=%d stderr=%s", code, stderr)
	}
	if stdout != "prod-turbopuffer\nbackup-turbopuffer\n" {
		t.Fatalf("unexpected names output: %q", stdout)
	}

	stdout, stderr, code = runTestCLI(t, server.URL, []string{"vectorstore", "get"})
	if code != ExitOK {
		t.Fatalf("default get code=%d stderr=%s", code, stderr)
	}
	if !strings.Contains(stdout, "TURBOPUFFER_URL") || !strings.Contains(stdout, "organizations/org_123") {
		t.Fatalf("default detail missing deep link: %s", stdout)
	}

	stdout, stderr, code = runTestCLI(t, server.URL, []string{"vectorstore", "get", "prod-turbopuffer", "-o", "json"})
	if code != ExitOK {
		t.Fatalf("named get code=%d stderr=%s", code, stderr)
	}
	if !strings.Contains(stdout, `"turbopufferUrl": "https://turbopuffer.com/organizations/org_123"`) {
		t.Fatalf("json detail missing link: %s", stdout)
	}
}

func TestWarehouseCommandsUseGatewayAPI(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requireAuth(t, r)
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v2/warehouses":
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"warehouses": []map[string]interface{}{warehousePayload()},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/v2/warehouses/prod-snowflake":
			_ = json.NewEncoder(w).Encode(warehousePayload())
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	stdout, stderr, code := runTestCLI(t, server.URL, []string{"warehouse", "list"})
	if code != ExitOK {
		t.Fatalf("code=%d stderr=%s", code, stderr)
	}
	if !strings.Contains(stdout, "PHASE") || !strings.Contains(stdout, "Verified") || !strings.Contains(stdout, "prod-snowflake") {
		t.Fatalf("warehouse list missing health row: %s", stdout)
	}

	stdout, stderr, code = runTestCLI(t, server.URL, []string{"warehouse", "get", "prod-snowflake"})
	if code != ExitOK {
		t.Fatalf("get code=%d stderr=%s", code, stderr)
	}
	for _, want := range []string{"ACCOUNT", "acme-xy12345", "CONSUMER_PIPELINES", "2", "CONSUMER_API_KEYS", "1"} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("warehouse detail missing %q: %s", want, stdout)
		}
	}
}

func TestEmitPipelinesTable(t *testing.T) {
	var buf bytes.Buffer
	rows := []pipelineRow{
		{
			Pipeline: hevlayer.Pipeline{ID: "images", TargetNamespace: "products"},
			Queue:    &hevlayer.PipelineStatus{PendingCount: 5, ProcessingCount: 2, FailedCount: 1, IndexedRatePerMin: 12.5},
		},
		{Pipeline: hevlayer.Pipeline{ID: "embed", TargetNamespace: "docs"}}, // no worker registered yet
	}
	if err := emitPipelines(&buf, output.Table, rows); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if !strings.Contains(out, "ID") || !strings.Contains(out, "RATE/MIN") {
		t.Fatalf("header missing: %s", out)
	}
	if !strings.Contains(out, "images") || !strings.Contains(out, "products") {
		t.Fatalf("row missing: %s", out)
	}
	if !strings.Contains(out, "—") { // embed has no queue registered
		t.Fatalf("expected dash for missing queue: %s", out)
	}
}

func vectorstorePayload(name string, isDefault bool) map[string]interface{} {
	return map[string]interface{}{
		"name":    name,
		"kind":    "turbopuffer",
		"default": isDefault,
		"endpoint": map[string]interface{}{
			"url":    "https://aws-us-east-1.turbopuffer.com",
			"region": "aws-us-east-1",
		},
		"turbopuffer": map[string]interface{}{
			"orgId": "org_123",
		},
		"credential": map[string]interface{}{
			"secretRef": map[string]interface{}{
				"name": "layer-turbopuffer",
				"key":  "turbopuffer-api-key",
			},
		},
		"inboundAuth": map[string]interface{}{
			"mode": "deriveFromStore",
		},
		"status": map[string]interface{}{
			"reachable":          true,
			"observedGeneration": 7,
			"conditions":         []interface{}{},
		},
		"turbopufferUrl": "https://turbopuffer.com/organizations/org_123",
	}
}

func warehousePayload() map[string]interface{} {
	return map[string]interface{}{
		"name": "prod-snowflake",
		"kind": "snowflake",
		"snowflake": map[string]interface{}{
			"account":   "acme-xy12345",
			"user":      "SVC_LAYER",
			"role":      "SVC_LAYER_ROLE",
			"warehouse": "EXTRACT_WH",
			"keyPairSecretRef": map[string]interface{}{
				"name": "prod-snowflake-credential",
			},
		},
		"verifyInterval": "1h",
		"status": map[string]interface{}{
			"phase":              "Verified",
			"verifiedAt":         "2026-06-10T00:00:00Z",
			"observedGeneration": 4,
			"consumers": map[string]interface{}{
				"pipelines": 2,
				"apiKeys":   1,
			},
			"conditions": []interface{}{},
		},
	}
}

func TestEmitPipelineDetailIncludesQueue(t *testing.T) {
	var buf bytes.Buffer
	detail := pipelineRow{
		Pipeline: hevlayer.Pipeline{ID: "images", TargetNamespace: "products", DistanceMetric: "cosine_distance", CreatedAt: "2026-06-07T18:21:04Z"},
		Queue:    &hevlayer.PipelineStatus{PendingCount: 5, ProcessingCount: 2, FailedCount: 1, IndexedRatePerMin: 12.5},
	}
	if err := emitPipelineDetail(&buf, output.Table, detail); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if !strings.Contains(out, "QUEUE_PENDING") || !strings.Contains(out, "QUEUE_FAILED") {
		t.Fatalf("queue fields missing: %s", out)
	}

	buf.Reset()
	noQueue := pipelineRow{Pipeline: hevlayer.Pipeline{ID: "images"}}
	if err := emitPipelineDetail(&buf, output.Table, noQueue); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "No gateway queue registered yet") {
		t.Fatalf("expected no-queue hint: %s", buf.String())
	}
}
