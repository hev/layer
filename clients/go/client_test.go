package hevlayer

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestClientCoreOperations(t *testing.T) {
	ctx := context.Background()
	seen := map[string]bool{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-token" {
			t.Fatalf("missing bearer token: %q", r.Header.Get("Authorization"))
		}
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v2/namespaces":
			seen["list"] = true
			if r.URL.Query().Get("prefix") != "shop-" {
				t.Fatalf("missing prefix query: %s", r.URL.RawQuery)
			}
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"namespaces": []map[string]interface{}{{"name": "shop-products", "row_count": 10}},
				"next_cursor": nil,
			})
		case r.Method == http.MethodPost && r.URL.Path == "/v2/namespaces/ns/query":
			seen["queryNamespace"] = true
			if r.Header.Get("x-hevlayer-search-query") != "boots" {
				t.Fatalf("missing search query header: %q", r.Header.Get("x-hevlayer-search-query"))
			}
			if r.Header.Get("x-hevlayer-tags") != "app:shop,surface:store" {
				t.Fatalf("unexpected tags header: %q", r.Header.Get("x-hevlayer-tags"))
			}
			w.Header().Set("x-layer-stable-as-of", "42")
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"rows": []map[string]interface{}{{"id": "doc-1", "$dist": 0.1, "title": "Boot"}},
			})
		case r.Method == http.MethodPost && r.URL.Path == "/v2/namespaces/ns":
			var body map[string]interface{}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			if patchColumns, ok := body["patch_columns"].(map[string]interface{}); ok {
				seen["patchColumns"] = true
				ids, _ := patchColumns["id"].([]interface{})
				tags, _ := patchColumns["tags"].([]interface{})
				if len(ids) != 2 || len(tags) != 2 {
					t.Fatalf("unexpected patch columns body: %#v", patchColumns)
				}
			} else {
				seen["writeNamespace"] = true
				if body["upsert_rows"] == nil {
					t.Fatalf("write namespace body missing upsert_rows: %#v", body)
				}
			}
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"status": "OK", "message": "written", "rows_affected": 1, "billing": map[string]interface{}{}})
		case r.Method == http.MethodDelete && r.URL.Path == "/v2/namespaces/shop-products":
			seen["deleteNamespace"] = true
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"status": "OK", "message": "deleted"})
		case r.Method == http.MethodPost && r.URL.Path == "/v2/pipelines":
			seen["createPipelineConflict"] = true
			w.WriteHeader(http.StatusConflict)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"error": "conflict", "message": "exists"})
		case r.Method == http.MethodGet && r.URL.Path == "/v2/pipelines":
			seen["listPipelines"] = true
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"pipelines": []map[string]interface{}{{"id": "p1", "target_namespace": "ns", "distance_metric": "cosine", "created_at": "2026-06-07T18:21:04Z"}},
			})
		case r.Method == http.MethodPost && r.URL.Path == "/v2/pipelines/p1/documents/stage":
			seen["completeDocuments"] = true
			var body SetDocumentsStageRequest
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			if body.Stage != "indexed" || body.FromStage != "embedding" || body.WorkerID != "w1" {
				t.Fatalf("unexpected stage body: %#v", body)
			}
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"pipeline_id": "p1", "stage": "indexed", "updated": 2})
		case r.Method == http.MethodPut && r.URL.Path == "/v2/pipelines/p1/documents/doc-1/vectors":
			var body PutVectorsRequest
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			if len(body.Vectors) != 1 {
				t.Fatalf("unexpected vector body: %#v", body)
			}
			switch body.Vectors[0].ID {
			case "doc-1:chunk-1":
				seen["writeSingleVector"] = true
			case "doc-1:multi-1":
				seen["writeSingleMultivector"] = true
				if len(body.Vectors[0].Vectors) != 2 || body.Vectors[0].Vectors[1][1] != 0.4 {
					t.Fatalf("unexpected multivector body: %#v", body)
				}
			default:
				t.Fatalf("unexpected vector id: %#v", body)
			}
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"status": "ok", "message": "vector"})
		case r.Method == http.MethodPost && r.URL.Path == "/v2/namespaces/ns/warm":
			seen["warmNamespace"] = true
			if r.URL.Query().Get("page_size") != "42" {
				t.Fatalf("missing warm page_size query: %s", r.URL.RawQuery)
			}
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"id": "warm-1", "namespace": "ns", "status": "completed", "progress": 1, "documents_scanned": 10, "created_at": "2026-06-07T18:21:04Z"})
		case r.Method == http.MethodPost && r.URL.Path == "/v2/namespaces/ns/scans":
			seen["createScan"] = true
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"id": "scan-1", "namespace": "ns", "status": "running", "progress": 0, "documents_scanned": 0, "created_at": "2026-06-07T18:21:04Z", "source": "stored"})
		case r.Method == http.MethodGet && r.URL.Path == "/v2/namespaces/ns/scans/scan-1":
			seen["getScan"] = true
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"id": "scan-1", "namespace": "ns", "status": "completed", "progress": 1, "documents_scanned": 10, "created_at": "2026-06-07T18:21:04Z", "source": "stored"})
		case r.Method == http.MethodPost && r.URL.Path == "/v2/udfs":
			seen["createUdf"] = true
			_ = json.NewEncoder(w).Encode(udfPayload())
		case r.Method == http.MethodGet && r.URL.Path == "/v2/udfs/product-tags":
			seen["getUdf"] = true
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"udf": udfPayload(), "status": udfStatusPayload(0)})
		case r.Method == http.MethodGet && r.URL.Path == "/v2/udfs/product-tags/status":
			seen["status"] = true
			_ = json.NewEncoder(w).Encode(udfStatusPayload(1))
		case r.Method == http.MethodPost && r.URL.Path == "/v2/udfs/product-tags/discover":
			seen["discover"] = true
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"udf_id": "product-tags", "enqueued": 0, "namespaces": []string{"shop-products"}})
		case r.Method == http.MethodDelete && r.URL.Path == "/v2/udfs/product-tags":
			seen["deleteUdf"] = true
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"status": "OK", "message": "deleted"})
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	client := NewClient(WithBaseURL(server.URL), WithAPIKey("test-token"))
	listed, err := client.ListNamespaces(ctx, &ListNamespacesParams{Prefix: "shop-"})
	if err != nil {
		t.Fatal(err)
	}
	if len(listed.Namespaces) != 1 || listed.Namespaces[0].Name != "shop-products" {
		t.Fatalf("unexpected namespace list: %#v", listed)
	}
	query, err := client.QueryNamespaceWithPerf(
		ctx,
		"ns",
		&QueryRequest{Vector: []float64{0.1, 0.2}, TopK: 1, IncludeAttributes: []string{"title"}},
		WithSearchQuery(" boots "),
		WithSearchTags([]string{"surface:store", "app:shop", "surface:store"}),
	)
	if err != nil {
		t.Fatal(err)
	}
	if query.Perf.CacheStatus != "" || query.Data.Rows[0]["id"] != "doc-1" || query.Data.StableAsOf != 42 {
		t.Fatalf("unexpected query response: %#v", query)
	}
	write, err := client.WriteNamespace(ctx, "ns", TurbopufferWriteRequest{"upsert_rows": []map[string]interface{}{{"id": "doc-1", "vector": []float64{0.1, 0.2}}}})
	if err != nil {
		t.Fatal(err)
	}
	if write.Status != "OK" || write.RowsAffected != 1 {
		t.Fatalf("unexpected write response: %#v", write)
	}
	if _, err := client.DeleteNamespace(ctx, "shop-products"); err != nil {
		t.Fatal(err)
	}
	pipeline, err := client.EnsurePipeline(ctx, &CreatePipelineRequest{ID: "p1", TargetNamespace: "ns"})
	if err != nil {
		t.Fatal(err)
	}
	if pipeline.ID != "p1" {
		t.Fatalf("unexpected ensured pipeline: %#v", pipeline)
	}
	completed, err := client.CompleteDocuments(ctx, "p1", []string{"doc-1", "doc-2"}, &DocumentStageOptions{FromStage: "embedding", WorkerID: "w1"})
	if err != nil {
		t.Fatal(err)
	}
	if completed.Updated != 2 {
		t.Fatalf("unexpected complete response: %#v", completed)
	}
	if _, err := client.WriteSingleVector(ctx, "p1", "doc-1", VectorEntry{ID: "doc-1:chunk-1", Vector: []float64{0.3, 0.4}, Attributes: map[string]interface{}{"kind": "review"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.WriteSingleMultivector(ctx, "p1", "doc-1", "doc-1:multi-1", [][]float64{{0.1, 0.2}, {0.3, 0.4}}, map[string]interface{}{"kind": "late"}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.PatchColumns(ctx, "ns", []string{"doc-1", "doc-2"}, map[string][]interface{}{"tags": []interface{}{[]interface{}{"durable"}, []interface{}{"soft"}}, "tags_v": []interface{}{"v1", "v1"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.WarmNamespace(ctx, "ns", &WarmCacheParams{PageSize: 42}); err != nil {
		t.Fatal(err)
	}
	scan, err := client.Scan(ctx, "ns", &CreateScanRequest{}, &ScanWaitOptions{InitialDelay: 1})
	if err != nil {
		t.Fatal(err)
	}
	if scan.ID != "scan-1" || scan.Status != "completed" {
		t.Fatalf("unexpected scan: %#v", scan)
	}
	if _, err := client.ListSearchHistory(ctx, "ns", &ListSearchHistoryParams{Tag: []string{"bad,tag"}}); err == nil {
		t.Fatal("invalid search-history tag should fail before request")
	}
	if _, err := client.CreateUdf(ctx, &CreateUdfRequest{ID: "product-tags", Spec: testUdfSpec()}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.GetUdf(ctx, "product-tags"); err != nil {
		t.Fatal(err)
	}
	status, err := client.GetUdfStatus(ctx, "product-tags")
	if err != nil {
		t.Fatal(err)
	}
	if status.Discovery.SweepsCompleted != 1 {
		t.Fatalf("expected discovery sweep count, got %#v", status.Discovery)
	}
	if _, err := client.DiscoverUdf(ctx, "product-tags", &UdfDiscoverRequest{PageSize: 100}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.DeleteUdf(ctx, "product-tags"); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"list", "queryNamespace", "writeNamespace", "deleteNamespace", "createPipelineConflict", "listPipelines", "completeDocuments", "writeSingleVector", "writeSingleMultivector", "patchColumns", "warmNamespace", "createScan", "getScan", "createUdf", "getUdf", "status", "discover", "deleteUdf"} {
		if !seen[key] {
			t.Fatalf("operation %s was not exercised", key)
		}
	}
}

func testUdfSpec() UdfSpec {
	return UdfSpec{
		TargetNamespaces: []string{"shop-products"},
		Inputs: []string{"id", "title"},
		Version: "v1",
		Worker: UdfWorkerSpec{BatchSize: 16, TimeoutSeconds: 30},
		Schedule: UdfScheduleSpec{DiscoveryIntervalSeconds: 300, LeaseSeconds: 120, MaxInFlightBatches: 4, MaxConcurrentScans: 1},
		Retry: UdfRetrySpec{MaxAttempts: 6, InitialBackoffSeconds: 5, MaxBackoffSeconds: 300},
		Triggers: []UdfTrigger{"discovery"},
	}
}

func udfPayload() map[string]interface{} {
	return map[string]interface{}{
		"id": "product-tags",
		"spec": testUdfSpec(),
		"paused": false,
		"created_at": "2026-06-07T18:21:04Z",
		"updated_at": "2026-06-07T18:21:04Z",
	}
}

func udfStatusPayload(sweeps int) map[string]interface{} {
	return map[string]interface{}{
		"udf_id": "product-tags",
		"paused": false,
		"active_namespaces": []string{"shop-products"},
		"discovery": map[string]interface{}{"sweeps_completed": sweeps, "last_completed_at": "2026-06-07T18:21:04Z"},
		"counts": map[string]int{"pending": 0},
		"pending_count": 0,
		"processing_count": 0,
		"failed_count": 0,
		"indexed_rate_per_min": 0.0,
		"rate_window_seconds": 300,
	}
}
