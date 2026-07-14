package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/hev/layer/apps/layer-cli/internal/config"
)

func TestIndexListNamesPaginates(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requireAuth(t, r)
		if r.Method != http.MethodGet || r.URL.Path != "/v2/namespaces" {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
		if r.URL.Query().Get("prefix") != "shop-" {
			t.Fatalf("missing prefix: %s", r.URL.RawQuery)
		}
		calls++
		w.Header().Set("Content-Type", "application/json")
		if calls == 1 {
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"namespaces":  []map[string]interface{}{{"name": "shop-products", "row_count": 10}},
				"next_cursor": "page-2",
			})
			return
		}
		if r.URL.Query().Get("cursor") != "page-2" {
			t.Fatalf("missing cursor: %s", r.URL.RawQuery)
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"namespaces":  []map[string]interface{}{{"name": "shop-reviews", "row_count": 5}},
			"next_cursor": "",
		})
	}))
	defer server.Close()

	stdout, stderr, code := runTestCLI(t, server.URL, []string{"index", "list", "--prefix", "shop-", "-o", "names"})
	if code != ExitOK {
		t.Fatalf("code=%d stderr=%s", code, stderr)
	}
	if stdout != "shop-products\nshop-reviews\n" {
		t.Fatalf("unexpected output: %q", stdout)
	}
}

func TestIndexDeleteYesAfterName(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requireAuth(t, r)
		if r.Method != http.MethodDelete || r.URL.Path != "/v2/namespaces/shop-products" {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"status": "OK", "message": "deleted"})
	}))
	defer server.Close()

	stdout, stderr, code := runTestCLI(t, server.URL, []string{"index", "delete", "shop-products", "--yes"})
	if code != ExitOK {
		t.Fatalf("code=%d stderr=%s", code, stderr)
	}
	if !strings.Contains(stdout, "shop-products\tdeleted") {
		t.Fatalf("unexpected output: %q", stdout)
	}
}

func TestIndexDeleteNonTTYRequiresYes(t *testing.T) {
	called := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	}))
	defer server.Close()

	_, stderr, code := runTestCLI(t, server.URL, []string{"index", "delete", "shop-products"})
	if code != ExitUsage {
		t.Fatalf("code=%d stderr=%s", code, stderr)
	}
	if called {
		t.Fatal("delete request was sent without --yes on non-TTY stdin")
	}
	if !strings.Contains(stderr, "requires --yes") {
		t.Fatalf("unexpected stderr: %q", stderr)
	}
}

func TestIndexPolicySetsSnapshotPolicy(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requireAuth(t, r)
		if r.Method != http.MethodPut || r.URL.Path != "/v2/namespaces/chart-notes/snapshot-policy" {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
		var body map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if got := body["facetFields"]; !reflect.DeepEqual(got, []interface{}{"age_band", "route"}) {
			t.Fatalf("unexpected facetFields: %#v", got)
		}
		if body["interval"] != "2m" || body["retention"] != "30d" {
			t.Fatalf("unexpected policy body: %#v", body)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(body)
	}))
	defer server.Close()

	stdout, stderr, code := runTestCLI(t, server.URL, []string{
		"index", "policy", "chart-notes",
		"--facet-field", "age_band",
		"--facet-fields", "route",
		"--interval", "2m",
		"--retention", "30d",
		"-o", "json",
	})
	if code != ExitOK {
		t.Fatalf("code=%d stderr=%s", code, stderr)
	}
	if !strings.Contains(stdout, `"facetFields":`) || !strings.Contains(stdout, `"interval": "2m"`) {
		t.Fatalf("unexpected stdout: %s", stdout)
	}
}

func TestIndexSnapshotCreatesAndPollsJob(t *testing.T) {
	polled := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requireAuth(t, r)
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v2/namespaces/chart-notes/snapshots":
			var body map[string]interface{}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			if body["field"] != "age_band" || body["source"] != "origin" {
				t.Fatalf("unexpected snapshot body: %#v", body)
			}
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"id": "snap-1", "namespace": "chart-notes", "field": "age_band", "source": "origin",
				"status": "running", "progress": 0, "documents_scanned": 0, "created_at": "2026-07-08T00:00:00Z",
			})
		case r.Method == http.MethodGet && r.URL.Path == "/v2/namespaces/chart-notes/snapshot-jobs/snap-1":
			polled = true
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"id": "snap-1", "namespace": "chart-notes", "field": "age_band", "source": "origin",
				"status": "completed", "progress": 1, "documents_scanned": 42, "sha": "abc1234",
				"created_at": "2026-07-08T00:00:00Z", "completed_at": "2026-07-08T00:00:01Z",
			})
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	stdout, stderr, code := runTestCLI(t, server.URL, []string{
		"index", "snapshot", "chart-notes", "--field", "age_band", "--poll-interval", "1ms", "-o", "names",
	})
	if code != ExitOK {
		t.Fatalf("code=%d stderr=%s", code, stderr)
	}
	if stdout != "snap-1\n" || !polled {
		t.Fatalf("stdout=%q polled=%t", stdout, polled)
	}
}

func TestEnvAddListShowAndRemove(t *testing.T) {
	opts := defaultTestOptions(t, "http://unused.invalid")
	stdout, stderr, code := runCLIWithOptions(t, opts, []string{
		"env", "add", "partner",
		"--base-url", "https://partner.example",
		"--api-key", "secret-token",
		"--kube-context", "partner-cluster",
		"--kube-namespace", "hevlayer",
	})
	if code != ExitOK {
		t.Fatalf("code=%d stdout=%s stderr=%s", code, stdout, stderr)
	}

	dirInfo, err := os.Stat(filepath.Join(opts.HomeDir, ".hevlayer"))
	if err != nil {
		t.Fatal(err)
	}
	if dirInfo.Mode().Perm() != 0o700 {
		t.Fatalf("config dir mode=%#o", dirInfo.Mode().Perm())
	}
	fileInfo, err := os.Stat(filepath.Join(opts.HomeDir, ".hevlayer", "config.toml"))
	if err != nil {
		t.Fatal(err)
	}
	if fileInfo.Mode().Perm() != 0o600 {
		t.Fatalf("config file mode=%#o", fileInfo.Mode().Perm())
	}

	stdout, stderr, code = runCLIWithOptions(t, opts, []string{"env", "ls", "-o", "json"})
	if code != ExitOK {
		t.Fatalf("code=%d stderr=%s", code, stderr)
	}
	if strings.Contains(stdout, "secret-token") || !strings.Contains(stdout, "secret-t...") {
		t.Fatalf("api key was not masked in env ls: %s", stdout)
	}

	stdout, stderr, code = runCLIWithOptions(t, opts, []string{"env", "show", "partner"})
	if code != ExitOK {
		t.Fatalf("code=%d stderr=%s", code, stderr)
	}
	if !strings.Contains(stdout, "partner-cluster") || !strings.Contains(stdout, "hevlayer") {
		t.Fatalf("missing kube fields: %q", stdout)
	}

	stdout, stderr, code = runCLIWithOptions(t, opts, []string{"env", "rm", "partner"})
	if code != ExitOK {
		t.Fatalf("code=%d stdout=%s stderr=%s", code, stdout, stderr)
	}
	entries, err := config.ListEnvs(opts.HomeDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("expected no envs, got %#v", entries)
	}
}

func TestPipelineListViaGatewayAPI(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requireAuth(t, r)
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v2/pipelines":
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"pipelines": []map[string]interface{}{
					{"id": "products", "target_namespace": "shop-products", "distance_metric": "cosine_distance", "created_at": "2026-06-07T00:00:00Z"},
				},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/v2/pipelines/products/status":
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"pipeline_id": "products", "pending_count": 12, "processing_count": 3, "failed_count": 0, "indexed_rate_per_min": 450.0,
			})
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	stdout, stderr, code := runTestCLI(t, server.URL, []string{"pipeline", "list", "-o", "json"})
	if code != ExitOK {
		t.Fatalf("code=%d stderr=%s", code, stderr)
	}
	if !strings.Contains(stdout, `"id": "products"`) || !strings.Contains(stdout, `"pending_count": 12`) {
		t.Fatalf("unexpected stdout: %s", stdout)
	}
}

func TestInitNamespaceStartsAndWatches(t *testing.T) {
	metadataCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requireAuth(t, r)
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v2/namespaces/shop-products/init":
			var body map[string]interface{}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			if body["schema_version"] != float64(1) || body["shard_count"] != float64(8) {
				t.Fatalf("unexpected init body: %#v", body)
			}
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"namespace": "shop-products",
				"layer": map[string]interface{}{
					"schema_version": 1, "init_state": "running", "init_lag_rows": 2,
					"shard_count": 8, "shard_state": "backfilling", "shard_lag_rows": 2,
					"scatter_gather_active": false,
				},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/v2/namespaces/shop-products/metadata":
			metadataCalls++
			lag := 1
			active := false
			state := "backfilling"
			if metadataCalls > 1 {
				lag = 0
				active = true
				state = "ready"
			}
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"name": "shop-products",
				"layer": map[string]interface{}{
					"schema_version": 1, "init_state": "ready", "init_lag_rows": lag,
					"shard_count": 8, "shard_state": state, "shard_lag_rows": lag,
					"scatter_gather_active": active,
				},
			})
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	stdout, stderr, code := runTestCLI(t, server.URL, []string{"init", "shop-products", "--shards", "8", "--poll-interval", "1ms"})
	if code != ExitOK {
		t.Fatalf("code=%d stderr=%s", code, stderr)
	}
	if metadataCalls != 2 {
		t.Fatalf("metadata calls=%d", metadataCalls)
	}
	if !strings.Contains(stderr, "shard_lag_rows=1") || !strings.Contains(stderr, "scatter/gather active") {
		t.Fatalf("unexpected stderr: %q", stderr)
	}
	if !strings.Contains(stdout, "SCATTER_GATHER_ACTIVE  true") {
		t.Fatalf("unexpected stdout: %q", stdout)
	}
}

func TestInitNamespaceConflictMessage(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requireAuth(t, r)
		if r.Method != http.MethodPost || r.URL.Path != "/v2/namespaces/shop-products/init" {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"error":   "conflict",
			"message": "namespace 'shop-products' already initialized with shard_count=4; requested shard_count=8",
		})
	}))
	defer server.Close()

	_, stderr, code := runTestCLI(t, server.URL, []string{"init", "shop-products", "--shards", "8", "--watch=false"})
	if code != ExitFailed {
		t.Fatalf("code=%d stderr=%s", code, stderr)
	}
	if !strings.Contains(stderr, "already initialized with a different shard count") {
		t.Fatalf("unexpected stderr: %q", stderr)
	}
}

func TestUdfListFansOutStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requireAuth(t, r)
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v2/udfs":
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"udfs": []map[string]interface{}{udfPayload()},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/v2/udfs/product-tags/status":
			_ = json.NewEncoder(w).Encode(udfStatus(2))
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	stdout, stderr, code := runTestCLI(t, server.URL, []string{"udf", "ls", "-o", "json"})
	if code != ExitOK {
		t.Fatalf("code=%d stderr=%s", code, stderr)
	}
	if !strings.Contains(stdout, `"pending_count": 0`) || !strings.Contains(stdout, `"sweeps_completed": 2`) {
		t.Fatalf("unexpected stdout: %s", stdout)
	}
}

func TestUdfStatusWatchReturnsDrainedStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requireAuth(t, r)
		if r.Method != http.MethodGet || r.URL.Path != "/v2/udfs/product-tags/status" {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(udfStatus(0))
	}))
	defer server.Close()

	stdout, stderr, code := runTestCLI(t, server.URL, []string{"udf", "status", "product-tags", "--watch", "-o", "names"})
	if code != ExitOK {
		t.Fatalf("code=%d stderr=%s", code, stderr)
	}
	if stdout != "product-tags\n" {
		t.Fatalf("unexpected stdout: %q", stdout)
	}
	if !strings.Contains(stderr, "pending=0 processing=0 failed=0") {
		t.Fatalf("watch line missing: %q", stderr)
	}
}

func TestAskCommandDelegatesWithLayerDigest(t *testing.T) {
	opts := defaultTestOptions(t, "http://unused.invalid")
	opts.AskDigestPath = "/tmp/layer-digest.json"
	var got []string
	opts.RunAsk = func(ctx context.Context, args []string, stdin io.Reader, stdout io.Writer, stderr io.Writer) error {
		got = append([]string(nil), args...)
		_, _ = stdout.Write([]byte("asked\n"))
		return nil
	}

	stdout, stderr, code := runCLIWithOptions(t, opts, []string{"ask", "search", "warm cache"})
	if code != ExitOK {
		t.Fatalf("code=%d stderr=%s", code, stderr)
	}
	if stdout != "asked\n" {
		t.Fatalf("unexpected stdout: %q", stdout)
	}
	want := []string{"--digest-path", "/tmp/layer-digest.json", "search", "warm cache"}
	if strings.Join(got, "\x00") != strings.Join(want, "\x00") {
		t.Fatalf("args=%#v want %#v", got, want)
	}
}

func TestAskCommandAddsProFooterForProSurface(t *testing.T) {
	opts := defaultTestOptions(t, "http://unused.invalid")
	opts.RunAsk = func(ctx context.Context, args []string, stdin io.Reader, stdout io.Writer, stderr io.Writer) error {
		_, _ = stdout.Write([]byte("Use the Function CRD to run a classifier.\n"))
		return nil
	}

	stdout, stderr, code := runCLIWithOptions(t, opts, []string{"ask", "grep", "classifier"})
	if code != ExitOK || stderr != "" {
		t.Fatalf("code=%d stderr=%q", code, stderr)
	}
	if stdout != "Use the Function CRD to run a classifier.\n\n"+askProFooter+"\n" {
		t.Fatalf("unexpected stdout: %q", stdout)
	}
}

func TestAskCommandLeavesOpenSurfaceAndJSONUnchanged(t *testing.T) {
	for _, args := range [][]string{{"ask", "grep", "hybrid"}, {"ask", "-o", "json", "grep", "function"}} {
		opts := defaultTestOptions(t, "http://unused.invalid")
		opts.RunAsk = func(ctx context.Context, askArgs []string, stdin io.Reader, stdout io.Writer, stderr io.Writer) error {
			if args[1] == "-o" {
				_, _ = stdout.Write([]byte(`{"summary":"Function CRD"}`))
			} else {
				_, _ = stdout.Write([]byte("Hybrid text fusion stays open.\n"))
			}
			return nil
		}
		stdout, stderr, code := runCLIWithOptions(t, opts, args)
		if code != ExitOK || stderr != "" || strings.Contains(stdout, askProFooter) {
			t.Fatalf("args=%v code=%d stdout=%q stderr=%q", args, code, stdout, stderr)
		}
	}
}

func TestLicenseRequiredBecomesGatedCommandNudge(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusPaymentRequired)
		_, _ = w.Write([]byte(`{"error":"license_required","feature":"transform-runtime"}`))
	}))
	defer server.Close()

	stdout, stderr, code := runTestCLI(t, server.URL, []string{"udf", "list"})
	if code != ExitOK || stdout != "" || strings.TrimSpace(stderr) != gatedCommandNudge {
		t.Fatalf("code=%d stdout=%q stderr=%q", code, stdout, stderr)
	}
}

func TestAskCommandMapsLayerJSONOutput(t *testing.T) {
	opts := defaultTestOptions(t, "http://unused.invalid")
	opts.AskDigestPath = "/tmp/layer-digest.json"
	var got []string
	opts.RunAsk = func(ctx context.Context, args []string, stdin io.Reader, stdout io.Writer, stderr io.Writer) error {
		got = append([]string(nil), args...)
		return nil
	}

	_, stderr, code := runCLIWithOptions(t, opts, []string{"ask", "-o", "json", "tree"})
	if code != ExitOK {
		t.Fatalf("code=%d stderr=%s", code, stderr)
	}
	want := []string{"--digest-path", "/tmp/layer-digest.json", "--json", "tree"}
	if strings.Join(got, "\x00") != strings.Join(want, "\x00") {
		t.Fatalf("args=%#v want %#v", got, want)
	}
}

func TestAskCommandDoesNotOverrideExplicitEndpoint(t *testing.T) {
	opts := defaultTestOptions(t, "http://unused.invalid")
	opts.AskDigestPath = "/tmp/layer-digest.json"
	var got []string
	opts.RunAsk = func(ctx context.Context, args []string, stdin io.Reader, stdout io.Writer, stderr io.Writer) error {
		got = append([]string(nil), args...)
		return nil
	}

	_, stderr, code := runCLIWithOptions(t, opts, []string{"ask", "--endpoint", "https://docs.example/api/ask", "tree"})
	if code != ExitOK {
		t.Fatalf("code=%d stderr=%s", code, stderr)
	}
	want := []string{"--endpoint", "https://docs.example/api/ask", "tree"}
	if strings.Join(got, "\x00") != strings.Join(want, "\x00") {
		t.Fatalf("args=%#v want %#v", got, want)
	}
}

func TestRunNoApplyDetachRegistersAndDiscovers(t *testing.T) {
	var sawCreate bool
	var sawDiscover bool
	var statusCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requireAuth(t, r)
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v2/udfs/product-tags/status":
			statusCalls++
			if statusCalls == 1 {
				http.Error(w, `{"error":"not_found","message":"missing"}`, http.StatusNotFound)
				return
			}
			_ = json.NewEncoder(w).Encode(udfStatus(0))
		case r.Method == http.MethodPost && r.URL.Path == "/v2/udfs":
			sawCreate = true
			var body map[string]interface{}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			spec := body["spec"].(map[string]interface{})
			if _, ok := spec["scaling"]; ok {
				t.Fatalf("gateway spec leaked scaling: %#v", spec)
			}
			if _, ok := spec["index_selector"]; ok {
				t.Fatalf("gateway spec leaked index_selector: %#v", spec)
			}
			if _, ok := spec["output"]; ok {
				t.Fatalf("gateway spec leaked removed output: %#v", spec)
			}
			if got := spec["version"]; got != "v1" {
				t.Fatalf("gateway spec version mismatch: %#v", spec)
			}
			if got := spec["target_namespaces"].([]interface{})[0]; got != "shop-products" {
				t.Fatalf("target namespace override failed: %#v", spec["target_namespaces"])
			}
			worker := spec["worker"].(map[string]interface{})
			if _, ok := worker["pod_spec"]; ok {
				t.Fatalf("gateway spec leaked worker pod_spec: %#v", worker)
			}
			_ = json.NewEncoder(w).Encode(udfPayload())
		case r.Method == http.MethodPost && r.URL.Path == "/v2/udfs/product-tags/discover":
			sawDiscover = true
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"udf_id": "product-tags", "enqueued": 0, "namespaces": []string{"shop-products"}})
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	manifest := filepath.Join(t.TempDir(), "function.yaml")
	err := os.WriteFile(manifest, []byte(`
apiVersion: hevlayer.com/v1alpha1
kind: Function
metadata:
  name: product-tags
spec:
  paused: false
  indexSelector:
    matchLabels:
      app: shop
  targetNamespaces:
    - original-index
  inputs: [id, title]
  version: v1
  filter: ["category", "Eq", "outdoor"]
  worker:
    image: 186219257916.dkr.ecr.us-east-1.amazonaws.com/hevlayer-test:latest
    batchSize: 16
    timeoutSeconds: 30
    podSpec:
      nodeSelector:
        pool: udf
  schedule:
    discoveryIntervalSeconds: 300
    leaseSeconds: 120
    maxInFlightBatches: 4
    maxConcurrentScans: 1
  retry:
    maxAttempts: 6
    initialBackoffSeconds: 5
    maxBackoffSeconds: 300
  triggers: [discovery]
  scaling:
    pool: udf
    replicas:
      min: 0
      max: 1
`), 0o600)
	if err != nil {
		t.Fatal(err)
	}

	stdout, stderr, code := runTestCLI(t, server.URL, []string{"run", "-f", manifest, "--index", "shop-products", "--no-apply", "--detach"})
	if code != ExitOK {
		t.Fatalf("code=%d stderr=%s", code, stderr)
	}
	if !sawCreate || !sawDiscover {
		t.Fatalf("create=%t discover=%t", sawCreate, sawDiscover)
	}
	if !strings.Contains(stdout, "layer udf get product-tags --watch") {
		t.Fatalf("unexpected stdout: %q", stdout)
	}
}

func TestHelpExitsZero(t *testing.T) {
	for _, args := range [][]string{{"-h"}, {"--help"}, {"help"}} {
		stdout, stderr, code := runTestCLI(t, "http://unused.invalid", args)
		if code != ExitOK {
			t.Fatalf("args=%v code=%d stderr=%s", args, code, stderr)
		}
		if !strings.Contains(stdout, "layer [global flags] <command>") {
			t.Fatalf("args=%v unexpected output: %q", args, stdout)
		}
	}
}

func TestNoArgsPrintsUsageToStderr(t *testing.T) {
	stdout, stderr, code := runTestCLI(t, "http://unused.invalid", nil)
	if code != ExitUsage {
		t.Fatalf("code=%d", code)
	}
	if stdout != "" {
		t.Fatalf("unexpected stdout: %q", stdout)
	}
	if !strings.Contains(stderr, "layer [global flags] <command>") {
		t.Fatalf("unexpected stderr: %q", stderr)
	}
}

func TestBareLayerOnTTYLaunchesTUI(t *testing.T) {
	called := false
	opts := defaultTestOptions(t, "http://unused.invalid")
	opts.StdinIsTerminal = true
	opts.StdoutIsTerminal = true
	opts.LaunchTUI = func(ctx context.Context, options TUIOptions) error {
		called = true
		return nil
	}
	stdout, stderr, code := runCLIWithOptions(t, opts, nil)
	if code != ExitOK {
		t.Fatalf("code=%d stdout=%s stderr=%s", code, stdout, stderr)
	}
	if !called {
		t.Fatal("TUI launcher was not called")
	}
}

func TestBrowseAliasLaunchesTUIOnTTY(t *testing.T) {
	called := false
	opts := defaultTestOptions(t, "http://unused.invalid")
	opts.StdinIsTerminal = true
	opts.StdoutIsTerminal = true
	opts.LaunchTUI = func(ctx context.Context, options TUIOptions) error {
		called = true
		return nil
	}
	stdout, stderr, code := runCLIWithOptions(t, opts, []string{"browse"})
	if code != ExitOK {
		t.Fatalf("code=%d stdout=%s stderr=%s", code, stdout, stderr)
	}
	if !called {
		t.Fatal("TUI launcher was not called")
	}
}

func TestVersionCommand(t *testing.T) {
	for _, args := range [][]string{{"version"}, {"--version"}} {
		stdout, stderr, code := runTestCLI(t, "http://unused.invalid", args)
		if code != ExitOK {
			t.Fatalf("args=%v code=%d stderr=%s", args, code, stderr)
		}
		if !strings.HasPrefix(stdout, "layer test") {
			t.Fatalf("args=%v unexpected output: %q", args, stdout)
		}
	}
}

func runTestCLI(t *testing.T, baseURL string, args []string) (string, string, int) {
	t.Helper()
	return runCLIWithOptions(t, defaultTestOptions(t, baseURL), args)
}

func runCLIWithOptions(t *testing.T, opts Options, args []string) (string, string, int) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	opts.Stdout = &stdout
	opts.Stderr = &stderr
	if opts.Stdin == nil {
		opts.Stdin = strings.NewReader("yes\n")
	}
	code := Execute(t.Context(), args, opts)
	return stdout.String(), stderr.String(), code
}

func defaultTestOptions(t *testing.T, baseURL string) Options {
	t.Helper()
	return Options{
		Stdin: strings.NewReader("yes\n"),
		Env: map[string]string{
			"LAYER_BASE_URL": baseURL,
			"LAYER_API_KEY":  "test-token",
		},
		HomeDir:         t.TempDir(),
		StdinIsTerminal: false,
		Version:         "test",
	}
}

func requireAuth(t *testing.T, r *http.Request) {
	t.Helper()
	if r.Header.Get("Authorization") != "Bearer test-token" {
		t.Fatalf("missing authorization header: %q", r.Header.Get("Authorization"))
	}
}

func udfPayload() map[string]interface{} {
	return map[string]interface{}{
		"id":         "product-tags",
		"spec":       map[string]interface{}{"target_namespaces": []string{"shop-products"}, "version": "v1", "worker": map[string]interface{}{}},
		"paused":     false,
		"created_at": "2026-06-07T18:21:04Z",
		"updated_at": "2026-06-07T18:21:04Z",
	}
}

func udfStatus(sweeps int) map[string]interface{} {
	return map[string]interface{}{
		"udf_id":               "product-tags",
		"paused":               false,
		"active_namespaces":    []string{"shop-products"},
		"discovery":            map[string]interface{}{"sweeps_completed": sweeps, "last_completed_at": nil},
		"counts":               map[string]int{"pending": 0},
		"pending_count":        0,
		"processing_count":     0,
		"failed_count":         0,
		"indexed_rate_per_min": 0,
		"rate_window_seconds":  60,
	}
}
