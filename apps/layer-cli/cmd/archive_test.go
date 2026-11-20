package cmd

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func archiveRun(kind, state string, extra map[string]interface{}) map[string]interface{} {
	run := map[string]interface{}{
		"id":        "rst-0197a5c2f1e87c0b9d3e4f5a6b7c8d9e",
		"kind":      kind,
		"namespace": "contracts",
		"store":     "luxir-main",
		"state":     state,
		"startedAt": "2026-12-02T10:00:00.000Z",
		"rows":      0,
	}
	for key, value := range extra {
		run[key] = value
	}
	return run
}

func TestArchiveRestoreWaitsForTheRunAndPrintsVerification(t *testing.T) {
	var polls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requireAuth(t, r)
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/control/archive/restores":
			body, _ := io.ReadAll(r.Body)
			var request map[string]interface{}
			if err := json.Unmarshal(body, &request); err != nil {
				t.Fatal(err)
			}
			if request["namespace"] != "contracts" || request["store"] != "luxir-main" {
				t.Fatalf("request=%s", body)
			}
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(archiveRun("restore", "running", nil))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/control/archive/namespaces/contracts/runs/rst-0197a5c2f1e87c0b9d3e4f5a6b7c8d9e":
			if polls.Add(1) < 2 {
				_ = json.NewEncoder(w).Encode(archiveRun("restore", "running", map[string]interface{}{
					"phases": []map[string]interface{}{{"name": "manifest", "seconds": 0.2}},
				}))
				return
			}
			_ = json.NewEncoder(w).Encode(archiveRun("restore", "succeeded", map[string]interface{}{
				"rows":       100000,
				"finishedAt": "2026-12-02T10:03:00.000Z",
				"phases": []map[string]interface{}{
					{"name": "manifest", "seconds": 0.2},
					{"name": "load", "seconds": 171.5},
				},
				"result": map[string]interface{}{
					"rows": 100000,
					"verification": map[string]interface{}{
						"passed": true, "engineRows": 100000, "sampleMismatches": 0, "embedRequests": 0,
					},
				},
			}))
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	stdout, stderr, code := runTestCLI(t, server.URL, []string{
		"archive", "restore", "contracts", "--store", "luxir-main", "--poll-interval", "1ms",
	})
	if code != ExitOK {
		t.Fatalf("code=%d stderr=%s", code, stderr)
	}
	for _, want := range []string{"succeeded", "PHASE load", "171.5s", "verification.passed", "verification.embedRequests"} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("missing %q in:\n%s", want, stdout)
		}
	}
	// Progress goes to stderr, each phase once.
	if strings.Count(stderr, "manifest: 0.2s") != 1 || !strings.Contains(stderr, "load: 171.5s") {
		t.Fatalf("stderr=%s", stderr)
	}
}

func TestArchiveExportFailureExitsNonZeroAndSendsFlags(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requireAuth(t, r)
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/control/archive/exports":
			body, _ := io.ReadAll(r.Body)
			var request map[string]interface{}
			if err := json.Unmarshal(body, &request); err != nil {
				t.Fatal(err)
			}
			if request["distanceMetric"] != "cosine_distance" || request["resume"] != "exp-1" {
				t.Fatalf("request=%s", body)
			}
			if _, set := request["store"]; set {
				t.Fatalf("store must be omitted when not given: %s", body)
			}
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(archiveRun("export", "running", nil))
		case r.Method == http.MethodGet:
			_ = json.NewEncoder(w).Encode(archiveRun("export", "failed", map[string]interface{}{
				"error": "engine: scan of contracts: connection refused",
			}))
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	stdout, stderr, code := runTestCLI(t, server.URL, []string{
		"archive", "export", "contracts", "--distance-metric", "cosine_distance", "--resume", "exp-1",
		"--poll-interval", "1ms", "-o", "json",
	})
	if code != ExitFailed {
		t.Fatalf("code=%d stdout=%s stderr=%s", code, stdout, stderr)
	}
	if !strings.Contains(stdout, `"state": "failed"`) || !strings.Contains(stderr, "connection refused") {
		t.Fatalf("stdout=%s stderr=%s", stdout, stderr)
	}
}

func TestArchiveNoWaitReturnsTheStartedRun(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requireAuth(t, r)
		if r.Method != http.MethodPost {
			t.Fatalf("--no-wait must not poll: %s %s", r.Method, r.URL.String())
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(archiveRun("export", "running", nil))
	}))
	defer server.Close()
	stdout, stderr, code := runTestCLI(t, server.URL, []string{"archive", "export", "contracts", "--no-wait", "-o", "names"})
	if code != ExitOK || strings.TrimSpace(stdout) != "rst-0197a5c2f1e87c0b9d3e4f5a6b7c8d9e" {
		t.Fatalf("code=%d stdout=%q stderr=%s", code, stdout, stderr)
	}
}

func TestArchiveStatusShowsTableEnginesAndRuns(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requireAuth(t, r)
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/v1/control/archive/namespaces/contracts":
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"namespace":   "contracts",
				"archived":    true,
				"manifestUrl": "s3://archive/layer/contracts/_layer.json",
				"manifest": map[string]interface{}{
					"formatVersion": 1, "incarnation": "0197a5c2", "createdAt": "2026-12-02T10:00:00.000Z",
					"mode": "snapshot", "schemaVersion": 2, "attributes": 7,
					"vectors": map[string]interface{}{"vector": map[string]interface{}{"dimensions": 1024}},
					"table": map[string]interface{}{
						"format": "iceberg", "location": "s3://archive/layer/contracts/0197a5c2/table",
						"metadataVersion": 3, "snapshotId": 8123, "compactedSeq": 0,
						"rowCount": 10400000, "logicalBytes": 63350000000,
					},
					"engines": []map[string]interface{}{{
						"store": "luxir-main", "kind": "luxir", "engineVersion": "0.1.0",
						"appliedSeq": 0, "loadedAt": "2026-12-02T11:42:07.000Z", "loadedFromSnapshot": 8123,
					}},
				},
				"runs": []map[string]interface{}{archiveRun("restore", "succeeded", map[string]interface{}{"rows": 10400000})},
			})
		case "/v1/control/archive/namespaces/empty":
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"namespace": "empty", "archived": false,
				"manifestUrl": "s3://archive/layer/empty/_layer.json", "runs": []interface{}{},
			})
		case "/v1/control/archive/namespaces/gone":
			http.Error(w, `{"error":"not_found","message":"namespace \"gone\" is not archived"}`, http.StatusNotFound)
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	stdout, stderr, code := runTestCLI(t, server.URL, []string{"archive", "status", "contracts"})
	if code != ExitOK {
		t.Fatalf("code=%d stderr=%s", code, stderr)
	}
	for _, want := range []string{
		"s3://archive/layer/contracts/0197a5c2/table", "10400000", "63350000000", "snapshot",
		"ENGINE luxir-main", "luxir 0.1.0, snapshot 8123", "rst-0197a5c2f1e87c0b9d3e4f5a6b7c8d9e",
	} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("missing %q in:\n%s", want, stdout)
		}
	}
	// A namespace with no manifest yet prints what there is and no table.
	stdout, _, code = runTestCLI(t, server.URL, []string{"archive", "status", "empty"})
	if code != ExitOK || strings.Contains(stdout, "TABLE") || !strings.Contains(stdout, "false") {
		t.Fatalf("code=%d stdout=%s", code, stdout)
	}
	_, stderr, code = runTestCLI(t, server.URL, []string{"archive", "status", "gone"})
	if code == ExitOK || !strings.Contains(stderr, "not archived") {
		t.Fatalf("code=%d stderr=%s", code, stderr)
	}
}

func TestArchiveUsage(t *testing.T) {
	for _, args := range [][]string{{"archive"}, {"archive", "export"}, {"archive", "restore", "a", "b"}} {
		_, stderr, code := runTestCLI(t, "http://127.0.0.1:1", args)
		if code != ExitUsage {
			t.Fatalf("%v: code=%d stderr=%s", args, code, stderr)
		}
	}
}
