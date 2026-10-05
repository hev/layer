package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func runRetrieval(t *testing.T, ctx context.Context, server string, args []string, input string) (int, string, string) {
	t.Helper()
	var out, err bytes.Buffer
	code := Execute(ctx, args, Options{Stdin: strings.NewReader(input), Stdout: &out, Stderr: &err, HomeDir: t.TempDir(), Env: map[string]string{"LAYER_BASE_URL": server, "LAYER_API_KEY": "test-private-key"}})
	return code, out.String(), err.String()
}
func TestRetrievalWireContract(t *testing.T) {
	body := `{"rank_by":["text","HybridText","aspirin"],"top_k":3,"include_attributes":true,"filters":["year","Gte",9007199254740993],"cursor":"opaque"}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || r.URL.Path != "/v2/namespaces/rx/query" || r.Header.Get("Authorization") != "Bearer test-private-key" {
			t.Errorf("request contract violated: %s %s", r.Method, r.URL.Path)
		}
		var raw bytes.Buffer
		_, _ = raw.ReadFrom(r.Body)
		if raw.String() != body {
			t.Errorf("request changed: %s", raw.String())
		}
		w.Header().Set("x-layer-next-cursor", "next")
		w.Header().Set("x-layer-warning", "bm25_only")
		_, _ = w.Write([]byte(`{"rows":[{"id":9007199254740993}],"hybrid":{"legs":1},"next_cursor":"next","future":{"a":true}}`))
	}))
	defer server.Close()
	code, out, err := runRetrieval(t, context.Background(), server.URL, []string{"query", "rx", "-f", "-"}, body)
	if code != 0 || err != "" {
		t.Fatalf("%d %s", code, err)
	}
	var receipt retrievalReceipt
	if e := json.Unmarshal([]byte(out), &receipt); e != nil {
		t.Fatal(e)
	}
	if receipt.Contract != "layer.retrieval.v1" || receipt.Headers["x-layer-next-cursor"] != "next" || !bytes.Contains(receipt.Data, []byte(`9007199254740993`)) || !bytes.Contains(receipt.Data, []byte(`"legs":1`)) || !bytes.Contains(receipt.Data, []byte(`"future"`)) {
		t.Fatal(out)
	}
}
func TestRetrievalEndpoints(t *testing.T) {
	cases := []struct {
		args                []string
		method, path, input string
	}{
		{[]string{"query-all", "-f", "-"}, "POST", "/v2/query", `{ "namespaces":["rx"],"rank_by":["text","BM25","a"]}`},
		{[]string{"explain", "rx", "-f", "-"}, "POST", "/v2/namespaces/rx/explain_query", `{ "rank_by":["id","asc"]}`},
		{[]string{"read", "rx", "-f", "-"}, "POST", "/v2/namespaces/rx/documents", `{"ids":[42,"a"],"include_attributes":["text"]}`},
		{[]string{"search", "rx", "-f", "-"}, "POST", "/v2/namespaces/rx/search", `{"query":"a","rerank":false}`},
		{[]string{"scan", "create", "rx", "-f", "-"}, "POST", "/v2/namespaces/rx/scans", `{"mode":"ids","source":"origin","exhaustive":true}`},
		{[]string{"scan", "results", "rx", "s1", "--limit", "20", "--offset", "40"}, "GET", "/v2/namespaces/rx/scans/s1/results?limit=20&offset=40", ""},
		{[]string{"scan", "get", "rx", "s1"}, "GET", "/v2/namespaces/rx/scans/s1", ""},
		{[]string{"scan", "delete", "rx", "s1"}, "DELETE", "/v2/namespaces/rx/scans/s1", ""},
		{[]string{"capabilities", "store", "--store"}, "GET", "/v2/vectorstores/store/capabilities", ""},
	}
	for _, tc := range cases {
		t.Run(strings.Join(tc.args, " "), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != tc.method || r.URL.RequestURI() != tc.path {
					t.Errorf("%s %s", r.Method, r.URL.RequestURI())
				}
				_, _ = w.Write([]byte(`{"ok":true}`))
			}))
			defer server.Close()
			code, out, err := runRetrieval(t, context.Background(), server.URL, tc.args, tc.input)
			if code != 0 || !strings.Contains(out, `"ok":true`) {
				t.Fatalf("%d %s %s", code, out, err)
			}
		})
	}
}
func TestCapabilitiesIncludesSchemaWithoutInventingLegs(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v2/namespaces/rx/capabilities":
			_, _ = w.Write([]byte(`{"features":[{"name":"fuzzy","supported":true}],"store":{"kind":"turbopuffer"}}`))
		case "/v2/namespaces/rx/metadata":
			_, _ = w.Write([]byte(`{"schema":{"text":{"full_text_search":true,"fuzzy":false}},"id":"rx"}`))
		default:
			t.Errorf("unexpected %s", r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	defer server.Close()
	code, out, err := runRetrieval(t, context.Background(), server.URL, []string{"capabilities", "rx"}, "")
	if code != 0 || !strings.Contains(out, `"fuzzy":false`) || strings.Contains(out, `"legs"`) {
		t.Fatalf("%d %s %s", code, out, err)
	}
}
func TestRetrievalFailuresAndCancellation(t *testing.T) {
	for _, tc := range []struct {
		status int
		body   string
	}{{422, `{"error":"UnsupportedByStore","message":"test-private-key secret query"}`}, {200, `not json`}, {302, `redirect`}} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(tc.status)
			_, _ = w.Write([]byte(tc.body))
		}))
		code, out, err := runRetrieval(t, context.Background(), server.URL, []string{"query", "rx", "-f", "-"}, `{"rank_by":["id","asc"]}`)
		server.Close()
		if code != 1 || out != "" || strings.Contains(err, "test-private-key") || strings.Contains(err, "secret query") {
			t.Fatalf("%d %s %s", code, out, err)
		}
		if tc.status == 422 && !strings.Contains(err, "UnsupportedByStore") {
			t.Fatal(err)
		}
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(100 * time.Millisecond):
		}
	}))
	defer server.Close()
	code, out, err := runRetrieval(t, context.Background(), server.URL, []string{"query", "rx", "-f", "-", "--timeout", "10ms"}, `{}`)
	if code != 1 || out != "" || !strings.Contains(err, "deadline") {
		t.Fatalf("%d %s %s", code, out, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	started := time.Now()
	code, _, _ = runRetrieval(t, ctx, server.URL, []string{"read", "rx", "-f", "-"}, `{"ids":["x"]}`)
	if code != 1 || time.Since(started) > time.Second {
		t.Fatal("cancellation not respected")
	}
}
func TestDenseSpaceContract(t *testing.T) {
	identity := embeddingIdentity{Model: "test-model", Revision: "revision-1", Dims: 2, Normalization: "l2", DistanceMetric: "cosine_distance"}
	receipt := map[string]any{"query": identity, "document": identity, "document_vectors_verified": true, "document_manifest_sha256": strings.Repeat("a", 64)}
	file := filepath.Join(t.TempDir(), "space.json")
	write := func() {
		raw, _ := json.Marshal(receipt)
		if err := os.WriteFile(file, raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	write()
	if err := validateEmbeddingSpace([]byte(`{"rank_by":["text","ANN",["Embed","fixture"]]}`), ""); err != nil {
		t.Fatal(err)
	}
	if err := validateEmbeddingSpace([]byte(`{"vector":[0.1,0.2]}`), file); err != nil {
		t.Fatal(err)
	}
	for _, body := range []string{`{"vector":[0.1]}`, `{"rank_by":["vector","ANN",[null,0.2]]}`, `{"queries":[{"vector":[1]}]}`, `{"ann":{"vector":[1],"radius":0.1}}`} {
		if validateEmbeddingSpace([]byte(body), file) == nil {
			t.Fatal("accepted incompatible vector", body)
		}
	}
	if validateEmbeddingSpace([]byte(`{"vector":[0.1,0.2]}`), "") == nil {
		t.Fatal("accepted unverified space")
	}
	identity.Revision = "different"
	receipt["document"] = identity
	write()
	if validateEmbeddingSpace([]byte(`{"vector":[0.1,0.2]}`), file) == nil {
		t.Fatal("accepted different model revision")
	}
}
