package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"syscall"
	"testing"

	hevlayer "github.com/hev/layer/clients/go"
)

func TestIndexDeleteBatchCompletes(t *testing.T) {
	for _, format := range []string{"table", "json", "names"} {
		t.Run(format, func(t *testing.T) {
			names := []string{"batch-1", "batch-2", "batch-3", "batch-4", "batch-5"}
			var attempted []string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.Method == http.MethodGet {
					entries := []map[string]string{}
					for _, name := range names {
						entries = append(entries, map[string]string{"name": name})
					}
					json.NewEncoder(w).Encode(map[string]any{"namespaces": entries})
					return
				}
				name := strings.TrimPrefix(r.URL.Path, "/v2/namespaces/")
				attempted = append(attempted, name)
				if name == "batch-3" {
					http.Error(w, `{"message":"injected failure"}`, 500)
					return
				}
				fmt.Fprintf(w, `{"status":"OK","message":"purged %s"}`, name)
			}))
			defer server.Close()
			stdout, stderr, code := runTestCLI(t, server.URL, []string{"index", "delete", "--prefix", "batch-", "--yes", "-o", format})
			if code != ExitFailed {
				t.Fatalf("exit=%d stdout=%s stderr=%s", code, stdout, stderr)
			}
			if strings.Join(attempted, ",") != strings.Join(names, ",") {
				t.Fatalf("attempted=%v", attempted)
			}
			if format == "json" {
				var rows []struct{ Name, Status, Message string }
				if err := json.Unmarshal([]byte(stdout), &rows); err != nil {
					t.Fatal(err)
				}
				if len(rows) != 5 {
					t.Fatalf("rows=%v", rows)
				}
				for i, row := range rows {
					if row.Name != names[i] {
						t.Fatalf("row=%v", row)
					}
					if i == 2 {
						if !strings.HasPrefix(row.Status, "failed: ") {
							t.Fatal(row)
						}
					} else if row.Status != "deleted" || row.Message != "purged "+row.Name {
						t.Fatal(row)
					}
				}
			} else {
				outcomes := stdout
				if format == "names" {
					outcomes = stderr
					if stdout != strings.Join(names, "\n")+"\n" {
						t.Fatal(stdout)
					}
				}
				for i, name := range names {
					want := name + "\tdeleted\tpurged " + name
					if i == 2 {
						want = name + "\tfailed: "
					}
					if !strings.Contains(outcomes, want) {
						t.Fatalf("missing %q in %s", want, outcomes)
					}
				}
			}
			t.Logf("exit=%d\n%s", code, stdout)
		})
	}
}

func TestIndexDeleteRetryAndVerification(t *testing.T) {
	for _, tc := range []struct {
		name                      string
		status                    int
		recover, absent, listFail bool
		attempts                  int
		exit                      int
	}{
		{"502 retry succeeds", 502, true, false, false, 2, ExitOK},
		{"504 retry succeeds", 504, true, false, false, 2, ExitOK},
		{"502 committed upstream", 502, false, true, false, 2, ExitOK},
		{"502 still present", 502, false, false, false, 2, ExitFailed},
		{"504 list fails", 504, false, false, true, 2, ExitFailed},
		{"500 no retry", 500, false, false, false, 1, ExitFailed},
		{"402 remains failure", 402, false, false, false, 1, ExitFailed},
		{"403 no retry", 403, false, false, false, 1, ExitFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			deletes, lists := 0, 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.Method == http.MethodDelete {
					deletes++
					if tc.recover && deletes == 2 {
						fmt.Fprint(w, `{"status":"OK","message":"gateway cleanup message"}`)
						return
					}
					http.Error(w, `{"error":"license_required","message":"delete fault"}`, tc.status)
					return
				}
				lists++
				if r.URL.Query().Get("prefix") != "target" {
					t.Error(r.URL)
				}
				if tc.listFail {
					http.Error(w, "list unavailable", 503)
					return
				}
				if r.URL.Query().Get("cursor") == "" {
					fmt.Fprint(w, `{"namespaces":[{"name":"target-other"}],"next_cursor":"next"}`)
					return
				}
				if tc.absent {
					fmt.Fprint(w, `{"namespaces":[]}`)
				} else {
					fmt.Fprint(w, `{"namespaces":[{"name":"target"}]}`)
				}
			}))
			defer server.Close()
			stdout, stderr, code := runTestCLI(t, server.URL, []string{"index", "delete", "target", "--yes"})
			if code != tc.exit || deletes != tc.attempts {
				t.Fatalf("exit=%d deletes=%d stdout=%s stderr=%s", code, deletes, stdout, stderr)
			}
			if tc.recover {
				if lists != 0 || !strings.Contains(stdout, "gateway cleanup message") {
					t.Fatal(stdout, lists)
				}
			} else if lists == 0 {
				t.Fatal("missing verification")
			}
			if tc.absent && !strings.Contains(stdout, "confirmed absent by list") {
				t.Fatal(stdout)
			}
			if tc.listFail && !strings.Contains(stdout, "list verification failed") {
				t.Fatal(stdout)
			}
		})
	}
}

type deleteRoundTripper func(*http.Request) (*http.Response, error)

func (f deleteRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestIndexDeleteConnectionResetRetry(t *testing.T) {
	calls := 0
	client := hevlayer.NewClient(hevlayer.WithHTTPClient(&http.Client{Transport: deleteRoundTripper(func(r *http.Request) (*http.Response, error) {
		calls++
		if calls == 1 {
			return nil, fmt.Errorf("read: %w", syscall.ECONNRESET)
		}
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"status":"OK","message":"reset recovered"}`))}, nil
	})}))
	var out, errOut bytes.Buffer
	if err := deleteNamespaces(t.Context(), &out, &errOut, client, "table", []string{"target"}); err != nil {
		t.Fatal(err)
	}
	if calls != 2 || !strings.Contains(out.String(), "reset recovered") {
		t.Fatal(calls, out.String())
	}
}
