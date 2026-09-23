package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/hev/layer/apps/layer-cli/internal/config"
)

func TestCEIndexGetHistoryErrors(t *testing.T) {
	for _, tc := range []struct {
		name              string
		metadata, history int
		transport, ok     bool
	}{
		{"missing history", 200, 404, false, true}, {"empty history", 200, 200, false, true},
		{"history unauthorized", 200, 401, false, false}, {"history forbidden", 200, 403, false, false},
		{"history upstream", 200, 502, false, false}, {"history transport", 200, 200, true, false},
		{"metadata missing", 404, 404, false, false}, {"metadata unauthorized", 401, 404, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requireAuth(t, r)
				switch r.URL.Path {
				case "/metrics":
					fmt.Fprintln(w, "layer_namespace_purge_discovery_ready 1")
				case "/v2/namespaces":
					w.WriteHeader(tc.metadata)
					if tc.metadata == 200 {
						fmt.Fprint(w, `{"namespaces":[{"name":"local","row_count":2}]}`)
					}
				case "/v2/namespaces/local/history":
					if tc.transport {
						conn, _, err := w.(http.Hijacker).Hijack()
						if err != nil {
							t.Error(err)
							return
						}
						conn.Close()
						return
					}
					w.WriteHeader(tc.history)
					if tc.history == 200 {
						fmt.Fprint(w, `[]`)
					}
				default:
					t.Errorf("unexpected path %s", r.URL.Path)
					w.WriteHeader(500)
				}
			}))
			defer server.Close()
			stdout, stderr, code := runTestCLI(t, server.URL, []string{"index", "get", "local", "-o", "json"})
			if (code == ExitOK) != tc.ok {
				t.Fatalf("code=%d stdout=%s stderr=%s", code, stdout, stderr)
			}
			if tc.ok {
				var detail indexDetail
				if err := json.Unmarshal([]byte(stdout), &detail); err != nil {
					t.Fatal(err)
				}
				if detail.Name != "local" || detail.RowCount != 2 || detail.Snapshots != 0 || detail.LastSnapshotMs != 0 || detail.SnapshotsAtLimit || len(detail.RecentSnapshots) != 0 {
					t.Fatalf("detail=%+v", detail)
				}
			} else if stderr == "" {
				t.Fatal("error was hidden")
			}
		})
	}
}

func TestCEEnvEmptyKeyReload(t *testing.T) {
	for _, tc := range []struct {
		name          string
		tty, explicit bool
		input, key    string
	}{
		{"noninteractive empty", false, true, "", ""}, {"interactive explicit empty", true, true, "", ""},
		{"interactive prompt empty", true, false, "\n", ""}, {"interactive keyed", true, false, "secret\n", "secret"},
		{"noninteractive keyed", false, true, "", "secret"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				want := ""
				if tc.key != "" {
					want = "Bearer " + tc.key
				}
				if got := r.Header.Get("Authorization"); got != want {
					t.Errorf("authorization=%q want %q", got, want)
				}
				fmt.Fprint(w, `{"namespaces":[{"name":"local"}]}`)
			}))
			defer server.Close()
			opts := defaultTestOptions(t, server.URL)
			opts.Env = map[string]string{}
			opts.StdinIsTerminal = tc.tty
			opts.Stdin = strings.NewReader(tc.input)
			args := []string{"env", "add", "local", "--base-url", server.URL, "--kube-context", "unused", "--kube-namespace", "unused"}
			if tc.explicit {
				args = append(args, "--api-key", tc.key)
			}
			stdout, stderr, code := runCLIWithOptions(t, opts, args)
			if code != ExitOK {
				t.Fatalf("code=%d stdout=%s stderr=%s", code, stdout, stderr)
			}
			if tc.explicit && strings.Contains(stdout, "API key") {
				t.Fatalf("explicit key prompted: %s", stdout)
			}
			cfg, err := config.Load(opts.HomeDir)
			if err != nil {
				t.Fatal(err)
			}
			env, ok := cfg.Envs["local"]
			if !ok || env.APIKey != tc.key || env.BaseURL != server.URL {
				t.Fatalf("config=%+v", cfg)
			}
			stdout, stderr, code = runCLIWithOptions(t, opts, []string{"index", "list", "-o", "names"})
			if code != ExitOK || stdout != "local\n" {
				t.Fatalf("reload code=%d stdout=%s stderr=%s", code, stdout, stderr)
			}
		})
	}
}

func TestCEEnvMissingKeyArgument(t *testing.T) {
	for _, tail := range [][]string{nil, {"--api-key"}} {
		opts := defaultTestOptions(t, "http://unused.invalid")
		args := append([]string{"env", "add", "local", "--base-url", "http://localhost:8080"}, tail...)
		_, stderr, code := runCLIWithOptions(t, opts, args)
		if code != ExitUsage || !strings.Contains(stderr, "api-key") {
			t.Fatalf("code=%d stderr=%s", code, stderr)
		}
		cfg, err := config.Load(opts.HomeDir)
		if err != nil || len(cfg.Envs) != 0 {
			t.Fatalf("config saved on failure: %+v %v", cfg, err)
		}
	}
}

func TestCESnapshotSources(t *testing.T) {
	for _, source := range []string{"auto", "stored", "cache", "origin", "snapshot", "invalid"} {
		t.Run(source, func(t *testing.T) {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				var body struct {
					Source string `json:"source"`
				}
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Error(err)
				}
				want := source
				if want == "snapshot" {
					want = "stored"
				}
				if body.Source != want {
					t.Errorf("source=%q want %q", body.Source, want)
				}
				fmt.Fprint(w, `{"id":"snap-1","status":"completed"}`)
			}))
			defer server.Close()
			_, stderr, code := runTestCLI(t, server.URL, []string{"index", "snapshot", "local", "--field", "category", "--source", source})
			if source == "invalid" {
				if code != ExitUsage || calls != 0 {
					t.Fatalf("code=%d calls=%d stderr=%s", code, calls, stderr)
				}
			} else if code != ExitOK || calls != 1 {
				t.Fatalf("code=%d calls=%d stderr=%s", code, calls, stderr)
			}
		})
	}
	stdout, _, code := runTestCLI(t, "http://unused.invalid", []string{"index", "snapshot", "--help"})
	if code != ExitOK || !strings.Contains(stdout, "auto, stored, cache, or origin") {
		t.Fatalf("help=%s", stdout)
	}
}

func TestCEExplicitEmptyKeyAndTUILaunch(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "" {
			t.Error("unexpected authorization")
		}
		fmt.Fprint(w, `{"namespaces":[]}`)
	}))
	defer server.Close()
	opts := defaultTestOptions(t, server.URL)
	opts.Env = map[string]string{}
	_, stderr, code := runCLIWithOptions(t, opts, []string{"index", "list", "--base-url", server.URL, "--api-key", "", "-o", "names"})
	if code != ExitOK {
		t.Fatalf("code=%d stderr=%s", code, stderr)
	}
	if err := config.AddEnv(opts.HomeDir, "local", config.EnvConfig{BaseURL: server.URL, APIKey: ""}); err != nil {
		t.Fatal(err)
	}
	called := false
	opts.StdinIsTerminal = true
	opts.StdoutIsTerminal = true
	opts.LaunchTUI = func(_ context.Context, tuiOpts TUIOptions) error {
		called = true
		resolved, err := config.Resolve(tuiOpts.HomeDir, tuiOpts.Env, tuiOpts.Request)
		if err != nil || resolved.BaseURL != server.URL || resolved.APIKey != "" {
			t.Fatalf("TUI config=%+v err=%v", resolved, err)
		}
		return nil
	}
	_, stderr, code = runCLIWithOptions(t, opts, nil)
	if code != ExitOK || !called {
		t.Fatalf("TUI code=%d called=%v stderr=%s", code, called, stderr)
	}
}
