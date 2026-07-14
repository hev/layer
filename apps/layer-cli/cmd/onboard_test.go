package cmd

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/hev/layer/apps/layer-cli/internal/config"
)

func TestOnboardingInteractiveSavesAndProceeds(t *testing.T) {
	var gotAuth string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/v2/license" {
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"valid":   false,
				"gateway": map[string]interface{}{"state": "floor", "seconds_to_deadline": 0, "grace_seconds_remaining": 0},
			})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"namespaces":  []map[string]interface{}{{"name": "shop-products", "row_count": 10}},
			"next_cursor": "",
		})
	}))
	defer server.Close()

	home := t.TempDir()
	opts := Options{
		// Answer the two onboarding prompts: gateway URL, then API key.
		Stdin:            strings.NewReader(server.URL + "\nsecret-token\n"),
		Env:              map[string]string{},
		HomeDir:          home,
		StdinIsTerminal:  true,
		StdoutIsTerminal: true,
		Version:          "test",
	}

	stdout, stderr, code := runCLIWithOptions(t, opts, []string{"index", "list", "-o", "json"})
	if code != ExitOK {
		t.Fatalf("code=%d stderr=%s", code, stderr)
	}
	if gotAuth != "Bearer secret-token" {
		t.Fatalf("gateway saw auth %q, want Bearer secret-token", gotAuth)
	}
	if !strings.Contains(stderr, "set up hev layer") {
		t.Fatalf("onboarding banner missing from stderr: %q", stderr)
	}
	wantNudge := "Saved environment \"default\". Manage it later with `layer env`.\n\n" + openGatewayNudge
	if !strings.Contains(stderr, wantNudge) {
		t.Fatalf("open-gateway onboarding nudge missing or changed: %q", stderr)
	}
	if !strings.Contains(stdout, "shop-products") {
		t.Fatalf("command output missing after onboarding: %q", stdout)
	}

	env, ok, err := config.GetEnv(home, onboardEnvName)
	if err != nil || !ok {
		t.Fatalf("default environment not saved: ok=%v err=%v", ok, err)
	}
	if env.APIKey != "secret-token" || env.BaseURL != server.URL {
		t.Fatalf("saved env = %+v", env)
	}
}

func TestOnboardingDefaultsBaseURLOnEmptyAnswer(t *testing.T) {
	home := t.TempDir()
	opts := Options{
		// Accept the default gateway URL (empty line), then supply a key.
		Stdin:            strings.NewReader("\nsecret-token\n"),
		Env:              map[string]string{},
		HomeDir:          home,
		StdinIsTerminal:  true,
		StdoutIsTerminal: true,
		Version:          "test",
	}

	// The default gateway is unreachable here, so the command fails, but the
	// credentials should still be persisted with the default base URL.
	_, _, _ = runCLIWithOptions(t, opts, []string{"index", "list"})

	env, ok, err := config.GetEnv(home, onboardEnvName)
	if err != nil || !ok {
		t.Fatalf("default environment not saved: ok=%v err=%v", ok, err)
	}
	if env.APIKey != "secret-token" {
		t.Fatalf("saved api key = %q", env.APIKey)
	}
	if env.BaseURL == "" {
		t.Fatalf("expected default base url to be stored, got empty")
	}
}

func TestOnboardingLicensedGatewayOmitsTrialNudge(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/v2/license" {
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"valid":   true,
				"gateway": map[string]interface{}{"state": "licensed", "seconds_to_deadline": 18 * 86400, "grace_seconds_remaining": 0},
			})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"namespaces": []interface{}{}, "next_cursor": ""})
	}))
	defer server.Close()

	opts := Options{
		Stdin:            strings.NewReader(server.URL + "\nsecret-token\n"),
		Env:              map[string]string{},
		HomeDir:          t.TempDir(),
		StdinIsTerminal:  true,
		StdoutIsTerminal: true,
		Version:          "test",
	}
	_, stderr, code := runCLIWithOptions(t, opts, []string{"index", "list"})
	if code != ExitOK {
		t.Fatalf("code=%d stderr=%s", code, stderr)
	}
	if strings.Contains(stderr, "Start a free 30-day trial") {
		t.Fatalf("licensed onboarding printed trial nudge: %q", stderr)
	}
}

func TestMissingCredentialsNonInteractive(t *testing.T) {
	opts := Options{
		Stdin:            strings.NewReader(""),
		Env:              map[string]string{},
		HomeDir:          t.TempDir(),
		StdinIsTerminal:  false,
		StdoutIsTerminal: false,
		Version:          "test",
	}

	stdout, stderr, code := runCLIWithOptions(t, opts, []string{"index", "list"})
	if code != ExitUsage {
		t.Fatalf("code=%d stderr=%s", code, stderr)
	}
	if stdout != "" {
		t.Fatalf("unexpected stdout: %q", stdout)
	}
	if !strings.Contains(stderr, "No API key configured") {
		t.Fatalf("missing onboarding help in stderr: %q", stderr)
	}
}
