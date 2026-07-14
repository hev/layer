package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestResolvePrecedence(t *testing.T) {
	home := t.TempDir()
	if err := AddEnv(home, "active", EnvConfig{
		BaseURL:       "https://active.example",
		APIKey:        "active-key",
		KubeContext:   "active-context",
		KubeNamespace: "active-namespace",
	}); err != nil {
		t.Fatal(err)
	}
	if err := AddEnv(home, "partner", EnvConfig{
		BaseURL:       "https://partner.example",
		APIKey:        "partner-key",
		KubeContext:   "partner-context",
		KubeNamespace: "partner-namespace",
	}); err != nil {
		t.Fatal(err)
	}

	resolved, err := Resolve(home, map[string]string{"LAYER_ENV": "partner"}, ResolveRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if resolved.BaseURL != "https://partner.example" || resolved.APIKey != "partner-key" || resolved.KubeContext != "partner-context" {
		t.Fatalf("LAYER_ENV did not select partner: %#v", resolved)
	}

	resolved, err = Resolve(home, map[string]string{
		"LAYER_BASE_URL": "https://env.example",
		"LAYER_API_KEY":  "env-key",
		"LAYER_ENV":      "partner",
	}, ResolveRequest{
		BaseURL:          "https://flag.example",
		BaseURLSet:       true,
		APIKey:           "flag-key",
		APIKeySet:        true,
		KubeContext:      "flag-context",
		KubeContextSet:   true,
		KubeNamespace:    "flag-namespace",
		KubeNamespaceSet: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if resolved.BaseURL != "https://flag.example" || resolved.APIKey != "flag-key" {
		t.Fatalf("flags did not win over env vars: %#v", resolved)
	}
	if resolved.KubeContext != "flag-context" || resolved.KubeNamespace != "flag-namespace" {
		t.Fatalf("explicit kube flags did not win: %#v", resolved)
	}
}

func TestRemoveActivePromotesAnotherEnv(t *testing.T) {
	home := t.TempDir()
	if err := AddEnv(home, "b", EnvConfig{BaseURL: "https://b.example", APIKey: "b"}); err != nil {
		t.Fatal(err)
	}
	if err := AddEnv(home, "a", EnvConfig{BaseURL: "https://a.example", APIKey: "a"}); err != nil {
		t.Fatal(err)
	}
	if err := RemoveEnv(home, "b"); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(home)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Active != "a" {
		t.Fatalf("active=%q", cfg.Active)
	}
}

func TestDirectGatewayEnvBypassesMalformedConfig(t *testing.T) {
	home := t.TempDir()
	if err := os.MkdirAll(filepath.Join(home, ".hevlayer"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(home, ".hevlayer", "config.toml"), []byte("not = [valid"), 0o600); err != nil {
		t.Fatal(err)
	}

	resolved, err := Resolve(home, map[string]string{
		"LAYER_BASE_URL": "https://env.example",
		"LAYER_API_KEY":  "env-key",
	}, ResolveRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if resolved.BaseURL != "https://env.example" || resolved.APIKey != "env-key" || resolved.ConfigUsed {
		t.Fatalf("unexpected resolution: %#v", resolved)
	}
}
