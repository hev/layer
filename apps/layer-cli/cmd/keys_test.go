package cmd

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/hev/layer/apps/layer-cli/internal/keys"
	hevlayer "github.com/hev/layer/clients/go"
)

func TestParseEntitlements(t *testing.T) {
	cases := []struct {
		name       string
		entitles   []string
		claims     []string
		namespaces string
		want       keys.Entitlements
	}{
		{
			name:     "single scope",
			entitles: []string{"vectorstore.prod=read"},
			want: keys.Entitlements{
				"vectorstore.prod": {Scopes: []string{"read"}},
			},
		},
		{
			name:     "layer admin",
			entitles: []string{"layer=admin"},
			want: keys.Entitlements{
				"layer": {Scopes: []string{"admin"}},
			},
		},
		{
			name:     "multiple scopes joined with plus",
			entitles: []string{"vectorstore.prod=read+write"},
			want: keys.Entitlements{
				"vectorstore.prod": {Scopes: []string{"read", "write"}},
			},
		},
		{
			name:     "bare target carries no scopes",
			entitles: []string{"warehouse.prod-snowflake"},
			want: keys.Entitlements{
				"warehouse.prod-snowflake": {},
			},
		},
		{
			name:     "agent target carries no scopes",
			entitles: []string{"agent.chart-notes"},
			want: keys.Entitlements{
				"agent.chart-notes": {},
			},
		},
		{
			name:     "repeated target merges and dedupes scopes",
			entitles: []string{"layer=read", "layer=admin", "layer=read"},
			want: keys.Entitlements{
				"layer": {Scopes: []string{"read", "admin"}},
			},
		},
		{
			name:   "claim creates the target entitlement",
			claims: []string{"warehouse.prod-snowflake=notes:cohort:*:read"},
			want: keys.Entitlements{
				"warehouse.prod-snowflake": {Claims: []string{"notes:cohort:*:read"}},
			},
		},
		{
			name:     "claim appends to an entitled target",
			entitles: []string{"warehouse.prod=read"},
			claims:   []string{"warehouse.prod=a:b", "warehouse.prod=c:d"},
			want: keys.Entitlements{
				"warehouse.prod": {Scopes: []string{"read"}, Claims: []string{"a:b", "c:d"}},
			},
		},
		{
			name:       "namespaces attach to the vectorstore entitlement",
			entitles:   []string{"vectorstore.prod=read", "layer=read"},
			namespaces: "cohort-*, shop-*",
			want: keys.Entitlements{
				"vectorstore.prod": {Scopes: []string{"read"}, Namespaces: []string{"cohort-*", "shop-*"}},
				"layer":            {Scopes: []string{"read"}},
			},
		},
		{
			name: "no flags yields no entitlements",
			want: nil,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseEntitlements(tc.entitles, tc.claims, tc.namespaces)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got %#v want %#v", got, tc.want)
			}
		})
	}
}

func TestParseEntitlementsErrors(t *testing.T) {
	cases := []struct {
		name       string
		entitles   []string
		claims     []string
		namespaces string
		wantErr    string
	}{
		{
			name:     "unknown target prefix",
			entitles: []string{"store.prod=read"},
			wantErr:  "must be vectorstore.<name>, warehouse.<name>, agent.<name>, or layer",
		},
		{
			name:     "vectorstore without a name",
			entitles: []string{"vectorstore.=read"},
			wantErr:  "missing a resource name",
		},
		{
			name:     "empty scope",
			entitles: []string{"vectorstore.prod="},
			wantErr:  "empty scope",
		},
		{
			name:     "empty scope between plus signs",
			entitles: []string{"vectorstore.prod=read++write"},
			wantErr:  "empty scope",
		},
		{
			name:    "claim without value",
			claims:  []string{"warehouse.prod"},
			wantErr: "must be TARGET=STRING",
		},
		{
			name:    "claim with empty value",
			claims:  []string{"warehouse.prod="},
			wantErr: "must be TARGET=STRING",
		},
		{
			name:    "claim with bad target",
			claims:  []string{"layerz=notes:read"},
			wantErr: "must be vectorstore.<name>, warehouse.<name>, agent.<name>, or layer",
		},
		{
			name:     "agent entitlement rejects scopes",
			entitles: []string{"agent.chart-notes=read"},
			wantErr:  "drop scopes",
		},
		{
			name:       "namespaces without a vectorstore entitlement",
			entitles:   []string{"layer=admin"},
			namespaces: "cohort-*",
			wantErr:    "requires a vectorstore entitlement",
		},
		{
			name:       "namespaces ambiguous across vectorstores",
			entitles:   []string{"vectorstore.a=read", "vectorstore.b=read"},
			namespaces: "cohort-*",
			wantErr:    "ambiguous",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseEntitlements(tc.entitles, tc.claims, tc.namespaces)
			if err == nil {
				t.Fatal("expected an error")
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error %q does not contain %q", err, tc.wantErr)
			}
		})
	}
}

func TestKeysMintSendsEntitlementsAndPrintsTokenAlone(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requireAuth(t, r)
		if r.Method != http.MethodPost || r.URL.Path != "/v2/keys" {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
		var body map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		want := map[string]interface{}{
			"name":  "cohort-reader",
			"owner": "acme",
			"entitlements": map[string]interface{}{
				"vectorstore.prod-turbopuffer": map[string]interface{}{
					"scopes":     []interface{}{"read"},
					"namespaces": []interface{}{"cohort-*"},
				},
				"warehouse.prod-snowflake": map[string]interface{}{
					"claims": []interface{}{"notes:cohort:*:read"},
				},
			},
			"expiresAfter": "365d",
		}
		if !reflect.DeepEqual(body, want) {
			t.Fatalf("mint body=%#v want %#v", body, want)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		payload := apiKeyPayload()
		payload["token"] = "hvl_iqGFsDD2PNkyhCqr59jjvKuKL47vqXMz"
		_ = json.NewEncoder(w).Encode(payload)
	}))
	defer server.Close()

	stdout, stderr, code := runTestCLI(t, server.URL, []string{
		"keys", "mint", "cohort-reader", "--owner", "acme",
		"--entitle", "vectorstore.prod-turbopuffer=read",
		"--namespaces", "cohort-*",
		"--claim", "warehouse.prod-snowflake=notes:cohort:*:read",
	})
	if code != ExitOK {
		t.Fatalf("code=%d stderr=%s", code, stderr)
	}
	// The raw token prints once, alone on stdout, so pipes capture only it.
	if stdout != "hvl_iqGFsDD2PNkyhCqr59jjvKuKL47vqXMz\n" {
		t.Fatalf("stdout must carry only the token: %q", stdout)
	}
	if !strings.Contains(stderr, "KEY_ID") || !strings.Contains(stderr, "cohort-reader") {
		t.Fatalf("metadata missing from stderr: %q", stderr)
	}
	if strings.Contains(stderr, "hvl_") {
		t.Fatalf("token leaked to stderr: %q", stderr)
	}
}

func TestKeysMintFromManifest(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requireAuth(t, r)
		if r.Method != http.MethodPost || r.URL.Path != "/v2/keys" {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
		var body map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body["name"] != "cohort-reader" || body["owner"] != "acme" {
			t.Fatalf("manifest fields missing: %#v", body)
		}
		if body["expiresAfter"] != "30d" {
			t.Fatalf("manifest expiresAfter not honored: %#v", body)
		}
		entitlements := body["entitlements"].(map[string]interface{})
		if _, ok := entitlements["vectorstore.prod-turbopuffer"]; !ok {
			t.Fatalf("manifest entitlements missing: %#v", entitlements)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		payload := apiKeyPayload()
		payload["token"] = "hvl_manifest"
		_ = json.NewEncoder(w).Encode(payload)
	}))
	defer server.Close()

	manifest := filepath.Join(t.TempDir(), "key.yaml")
	err := os.WriteFile(manifest, []byte(`
apiVersion: hevlayer.com/v1alpha1
kind: ApiKey
metadata:
  name: cohort-reader
spec:
  owner: acme
  expiresAfter: 30d
  entitlements:
    vectorstore.prod-turbopuffer:
      scopes: [read]
      namespaces: ["cohort-*"]
`), 0o600)
	if err != nil {
		t.Fatal(err)
	}

	stdout, stderr, code := runTestCLI(t, server.URL, []string{"keys", "mint", "-f", manifest})
	if code != ExitOK {
		t.Fatalf("code=%d stderr=%s", code, stderr)
	}
	if stdout != "hvl_manifest\n" {
		t.Fatalf("stdout must carry only the token: %q", stdout)
	}
}

func TestKeysMintManifestRejectsConflictingFlags(t *testing.T) {
	called := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	}))
	defer server.Close()

	manifest := filepath.Join(t.TempDir(), "key.yaml")
	if err := os.WriteFile(manifest, []byte("apiVersion: hevlayer.com/v1alpha1\nkind: ApiKey\nmetadata:\n  name: x\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	for _, args := range [][]string{
		{"keys", "mint", "-f", manifest, "--owner", "acme"},
		{"keys", "mint", "-f", manifest, "--entitle", "layer=admin"},
		{"keys", "mint", "extra-name", "-f", manifest},
	} {
		_, stderr, code := runTestCLI(t, server.URL, args)
		if code != ExitUsage {
			t.Fatalf("args=%v code=%d stderr=%s", args, code, stderr)
		}
		if !strings.Contains(stderr, "manifest") {
			t.Fatalf("args=%v unexpected stderr: %q", args, stderr)
		}
	}
	if called {
		t.Fatal("mint request was sent despite conflicting flags")
	}
}

func TestKeysMintManifestRequiresApiKeyKind(t *testing.T) {
	manifest := filepath.Join(t.TempDir(), "key.yaml")
	if err := os.WriteFile(manifest, []byte("apiVersion: hevlayer.com/v1alpha1\nkind: Function\nmetadata:\n  name: x\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, stderr, code := runTestCLI(t, "http://unused.invalid", []string{"keys", "mint", "-f", manifest})
	if code != ExitFailed {
		t.Fatalf("code=%d stderr=%s", code, stderr)
	}
	if !strings.Contains(stderr, "kind: ApiKey") {
		t.Fatalf("unexpected stderr: %q", stderr)
	}
}

func TestKeysListShowsMetadataNeverTokens(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requireAuth(t, r)
		if r.Method != http.MethodGet || r.URL.Path != "/v2/keys" {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
		if r.URL.Query().Get("includeRevoked") != "true" {
			t.Fatalf("missing includeRevoked: %s", r.URL.RawQuery)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"keys": []map[string]interface{}{apiKeyPayload()},
		})
	}))
	defer server.Close()

	stdout, stderr, code := runTestCLI(t, server.URL, []string{"keys", "ls", "--include-revoked"})
	if code != ExitOK {
		t.Fatalf("code=%d stderr=%s", code, stderr)
	}
	for _, column := range []string{"KEY_ID", "NAME", "OWNER", "PHASE", "ENTITLEMENTS", "EXPIRES_AT", "LAST_SEEN_AT"} {
		if !strings.Contains(stdout, column) {
			t.Fatalf("column %s missing: %s", column, stdout)
		}
	}
	if !strings.Contains(stdout, "vectorstore.prod-turbopuffer,warehouse.prod-snowflake") {
		t.Fatalf("entitlement targets missing: %s", stdout)
	}
	if strings.Contains(stdout, "hvl_") {
		t.Fatalf("token material leaked: %s", stdout)
	}
}

func TestKeysGetResolvesNameViaList(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requireAuth(t, r)
		if r.Method != http.MethodGet || r.URL.Path != "/v2/keys" {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"keys": []map[string]interface{}{apiKeyPayload()},
		})
	}))
	defer server.Close()

	stdout, stderr, code := runTestCLI(t, server.URL, []string{"keys", "get", "cohort-reader"})
	if code != ExitOK {
		t.Fatalf("code=%d stderr=%s", code, stderr)
	}
	if !strings.Contains(stdout, "0a1b2c3d-4e5f-6071-8293-a4b5c6d7e8f9") {
		t.Fatalf("key id missing: %s", stdout)
	}
	if !strings.Contains(stdout, "ENTITLEMENT vectorstore.prod-turbopuffer") || !strings.Contains(stdout, "namespaces=cohort-*") {
		t.Fatalf("entitlement detail missing: %s", stdout)
	}

	_, stderr, code = runTestCLI(t, server.URL, []string{"keys", "get", "missing-key"})
	if code != ExitFailed {
		t.Fatalf("code=%d stderr=%s", code, stderr)
	}
	if !strings.Contains(stderr, `key "missing-key" not found`) {
		t.Fatalf("unexpected stderr: %q", stderr)
	}
}

func TestKeysRevokeResolvesNameAndPostsRevoke(t *testing.T) {
	revoked := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requireAuth(t, r)
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v2/keys":
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"keys": []map[string]interface{}{apiKeyPayload()},
			})
		case r.Method == http.MethodPost && r.URL.Path == "/v2/keys/0a1b2c3d-4e5f-6071-8293-a4b5c6d7e8f9/revoke":
			revoked = true
			payload := apiKeyPayload()
			payload["phase"] = "Revoked"
			_ = json.NewEncoder(w).Encode(payload)
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	stdout, stderr, code := runTestCLI(t, server.URL, []string{"keys", "revoke", "cohort-reader"})
	if code != ExitOK {
		t.Fatalf("code=%d stderr=%s", code, stderr)
	}
	if !revoked {
		t.Fatal("revoke route was not called")
	}
	if !strings.Contains(stdout, "revoked") {
		t.Fatalf("unexpected stdout: %q", stdout)
	}
}

func TestKeysRmDeletesByID(t *testing.T) {
	deleted := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requireAuth(t, r)
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v2/keys":
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"keys": []map[string]interface{}{apiKeyPayload()},
			})
		case r.Method == http.MethodDelete && r.URL.Path == "/v2/keys/0a1b2c3d-4e5f-6071-8293-a4b5c6d7e8f9":
			deleted = true
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"status": "OK"})
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	stdout, stderr, code := runTestCLI(t, server.URL, []string{"keys", "rm", "0a1b2c3d-4e5f-6071-8293-a4b5c6d7e8f9"})
	if code != ExitOK {
		t.Fatalf("code=%d stderr=%s", code, stderr)
	}
	if !deleted {
		t.Fatal("delete route was not called")
	}
	if !strings.Contains(stdout, "deleted") {
		t.Fatalf("unexpected stdout: %q", stdout)
	}
}

func TestFormatEntitlement(t *testing.T) {
	got := formatEntitlement(hevlayer.ApiKeyEntitlement{
		Scopes:     []string{"read", "write"},
		Namespaces: []string{"cohort-*"},
		Claims:     []string{"a:b"},
	})
	if got != "scopes=read+write namespaces=cohort-* claims=a:b" {
		t.Fatalf("unexpected rendering: %q", got)
	}
	if formatEntitlement(hevlayer.ApiKeyEntitlement{}) != "—" {
		t.Fatalf("empty entitlement should render a dash")
	}
}

func apiKeyPayload() map[string]interface{} {
	return map[string]interface{}{
		"keyId": "0a1b2c3d-4e5f-6071-8293-a4b5c6d7e8f9",
		"name":  "cohort-reader",
		"owner": "acme",
		"entitlements": map[string]interface{}{
			"vectorstore.prod-turbopuffer": map[string]interface{}{"scopes": []string{"read"}, "namespaces": []string{"cohort-*"}},
			"warehouse.prod-snowflake":     map[string]interface{}{"claims": []string{"notes:cohort:*:read"}},
		},
		"phase":      "Active",
		"createdAt":  "2026-06-10T00:00:00Z",
		"expiresAt":  "2027-06-10T00:00:00Z",
		"lastSeenAt": "2026-06-10T12:00:00Z",
	}
}
