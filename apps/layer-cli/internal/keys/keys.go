// Package keys calls the gateway /v2/keys routes with wire-faithful
// entitlement maps. The generated Go client's ApiKeyEntitlements model is an
// empty struct (the generator drops additionalProperties maps), which would
// silently lose entitlements in both directions, so mint and list carry their
// own wire types here. Revoke and delete have no entitlement payload and stay
// on the generated client.
package keys

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	hevlayer "github.com/hev/layer/clients/go"
)

// Entitlements maps an entitlement target — "vectorstore.<name>",
// "warehouse.<name>", or "layer" — to its scopes, namespace globs, and claims.
type Entitlements map[string]hevlayer.ApiKeyEntitlement

// Key is one key's metadata as the gateway lists it — never token material.
type Key struct {
	KeyID        string       `json:"keyId"`
	Name         string       `json:"name"`
	Owner        string       `json:"owner,omitempty"`
	Description  string       `json:"description,omitempty"`
	Entitlements Entitlements `json:"entitlements,omitempty"`
	ExpiresAfter string       `json:"expiresAfter,omitempty"`
	Phase        string       `json:"phase"`
	CreatedAt    string       `json:"createdAt,omitempty"`
	ExpiresAt    string       `json:"expiresAt,omitempty"`
	RevokedAt    string       `json:"revokedAt,omitempty"`
	LastSeenAt   string       `json:"lastSeenAt,omitempty"`
}

type MintRequest struct {
	Name         string       `json:"name"`
	Owner        string       `json:"owner,omitempty"`
	Description  string       `json:"description,omitempty"`
	Entitlements Entitlements `json:"entitlements,omitempty"`
	ExpiresAfter string       `json:"expiresAfter,omitempty"`
}

// MintResponse is the only payload that ever carries the raw token.
type MintResponse struct {
	Key
	Token string `json:"token"`
}

// Error mirrors the generated client's HevlayerError shape for the key routes.
type Error struct {
	StatusCode int
	Kind       string
	Message    string
}

func (e *Error) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("hevlayer: status %d: %s", e.StatusCode, e.Message)
	}
	return fmt.Sprintf("hevlayer: status %d", e.StatusCode)
}

// Client speaks to the gateway key routes with the same base URL and bearer
// auth the generated client uses.
type Client struct {
	baseURL    string
	apiKey     string
	httpClient *http.Client
}

func NewClient(baseURL string, apiKey string) *Client {
	return &Client{
		baseURL:    strings.TrimRight(strings.TrimSpace(baseURL), "/"),
		apiKey:     strings.TrimSpace(apiKey),
		httpClient: http.DefaultClient,
	}
}

// Mint creates a key through POST /v2/keys; the response is the only place
// the raw token ever appears.
func (c *Client) Mint(ctx context.Context, body *MintRequest) (*MintResponse, error) {
	out := MintResponse{}
	if err := c.do(ctx, http.MethodPost, "/v2/keys", nil, body, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// List returns key metadata from GET /v2/keys. includeRevoked adds revoked
// and expired keys.
func (c *Client) List(ctx context.Context, includeRevoked bool) ([]Key, error) {
	query := url.Values{}
	if includeRevoked {
		query.Set("includeRevoked", "true")
	}
	out := struct {
		Keys []Key `json:"keys"`
	}{}
	if err := c.do(ctx, http.MethodGet, "/v2/keys", query, nil, &out); err != nil {
		return nil, err
	}
	return out.Keys, nil
}

func (c *Client) do(ctx context.Context, method string, path string, query url.Values, body interface{}, out interface{}) error {
	endpoint := c.baseURL + path
	if len(query) > 0 {
		endpoint += "?" + query.Encode()
	}
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(encoded)
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint, reader)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode >= 400 {
		var payload struct {
			Error   string `json:"error"`
			Message string `json:"message"`
		}
		_ = json.Unmarshal(data, &payload)
		if payload.Message == "" {
			payload.Message = strings.TrimSpace(string(data))
		}
		return &Error{StatusCode: resp.StatusCode, Kind: payload.Error, Message: payload.Message}
	}
	if out == nil || len(data) == 0 {
		return nil
	}
	return json.Unmarshal(data, out)
}
