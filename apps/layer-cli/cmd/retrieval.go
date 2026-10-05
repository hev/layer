package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/hev/layer/apps/layer-cli/internal/config"
	"github.com/spf13/cobra"
)

// Keep wire extensions that are absent from generated response models.
type retrievalReceipt struct {
	Contract string            `json:"contract"`
	Data     json.RawMessage   `json:"data"`
	Headers  map[string]string `json:"headers"`
}

func retrievalRequest(ctx context.Context, cfg config.Resolved, method, path string, body []byte) (retrievalReceipt, error) {
	out := retrievalReceipt{Contract: "layer.retrieval.v1", Headers: map[string]string{}}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(cfg.BaseURL, "/")+path, bytes.NewReader(body))
	if err != nil {
		return out, fmt.Errorf("invalid gateway request")
	}
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if cfg.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+cfg.APIKey)
	}
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return out, ctx.Err()
		}
		return out, fmt.Errorf("gateway transport failed")
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 32*1024*1024+1))
	if err != nil {
		return out, fmt.Errorf("reading gateway response failed")
	}
	if len(raw) > 32*1024*1024 {
		return out, fmt.Errorf("gateway response exceeds 32 MiB; request a smaller page")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var detail struct {
			Error string `json:"error"`
			Code  string `json:"code"`
		}
		_ = json.Unmarshal(raw, &detail)
		code := detail.Error
		if code == "" {
			code = detail.Code
		}
		if len(code) > 128 || strings.ContainsAny(code, " \n\r\t") || (cfg.APIKey != "" && strings.Contains(code, cfg.APIKey)) {
			code = ""
		}
		return out, fmt.Errorf("gateway HTTP %d %s", resp.StatusCode, code)
	}
	if !json.Valid(raw) {
		return out, fmt.Errorf("gateway returned invalid JSON")
	}
	out.Data = raw
	for _, key := range []string{"x-layer-next-cursor", "x-layer-stable-as-of", "x-layer-warning", "x-layer-cache", "traceparent"} {
		if value := resp.Header.Get(key); value != "" {
			out.Headers[key] = value
		}
	}
	return out, nil
}

func readRetrievalBody(app App, file string) ([]byte, error) {
	var reader io.Reader = app.opts.Stdin
	if file != "-" {
		f, err := os.Open(file)
		if err != nil {
			return nil, usagef("cannot open request file")
		}
		defer f.Close()
		reader = f
	}
	raw, err := io.ReadAll(io.LimitReader(reader, 8*1024*1024+1))
	if err != nil || len(raw) > 8*1024*1024 {
		return nil, usagef("request exceeds 8 MiB or could not be read")
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil || obj == nil {
		return nil, usagef("request must be one JSON object")
	}
	return raw, nil
}

func retrievalCommand(app App, flags *globalFlags, name, method string, bodyRequired bool, nargs int, path func([]string, *cobra.Command) (string, error)) *cobra.Command {
	var file, spaceFile string
	var timeout time.Duration
	cmd := &cobra.Command{Use: name + " NAMESPACE", Short: "Gateway retrieval API (JSON receipt)", Args: cobra.ExactArgs(nargs)}
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		if timeout <= 0 {
			return usagef("timeout must be positive")
		}
		if flags.output != "table" && flags.output != "json" {
			return usagef("retrieval supports JSON output only")
		}
		target, err := path(args, cmd)
		if err != nil {
			return err
		}
		var body []byte
		if bodyRequired {
			if file == "" {
				return usagef("--file is required (use - for stdin)")
			}
			body, err = readRetrievalBody(app, file)
			if err != nil {
				return err
			}
		}
		if name == "query" || name == "query-all" || name == "create" {
			if err := validateEmbeddingSpace(body, spaceFile); err != nil {
				return err
			}
		}
		cfg, _, err := app.resolve(cmd, flags, runFlags{})
		if err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(cmd.Context(), timeout)
		defer cancel()
		receipt, err := retrievalRequest(ctx, cfg, method, target, body)
		if err != nil {
			return err
		}
		return json.NewEncoder(cmd.OutOrStdout()).Encode(receipt)
	}
	if name == "query" || name == "query-all" || name == "create" {
		cmd.Flags().StringVar(&spaceFile, "embedding-space", "", "Verified query/document embedding identity receipt for numeric ANN")
	}
	if nargs == 0 {
		cmd.Use = name
	}
	if nargs == 2 {
		cmd.Use += " ID"
	}
	if bodyRequired {
		cmd.Flags().StringVarP(&file, "file", "f", "", "JSON request file, or - for stdin")
	}
	cmd.Flags().DurationVar(&timeout, "timeout", 30*time.Second, "Request deadline")
	return cmd
}

func namespacePath(args []string) string { return "/v2/namespaces/" + url.PathEscape(args[0]) }
func addRetrievalCommands(root *cobra.Command, app App, flags *globalFlags) {
	for _, item := range []struct{ name, suffix string }{{"query", "/query"}, {"search", "/search"}, {"read", "/documents"}} {
		suffix := item.suffix
		root.AddCommand(retrievalCommand(app, flags, item.name, "POST", true, 1, func(args []string, _ *cobra.Command) (string, error) { return namespacePath(args) + suffix, nil }))
	}
	root.AddCommand(retrievalCommand(app, flags, "query-all", "POST", true, 0, func(_ []string, _ *cobra.Command) (string, error) { return "/v2/query", nil }))
	root.AddCommand(retrievalCommand(app, flags, "explain", "POST", true, 1, func(args []string, _ *cobra.Command) (string, error) {
		return namespacePath(args) + "/explain_query", nil
	}))
	root.AddCommand(newCapabilitiesCommand(app, flags))
	scan := &cobra.Command{Use: "scan", Short: "Exhaustive selection jobs and paged results"}
	for _, item := range []struct {
		name, method string
		body         bool
		nargs        int
	}{{"create", "POST", true, 1}, {"list", "GET", false, 1}, {"get", "GET", false, 2}, {"delete", "DELETE", false, 2}} {
		scan.AddCommand(retrievalCommand(app, flags, item.name, item.method, item.body, item.nargs, func(args []string, _ *cobra.Command) (string, error) {
			p := namespacePath(args) + "/scans"
			if len(args) == 2 {
				p += "/" + url.PathEscape(args[1])
			}
			return p, nil
		}))
	}
	var limit, offset int64
	results := retrievalCommand(app, flags, "results", "GET", false, 2, func(args []string, _ *cobra.Command) (string, error) {
		if limit < 1 || limit > 10000 || offset < 0 {
			return "", usagef("limit must be 1..10000 and offset nonnegative")
		}
		return fmt.Sprintf("%s/scans/%s/results?limit=%d&offset=%d", namespacePath(args), url.PathEscape(args[1]), limit, offset), nil
	})
	results.Flags().Int64Var(&limit, "limit", 1000, "Result page size (1..10000)")
	results.Flags().Int64Var(&offset, "offset", 0, "Result offset")
	scan.AddCommand(results)
	root.AddCommand(scan)
}

func newCapabilitiesCommand(app App, flags *globalFlags) *cobra.Command {
	var store bool
	var timeout time.Duration
	cmd := &cobra.Command{Use: "capabilities NAME", Short: "Store capabilities and namespace schema receipt", Args: cobra.ExactArgs(1)}
	cmd.Flags().BoolVar(&store, "store", false, "Argument is a VectorStore name")
	cmd.Flags().DurationVar(&timeout, "timeout", 30*time.Second, "Total request deadline")
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		if timeout <= 0 {
			return usagef("timeout must be positive")
		}
		if flags.output != "table" && flags.output != "json" {
			return usagef("retrieval supports JSON output only")
		}
		cfg, _, err := app.resolve(cmd, flags, runFlags{})
		if err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(cmd.Context(), timeout)
		defer cancel()
		path := namespacePath(args)
		if store {
			path = "/v2/vectorstores/" + url.PathEscape(args[0])
		}
		caps, err := retrievalRequest(ctx, cfg, "GET", path+"/capabilities", nil)
		if err != nil {
			return err
		}
		if store {
			return json.NewEncoder(cmd.OutOrStdout()).Encode(caps)
		}
		metadata, err := retrievalRequest(ctx, cfg, "GET", path+"/metadata", nil)
		if err != nil {
			return err
		}
		return json.NewEncoder(cmd.OutOrStdout()).Encode(struct {
			Contract     string          `json:"contract"`
			Capabilities json.RawMessage `json:"capabilities"`
			Metadata     json.RawMessage `json:"metadata"`
		}{"layer.retrieval.v1", caps.Data, metadata.Data})
	}
	return cmd
}

// This checks a caller's verified corpus receipt, not vector provenance. Equal
// dimensions alone do not establish compatible embedding spaces.
type embeddingIdentity struct {
	Model          string `json:"model"`
	Revision       string `json:"revision"`
	Dims           int    `json:"dims"`
	Normalization  string `json:"normalization"`
	DistanceMetric string `json:"distance_metric"`
}

func validateEmbeddingSpace(body []byte, file string) error {
	var request struct {
		Vector  []json.RawMessage `json:"vector"`
		RankBy  []json.RawMessage `json:"rank_by"`
		Queries []json.RawMessage `json:"queries"`
		Ann     *struct {
			Vector []json.RawMessage `json:"vector"`
		} `json:"ann"`
	}
	if err := json.Unmarshal(body, &request); err != nil {
		return usagef("invalid query shape")
	}
	vector := request.Vector
	if request.Ann != nil {
		vector = request.Ann.Vector
	}
	if len(request.RankBy) == 3 {
		var op string
		_ = json.Unmarshal(request.RankBy[1], &op)
		if op == "ANN" {
			_ = json.Unmarshal(request.RankBy[2], &vector)
		}
	}
	// Nested queries can contain multiple vector spaces; require callers to
	// submit numeric ANN individually so every vector is checked.
	for _, leg := range request.Queries {
		if err := validateEmbeddingSpace(leg, file); err != nil {
			return err
		}
	}
	if len(vector) == 2 {
		var op string
		if json.Unmarshal(vector[0], &op) == nil && op == "Embed" {
			return nil
		}
	}
	if len(vector) == 0 {
		return nil
	}
	if file == "" {
		return usagef("numeric dense query requires --embedding-space with verified query/document identities")
	}
	raw, err := os.ReadFile(file)
	if err != nil {
		return usagef("cannot read embedding-space receipt")
	}
	var receipt struct {
		Query    embeddingIdentity `json:"query"`
		Document embeddingIdentity `json:"document"`
		Verified bool              `json:"document_vectors_verified"`
		Manifest string            `json:"document_manifest_sha256"`
	}
	if err := json.Unmarshal(raw, &receipt); err != nil {
		return usagef("invalid embedding-space receipt")
	}
	q := receipt.Query
	if !receipt.Verified || len(receipt.Manifest) != 64 || q.Model == "" || q.Revision == "" || q.Normalization == "" || q.DistanceMetric == "" || q.Dims <= 0 {
		return usagef("embedding-space receipt requires verified document manifest and complete identities")
	}
	for _, c := range receipt.Manifest {
		if !strings.ContainsRune("0123456789abcdef", c) {
			return usagef("document manifest must be a SHA-256 hex digest")
		}
	}
	if q != receipt.Document {
		return usagef("query/document embedding spaces differ")
	}
	if len(vector) != q.Dims {
		return usagef("query vector dimension differs from embedding-space receipt")
	}
	for _, v := range vector {
		var n float64
		if err := json.Unmarshal(v, &n); err != nil || string(v) == "null" {
			return usagef("dense vector must contain finite numbers")
		}
	}
	return nil
}
