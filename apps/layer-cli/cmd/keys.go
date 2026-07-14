package cmd

import (
	"context"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/hev/layer/apps/layer-cli/internal/config"
	"github.com/hev/layer/apps/layer-cli/internal/keys"
	"github.com/hev/layer/apps/layer-cli/internal/output"
	hevlayer "github.com/hev/layer/clients/go"
	"github.com/spf13/cobra"
	"sigs.k8s.io/yaml"
)

const (
	apiKeyAPIVersion    = "hevlayer.com/v1alpha1"
	defaultExpiresAfter = "365d"
)

func newKeysCommand(app App, flags *globalFlags) *cobra.Command {
	keysCmd := &cobra.Command{
		Use:   "keys",
		Short: "Mint, list, revoke, and delete gateway API keys",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return usagef("usage: layer keys <mint|ls|get|revoke|rm> ...")
		},
	}
	keysCmd.AddCommand(newKeysMintCommand(app, flags))
	keysCmd.AddCommand(newKeysListCommand(app, flags))
	keysCmd.AddCommand(newKeysGetCommand(app, flags))
	keysCmd.AddCommand(newKeysRevokeCommand(app, flags))
	keysCmd.AddCommand(newKeysRmCommand(app, flags))
	return keysCmd
}

// keysClientFor builds the wire-faithful key-route client from the same
// resolved environment the generated client uses.
func (app App) keysClientFor(resolved config.Resolved) *keys.Client {
	return keys.NewClient(resolved.BaseURL, resolved.APIKey)
}

type mintFlags struct {
	filename     string
	owner        string
	description  string
	namespaces   string
	expiresAfter string
	entitles     []string
	claims       []string
}

func newKeysMintCommand(app App, flags *globalFlags) *cobra.Command {
	var mint mintFlags
	cmd := &cobra.Command{
		Use:   "mint NAME | mint -f KEY.yaml",
		Short: "Mint an API key; the token prints once, alone on stdout",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			resolved, format, err := app.resolve(cmd, flags, runFlags{})
			if err != nil {
				return err
			}
			req, err := buildMintRequest(cmd, args, mint)
			if err != nil {
				return err
			}
			minted, err := app.keysClientFor(resolved).Mint(cmd.Context(), req)
			if err != nil {
				return err
			}
			// Metadata goes to stderr; the raw token prints once, alone on
			// stdout, so `layer keys mint ... | pbcopy` captures only the token.
			if err := emitKeyDetail(cmd.ErrOrStderr(), format, minted.Key); err != nil {
				return err
			}
			fmt.Fprintln(cmd.OutOrStdout(), minted.Token)
			return nil
		},
	}
	cmd.Flags().StringVarP(&mint.filename, "filename", "f", "", "ApiKey CRD manifest to mint through the gateway")
	cmd.Flags().StringVar(&mint.owner, "owner", "", "Free-form owner label")
	cmd.Flags().StringVar(&mint.description, "description", "", "Free-text description")
	cmd.Flags().StringArrayVar(&mint.entitles, "entitle", nil, "Entitlement TARGET[=SCOPE[+SCOPE]]; targets are vectorstore.<name>, warehouse.<name>, agent.<name>, layer")
	cmd.Flags().StringVar(&mint.namespaces, "namespaces", "", "Comma-separated upstream-namespace globs for the vectorstore entitlement")
	cmd.Flags().StringArrayVar(&mint.claims, "claim", nil, "Opaque claim TARGET=STRING appended to that target's entitlement")
	cmd.Flags().StringVar(&mint.expiresAfter, "expires-after", defaultExpiresAfter, "Duration or never")
	return cmd
}

// buildMintRequest turns either the flag surface or an ApiKey manifest into
// the gateway mint request. With -f the manifest supplies name, owner,
// description, and entitlements; only --expires-after may override it.
func buildMintRequest(cmd *cobra.Command, args []string, mint mintFlags) (*keys.MintRequest, error) {
	if mint.filename != "" {
		for _, name := range []string{"owner", "description", "entitle", "claim", "namespaces"} {
			if flagChanged(cmd, name) {
				return nil, usagef("layer keys mint -f takes %s from the manifest; only --expires-after may override", "--"+name)
			}
		}
		if len(args) > 0 {
			return nil, usagef("layer keys mint -f takes the key name from the manifest")
		}
		req, err := loadApiKeyManifest(mint.filename)
		if err != nil {
			return nil, err
		}
		if flagChanged(cmd, "expires-after") {
			req.ExpiresAfter = mint.expiresAfter
		}
		if req.ExpiresAfter == "" {
			req.ExpiresAfter = defaultExpiresAfter
		}
		return req, nil
	}
	if len(args) == 0 {
		return nil, usagef("usage: layer keys mint NAME [flags] | layer keys mint -f KEY.yaml")
	}
	entitlements, err := parseEntitlements(mint.entitles, mint.claims, mint.namespaces)
	if err != nil {
		return nil, usagef("%s", err)
	}
	return &keys.MintRequest{
		Name:         args[0],
		Owner:        mint.owner,
		Description:  mint.description,
		Entitlements: entitlements,
		ExpiresAfter: mint.expiresAfter,
	}, nil
}

// parseEntitlements folds the --entitle, --claim, and --namespaces flags into
// the entitlements map keyed by target. Repeated flags for one target merge;
// the namespace globs attach to the single vectorstore entitlement.
func parseEntitlements(entitles []string, claims []string, namespaces string) (keys.Entitlements, error) {
	entitlements := keys.Entitlements{}
	for _, spec := range entitles {
		target, scopes, err := parseEntitleFlag(spec)
		if err != nil {
			return nil, err
		}
		entitlement := entitlements[target]
		for _, scope := range scopes {
			if !containsString(entitlement.Scopes, scope) {
				entitlement.Scopes = append(entitlement.Scopes, scope)
			}
		}
		entitlements[target] = entitlement
	}
	for _, spec := range claims {
		target, claim, ok := strings.Cut(spec, "=")
		target = strings.TrimSpace(target)
		if !ok || claim == "" {
			return nil, fmt.Errorf("--claim %q must be TARGET=STRING", spec)
		}
		if err := validateEntitlementTarget(target); err != nil {
			return nil, fmt.Errorf("--claim %q: %w", spec, err)
		}
		entitlement := entitlements[target]
		entitlement.Claims = append(entitlement.Claims, claim)
		entitlements[target] = entitlement
	}
	if globs := splitCommaList(namespaces); len(globs) > 0 {
		var stores []string
		for target := range entitlements {
			if strings.HasPrefix(target, "vectorstore.") {
				stores = append(stores, target)
			}
		}
		if len(stores) == 0 {
			return nil, fmt.Errorf("--namespaces requires a vectorstore entitlement (--entitle vectorstore.<name>=...)")
		}
		if len(stores) > 1 {
			return nil, fmt.Errorf("--namespaces is ambiguous across %d vectorstore entitlements; write the manifest and use -f", len(stores))
		}
		entitlement := entitlements[stores[0]]
		entitlement.Namespaces = globs
		entitlements[stores[0]] = entitlement
	}
	if len(entitlements) == 0 {
		return nil, nil
	}
	return entitlements, nil
}

// parseEntitleFlag splits TARGET[=SCOPE[+SCOPE]]. A bare target carries no
// scopes (a claims-only entitlement authenticates but opens no Layer route).
func parseEntitleFlag(spec string) (string, []string, error) {
	target, scopePart, hasScopes := strings.Cut(spec, "=")
	target = strings.TrimSpace(target)
	if err := validateEntitlementTarget(target); err != nil {
		return "", nil, fmt.Errorf("--entitle %q: %w", spec, err)
	}
	if !hasScopes {
		return target, nil, nil
	}
	if strings.HasPrefix(target, "agent.") {
		return "", nil, fmt.Errorf("--entitle %q: agent invocation uses an empty entitlement object; drop scopes", spec)
	}
	var scopes []string
	for _, scope := range strings.Split(scopePart, "+") {
		scope = strings.TrimSpace(scope)
		if scope == "" {
			return "", nil, fmt.Errorf("--entitle %q has an empty scope", spec)
		}
		scopes = append(scopes, scope)
	}
	return target, scopes, nil
}

func validateEntitlementTarget(target string) error {
	if target == "layer" {
		return nil
	}
	for _, prefix := range []string{"vectorstore.", "warehouse.", "agent."} {
		if strings.HasPrefix(target, prefix) {
			if strings.TrimSpace(strings.TrimPrefix(target, prefix)) == "" {
				return fmt.Errorf("entitlement target %q is missing a resource name", target)
			}
			return nil
		}
	}
	return fmt.Errorf("entitlement target %q must be vectorstore.<name>, warehouse.<name>, agent.<name>, or layer", target)
}

func splitCommaList(value string) []string {
	var out []string
	for _, item := range strings.Split(value, ",") {
		if item = strings.TrimSpace(item); item != "" {
			out = append(out, item)
		}
	}
	return out
}

func containsString(values []string, value string) bool {
	for _, item := range values {
		if item == value {
			return true
		}
	}
	return false
}

// loadApiKeyManifest reads an ApiKey CRD manifest and maps spec fields onto
// the gateway mint request; both surfaces round-trip through one schema.
func loadApiKeyManifest(filename string) (*keys.MintRequest, error) {
	raw, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	var manifest struct {
		APIVersion string `json:"apiVersion"`
		Kind       string `json:"kind"`
		Metadata   struct {
			Name string `json:"name"`
		} `json:"metadata"`
		Spec struct {
			Owner        string            `json:"owner"`
			Description  string            `json:"description"`
			Entitlements keys.Entitlements `json:"entitlements"`
			ExpiresAfter string            `json:"expiresAfter"`
		} `json:"spec"`
	}
	if err := yaml.Unmarshal(raw, &manifest); err != nil {
		return nil, err
	}
	if manifest.Kind != "ApiKey" {
		return nil, fmt.Errorf("layer keys mint -f requires kind: ApiKey, got %q", manifest.Kind)
	}
	if manifest.APIVersion != apiKeyAPIVersion {
		return nil, fmt.Errorf("layer keys mint -f requires apiVersion: %s, got %q", apiKeyAPIVersion, manifest.APIVersion)
	}
	if manifest.Metadata.Name == "" {
		return nil, fmt.Errorf("metadata.name is required on the ApiKey manifest")
	}
	for target := range manifest.Spec.Entitlements {
		if err := validateEntitlementTarget(target); err != nil {
			return nil, err
		}
	}
	return &keys.MintRequest{
		Name:         manifest.Metadata.Name,
		Owner:        manifest.Spec.Owner,
		Description:  manifest.Spec.Description,
		Entitlements: manifest.Spec.Entitlements,
		ExpiresAfter: manifest.Spec.ExpiresAfter,
	}, nil
}

func newKeysListCommand(app App, flags *globalFlags) *cobra.Command {
	var includeRevoked bool
	cmd := &cobra.Command{
		Use:     "ls",
		Aliases: []string{"list"},
		Short:   "List API keys — metadata only, never tokens",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			resolved, format, err := app.resolve(cmd, flags, runFlags{})
			if err != nil {
				return err
			}
			list, err := app.keysClientFor(resolved).List(cmd.Context(), includeRevoked)
			if err != nil {
				return err
			}
			sort.Slice(list, func(i, j int) bool {
				return list[i].Name < list[j].Name
			})
			return emitKeys(cmd.OutOrStdout(), format, list)
		},
	}
	cmd.Flags().BoolVar(&includeRevoked, "include-revoked", false, "Include revoked and expired keys")
	return cmd
}

func newKeysGetCommand(app App, flags *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "get KEY_OR_ID",
		Short: "Show one API key's metadata",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			resolved, format, err := app.resolve(cmd, flags, runFlags{})
			if err != nil {
				return err
			}
			key, err := resolveKey(cmd.Context(), app.keysClientFor(resolved), args[0])
			if err != nil {
				return err
			}
			return emitKeyDetail(cmd.OutOrStdout(), format, key)
		},
	}
}

func newKeysRevokeCommand(app App, flags *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "revoke KEY_OR_ID",
		Short: "Revoke an API key, keeping the record",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			resolved, format, err := app.resolve(cmd, flags, runFlags{})
			if err != nil {
				return err
			}
			key, err := resolveKey(cmd.Context(), app.keysClientFor(resolved), args[0])
			if err != nil {
				return err
			}
			if _, err := app.clientFor(resolved).RevokeKey(cmd.Context(), key.KeyID); err != nil {
				return err
			}
			return emitKeyAction(cmd.OutOrStdout(), format, key, "revoked")
		},
	}
}

func newKeysRmCommand(app App, flags *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "rm KEY_OR_ID",
		Short: "Hard-delete an API key record",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			resolved, format, err := app.resolve(cmd, flags, runFlags{})
			if err != nil {
				return err
			}
			key, err := resolveKey(cmd.Context(), app.keysClientFor(resolved), args[0])
			if err != nil {
				return err
			}
			if _, err := app.clientFor(resolved).DeleteKey(cmd.Context(), key.KeyID); err != nil {
				return err
			}
			return emitKeyAction(cmd.OutOrStdout(), format, key, "deleted")
		},
	}
}

// resolveKey accepts a keyId or a key name. Names are unique per install, so
// one revoked-inclusive listing resolves either spelling.
func resolveKey(ctx context.Context, client *keys.Client, ref string) (keys.Key, error) {
	list, err := client.List(ctx, true)
	if err != nil {
		return keys.Key{}, err
	}
	for _, key := range list {
		if key.KeyID == ref {
			return key, nil
		}
	}
	for _, key := range list {
		if key.Name == ref {
			return key, nil
		}
	}
	return keys.Key{}, fmt.Errorf("key %q not found", ref)
}

// entitlementTargets returns a key's entitlement targets, sorted.
func entitlementTargets(key keys.Key) []string {
	targets := make([]string, 0, len(key.Entitlements))
	for target := range key.Entitlements {
		targets = append(targets, target)
	}
	sort.Strings(targets)
	return targets
}

func emitKeys(out io.Writer, format string, list []keys.Key) error {
	switch format {
	case output.JSON:
		return output.WriteJSON(out, list)
	case output.Names:
		for _, key := range list {
			fmt.Fprintln(out, key.KeyID)
		}
		return nil
	default:
		tableRows := make([][]string, 0, len(list))
		for _, key := range list {
			tableRows = append(tableRows, []string{
				key.KeyID,
				key.Name,
				dash(key.Owner),
				dash(key.Phase),
				dash(strings.Join(entitlementTargets(key), ",")),
				dash(key.ExpiresAt),
				dash(key.LastSeenAt),
			})
		}
		return output.WriteTable(out, []string{"KEY_ID", "NAME", "OWNER", "PHASE", "ENTITLEMENTS", "EXPIRES_AT", "LAST_SEEN_AT"}, tableRows)
	}
}

func emitKeyDetail(out io.Writer, format string, key keys.Key) error {
	switch format {
	case output.JSON:
		return output.WriteJSON(out, key)
	case output.Names:
		fmt.Fprintln(out, key.KeyID)
		return nil
	default:
		rows := [][]string{
			{"KEY_ID", key.KeyID},
			{"NAME", key.Name},
			{"OWNER", dash(key.Owner)},
			{"DESCRIPTION", dash(key.Description)},
			{"PHASE", dash(key.Phase)},
			{"CREATED_AT", dash(key.CreatedAt)},
			{"EXPIRES_AT", dash(key.ExpiresAt)},
		}
		if key.RevokedAt != "" {
			rows = append(rows, []string{"REVOKED_AT", key.RevokedAt})
		}
		rows = append(rows, []string{"LAST_SEEN_AT", dash(key.LastSeenAt)})
		for _, target := range entitlementTargets(key) {
			rows = append(rows, []string{"ENTITLEMENT " + target, formatEntitlement(key.Entitlements[target])})
		}
		return output.WriteFields(out, rows)
	}
}

// formatEntitlement renders one target's grant as scopes/namespaces/claims
// segments, omitting empty ones.
func formatEntitlement(entitlement hevlayer.ApiKeyEntitlement) string {
	var parts []string
	if len(entitlement.Scopes) > 0 {
		parts = append(parts, "scopes="+strings.Join(entitlement.Scopes, "+"))
	}
	if len(entitlement.Namespaces) > 0 {
		parts = append(parts, "namespaces="+strings.Join(entitlement.Namespaces, ","))
	}
	if len(entitlement.Claims) > 0 {
		parts = append(parts, "claims="+strings.Join(entitlement.Claims, ","))
	}
	if len(parts) == 0 {
		return "—"
	}
	return strings.Join(parts, " ")
}

func emitKeyAction(out io.Writer, format string, key keys.Key, status string) error {
	switch format {
	case output.JSON:
		return output.WriteJSON(out, struct {
			KeyID  string `json:"key_id"`
			Name   string `json:"name"`
			Status string `json:"status"`
		}{KeyID: key.KeyID, Name: key.Name, Status: status})
	case output.Names:
		fmt.Fprintln(out, key.KeyID)
		return nil
	default:
		fmt.Fprintf(out, "%s\t%s\t%s\n", key.KeyID, key.Name, status)
		return nil
	}
}
