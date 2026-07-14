package cmd

import (
	"bufio"
	"fmt"
	"io"
	"strings"

	"github.com/hev/layer/apps/layer-cli/internal/config"
	hevlayer "github.com/hev/layer/clients/go"
	"github.com/spf13/cobra"
)

// onboardEnvName is the environment created the first time someone runs a
// gateway command without any credentials configured.
const onboardEnvName = "default"

// missingCredentialsHelp is shown when no API key is configured and there is no
// terminal to prompt on (CI, pipes, scripts).
const missingCredentialsHelp = `No API key configured.

Supply credentials in one of these ways:
  layer env add NAME              save a reusable environment (prompts for URL + key)
  --base-url URL --api-key KEY    pass them as flags
  LAYER_BASE_URL / LAYER_API_KEY  set them in the environment

Run layer from an interactive terminal to be walked through setup.`

// ensureCredentials guarantees the resolved config carries an API key. When one
// is missing and a terminal is attached, it walks the operator through
// supplying a gateway URL and API key, saves them as a named environment, and
// returns the updated resolution. Without a terminal it returns an actionable
// usage error instead of letting the request fail later with an opaque 401.
func (app App) ensureCredentials(cmd *cobra.Command, resolved config.Resolved) (config.Resolved, error) {
	if strings.TrimSpace(resolved.APIKey) != "" {
		return resolved, nil
	}
	if !app.opts.StdinIsTerminal || !app.opts.StdoutIsTerminal {
		return resolved, cliError{message: missingCredentialsHelp, code: ExitUsage}
	}

	errOut := cmd.ErrOrStderr()
	reader := bufio.NewReader(app.opts.Stdin)

	fmt.Fprintln(errOut, "No API key configured yet — let's set up hev layer.")
	fmt.Fprintln(errOut)

	defaultBase := strings.TrimSpace(resolved.BaseURL)
	if defaultBase == "" {
		defaultBase = hevlayer.DefaultBaseURL
	}
	answer, err := promptLineW(errOut, reader, fmt.Sprintf("Gateway base URL [%s]: ", defaultBase))
	if err != nil {
		return resolved, err
	}
	baseURL := strings.TrimSpace(answer)
	if baseURL == "" {
		baseURL = defaultBase
	}

	answer, err = promptLineW(errOut, reader, "API key: ")
	if err != nil {
		return resolved, err
	}
	apiKey := strings.TrimSpace(answer)
	if apiKey == "" {
		return resolved, cliError{message: "an API key is required to reach the gateway", code: ExitUsage}
	}

	// Update the selected environment in place when one was named but lacked a
	// key; otherwise create (and activate) the default environment. Loading the
	// existing record first preserves any kube settings already stored on it.
	name := strings.TrimSpace(resolved.EnvName)
	if name == "" {
		name = onboardEnvName
	}
	env, _, err := config.GetEnv(app.opts.HomeDir, name)
	if err != nil {
		return resolved, err
	}
	env.BaseURL = baseURL
	env.APIKey = apiKey
	if err := config.AddEnv(app.opts.HomeDir, name, env); err != nil {
		return resolved, fmt.Errorf("save credentials: %w", err)
	}
	if err := config.SetActive(app.opts.HomeDir, name); err != nil {
		return resolved, fmt.Errorf("activate environment %q: %w", name, err)
	}

	fmt.Fprintf(errOut, "\nSaved environment %q. Manage it later with `layer env`.\n", name)
	client := hevlayer.NewClient(hevlayer.WithBaseURL(baseURL), hevlayer.WithAPIKey(apiKey))
	if license, licenseErr := client.GetLicense(cmd.Context()); licenseErr == nil && license.Gateway.State == "floor" {
		fmt.Fprintf(errOut, "\n%s\n", openGatewayNudge)
	}
	fmt.Fprintln(errOut)

	resolved.EnvName = name
	resolved.BaseURL = baseURL
	resolved.APIKey = apiKey
	resolved.ConfigUsed = true
	return resolved, nil
}

func promptLineW(w io.Writer, reader *bufio.Reader, prompt string) (string, error) {
	fmt.Fprint(w, prompt)
	value, err := reader.ReadString('\n')
	if err != nil && value == "" {
		return "", err
	}
	return value, nil
}
