package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime/debug"
	"strings"

	"github.com/hev/layer/apps/layer-cli/internal/config"
	"github.com/hev/layer/apps/layer-cli/internal/output"
	hevlayer "github.com/hev/layer/clients/go"
	"github.com/spf13/cobra"
)

const (
	ExitOK     = 0
	ExitFailed = 1
	ExitUsage  = 2
)

const usageText = `layer drives hev layer indexes, environments, and UDFs from the command line.

Usage:
  layer [global flags] <command> [args]

Commands:
  ask <command>              Query the Layer docs digest
  browse                    Open the operations TUI
  env add|use|ls|show|rm    Manage named environments
  init NAMESPACE --shards N  Initialize namespace sharding
  index list                List indexes
  index get NAME            Show one index with snapshot history
  index delete NAME...      Delete indexes and their hevlayer state
  index delete --prefix P   Delete every index whose name starts with P
  install aws               Wizard: provision AWS (Terraform) and install the Helm release
  keys mint NAME            Mint an API key (token prints once on stdout)
  keys ls                   List API keys (metadata only, never tokens)
  keys get KEY_OR_ID        Show one API key's metadata
  keys revoke KEY_OR_ID     Revoke an API key, keeping the record
  keys rm KEY_OR_ID         Hard-delete an API key record
  pipeline list             List pipelines with live queue status
  pipeline get ID           Show one pipeline with live queue status
  run -f FUNCTION.yaml      Apply a Function CR and run it to drained
  udf list                  List registered UDFs with queue status
  udf get UDF_ID            Show one UDF's queue counts and discovery progress
  vectorstore list          List vector stores
  vectorstore get [NAME]    Show one vector store
  warehouse list            List warehouses
  warehouse get NAME        Show one warehouse
  version                   Print version

Global flags:
  --base-url URL    Gateway base URL (env LAYER_BASE_URL)
  --api-key KEY     API key (env LAYER_API_KEY)
  --env NAME        Select a named environment (env LAYER_ENV)
  -o, --output FMT  Output format: table, json, names (default table)
`

type Options struct {
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
	Env    map[string]string

	HomeDir          string
	StdinIsTerminal  bool
	StdoutIsTerminal bool
	Version          string
	LaunchTUI        func(context.Context, TUIOptions) error
	RunAsk           func(context.Context, []string, io.Reader, io.Writer, io.Writer) error
	AskDigestPath    string
}

type TUIOptions struct {
	HomeDir string
	Env     map[string]string
	Request config.ResolveRequest
}

type App struct {
	opts Options
}

type globalFlags struct {
	baseURL string
	apiKey  string
	envName string
	output  string
}

type runFlags struct {
	contextName   string
	kubeNamespace string
}

type cliError struct {
	message string
	code    int
}

func (err cliError) Error() string {
	return err.message
}

func Execute(ctx context.Context, args []string, opts Options) int {
	app := App{opts: opts.withDefaults()}
	return app.Run(ctx, args)
}

func (app App) Run(ctx context.Context, args []string) int {
	root := app.newRootCommand()
	root.SetContext(ctx)
	root.SetArgs(args)
	root.SetIn(app.opts.Stdin)
	root.SetOut(app.opts.Stdout)
	root.SetErr(app.opts.Stderr)
	if err := root.Execute(); err != nil {
		err = nudgeLicenseRequired(err)
		var usage cliError
		if errors.As(err, &usage) {
			fmt.Fprintln(app.opts.Stderr, usage.message)
			return usage.code
		}
		code := ExitFailed
		if isUsageError(err) {
			code = ExitUsage
		}
		fmt.Fprintln(app.opts.Stderr, err)
		return code
	}
	return ExitOK
}

func (app App) newRootCommand() *cobra.Command {
	flags := &globalFlags{output: output.Table}
	root := &cobra.Command{
		Use:           "layer [global flags] <command> [args]",
		Short:         "hevlayer operations CLI",
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) != 0 {
				return usagef("unknown command %q", args[0])
			}
			if app.opts.StdinIsTerminal && app.opts.StdoutIsTerminal {
				return app.launchTUI(cmd, flags)
			}
			fmt.Fprint(cmd.ErrOrStderr(), usageText)
			return cliError{message: "bare layer requires a TTY; use a subcommand for non-interactive operation", code: ExitUsage}
		},
	}
	root.CompletionOptions.DisableDefaultCmd = true
	root.PersistentFlags().StringVar(&flags.baseURL, "base-url", "", "Gateway base URL")
	root.PersistentFlags().StringVar(&flags.apiKey, "api-key", "", "API key")
	root.PersistentFlags().StringVar(&flags.envName, "env", "", "Named environment")
	root.PersistentFlags().StringVarP(&flags.output, "output", "o", output.Table, "Output format: table, json, names")
	root.SetHelpFunc(func(cmd *cobra.Command, args []string) {
		fmt.Fprint(cmd.OutOrStdout(), usageText)
	})
	root.SetUsageFunc(func(cmd *cobra.Command) error {
		fmt.Fprint(cmd.ErrOrStderr(), usageText)
		return nil
	})
	root.SetVersionTemplate(versionString(app.opts.Version) + "\n")
	root.Version = strings.TrimPrefix(versionString(app.opts.Version), "layer ")
	root.SetFlagErrorFunc(func(cmd *cobra.Command, err error) error {
		return cliError{message: err.Error(), code: ExitUsage}
	})

	root.AddCommand(newAskCommand(app, flags))
	root.AddCommand(newBrowseCommand(app, flags))
	root.AddCommand(newEnvCommand(app, flags))
	root.AddCommand(newInitCommand(app, flags))
	root.AddCommand(newIndexCommand(app, flags))
	root.AddCommand(newInstallCommand(app, flags))
	root.AddCommand(newKeysCommand(app, flags))
	root.AddCommand(newPipelineCommand(app, flags))
	root.AddCommand(newRunCommand(app, flags))
	root.AddCommand(newUdfCommand(app, flags))
	root.AddCommand(newVectorstoreCommand(app, flags))
	root.AddCommand(newWarehouseCommand(app, flags))
	root.AddCommand(newVersionCommand(app))
	return root
}

func (app App) resolve(cmd *cobra.Command, flags *globalFlags, run runFlags) (config.Resolved, string, error) {
	format, err := app.outputFormat(flags)
	if err != nil {
		return config.Resolved{}, format, err
	}
	req := config.ResolveRequest{
		EnvName:          flags.envName,
		EnvNameSet:       flagChanged(cmd, "env"),
		BaseURL:          flags.baseURL,
		BaseURLSet:       flagChanged(cmd, "base-url"),
		APIKey:           flags.apiKey,
		APIKeySet:        flagChanged(cmd, "api-key"),
		KubeContext:      run.contextName,
		KubeContextSet:   run.contextName != "",
		KubeNamespace:    run.kubeNamespace,
		KubeNamespaceSet: run.kubeNamespace != "",
	}
	resolved, err := config.Resolve(app.opts.HomeDir, app.opts.Env, req)
	if err != nil {
		return resolved, format, err
	}
	resolved, err = app.ensureCredentials(cmd, resolved)
	if err != nil {
		return resolved, format, err
	}
	return resolved, format, nil
}

func (app App) outputFormat(flags *globalFlags) (string, error) {
	format := flags.output
	if format == "" {
		format = output.Table
	}
	if err := output.Validate(format); err != nil {
		return format, cliError{message: err.Error(), code: ExitUsage}
	}
	return format, nil
}

func (app App) clientFor(resolved config.Resolved) *hevlayer.Client {
	return hevlayer.NewClient(
		hevlayer.WithBaseURL(resolved.BaseURL),
		hevlayer.WithAPIKey(resolved.APIKey),
	)
}

func (app App) launchTUI(cmd *cobra.Command, flags *globalFlags) error {
	if err := output.Validate(flags.output); err != nil {
		return cliError{message: err.Error(), code: ExitUsage}
	}
	req := config.ResolveRequest{
		EnvName:    flags.envName,
		EnvNameSet: flagChanged(cmd, "env"),
		BaseURL:    flags.baseURL,
		BaseURLSet: flagChanged(cmd, "base-url"),
		APIKey:     flags.apiKey,
		APIKeySet:  flagChanged(cmd, "api-key"),
	}
	// Onboard before the alt-screen takes over so the TUI opens against a
	// configured gateway instead of a wall of auth errors.
	resolved, err := config.Resolve(app.opts.HomeDir, app.opts.Env, req)
	if err != nil {
		return err
	}
	if resolved, err = app.ensureCredentials(cmd, resolved); err != nil {
		return err
	}
	if resolved.EnvName != "" {
		req.EnvName = resolved.EnvName
		req.EnvNameSet = true
	}
	launcher := app.opts.LaunchTUI
	if launcher == nil {
		launcher = launchBubbleTea
	}
	return launcher(cmd.Context(), TUIOptions{
		HomeDir: app.opts.HomeDir,
		Env:     app.opts.Env,
		Request: req,
	})
}

func newVersionCommand(app App) *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print version",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Fprintln(cmd.OutOrStdout(), versionString(app.opts.Version))
			return nil
		},
	}
}

func (opts Options) withDefaults() Options {
	if opts.Stdin == nil {
		opts.Stdin = os.Stdin
	}
	if opts.Stdout == nil {
		opts.Stdout = os.Stdout
	}
	if opts.Stderr == nil {
		opts.Stderr = os.Stderr
	}
	if opts.Env == nil {
		opts.Env = EnvironMap(os.Environ())
	}
	if opts.HomeDir == "" {
		opts.HomeDir = config.DefaultHomeDir()
	}
	if opts.Version == "" {
		opts.Version = "dev"
	}
	return opts
}

func flagChanged(cmd *cobra.Command, name string) bool {
	flag := cmd.Flag(name)
	return flag != nil && flag.Changed
}

func usagef(format string, args ...interface{}) error {
	return cliError{message: fmt.Sprintf(format, args...), code: ExitUsage}
}

func isUsageError(err error) bool {
	message := err.Error()
	return strings.Contains(message, "unknown command") ||
		strings.Contains(message, "unknown flag") ||
		strings.Contains(message, "requires at least") ||
		strings.Contains(message, "accepts ") ||
		strings.Contains(message, "arg(s)")
}

func versionString(version string) string {
	value := version
	if value == "" {
		value = "dev"
	}
	if info, ok := debug.ReadBuildInfo(); ok {
		if value == "dev" && info.Main.Version != "" && info.Main.Version != "(devel)" {
			value = info.Main.Version
		}
		for _, setting := range info.Settings {
			if setting.Key == "vcs.revision" && len(setting.Value) >= 7 {
				return fmt.Sprintf("layer %s (%s)", value, setting.Value[:7])
			}
		}
	}
	return "layer " + value
}

func EnvironMap(values []string) map[string]string {
	out := make(map[string]string, len(values))
	for _, item := range values {
		key, value, ok := strings.Cut(item, "=")
		if ok {
			out[key] = value
		}
	}
	return out
}
