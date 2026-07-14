package cmd

import (
	"bufio"
	"fmt"
	"io"
	"strings"

	"github.com/hev/layer/apps/layer-cli/internal/config"
	"github.com/hev/layer/apps/layer-cli/internal/output"
	hevlayer "github.com/hev/layer/clients/go"
	"github.com/spf13/cobra"
)

type envRow struct {
	Active        bool   `json:"active"`
	Name          string `json:"name"`
	BaseURL       string `json:"base_url"`
	APIKey        string `json:"api_key"`
	KubeContext   string `json:"kube_context,omitempty"`
	KubeNamespace string `json:"kube_namespace,omitempty"`
}

func newEnvCommand(app App, flags *globalFlags) *cobra.Command {
	envCmd := &cobra.Command{
		Use:   "env",
		Short: "Manage named environments",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return usagef("usage: layer env <add|use|ls|show|rm> ...")
		},
	}
	envCmd.AddCommand(newEnvAddCommand(app, flags))
	envCmd.AddCommand(newEnvUseCommand(app))
	envCmd.AddCommand(newEnvListCommand(app, flags))
	envCmd.AddCommand(newEnvShowCommand(app, flags))
	envCmd.AddCommand(newEnvRemoveCommand(app))
	return envCmd
}

func newEnvAddCommand(app App, flags *globalFlags) *cobra.Command {
	var kubeContext string
	var kubeNamespace string
	cmd := &cobra.Command{
		Use:   "add NAME",
		Short: "Add or update an environment",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			env, err := app.readEnvAdd(cmd, flags, kubeContext, kubeNamespace)
			if err != nil {
				return err
			}
			if err := config.AddEnv(app.opts.HomeDir, args[0], env); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Environment %q saved.\n", args[0])
			return nil
		},
	}
	cmd.Flags().StringVar(&kubeContext, "kube-context", "", "Kubernetes context for Function applies")
	cmd.Flags().StringVar(&kubeNamespace, "kube-namespace", "", "Kubernetes namespace for Function applies")
	return cmd
}

func newEnvUseCommand(app App) *cobra.Command {
	return &cobra.Command{
		Use:   "use NAME",
		Short: "Switch the active environment",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := config.SetActive(app.opts.HomeDir, args[0]); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Switched to environment %q.\n", args[0])
			return nil
		},
	}
}

func newEnvListCommand(app App, flags *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:     "ls",
		Aliases: []string{"list"},
		Short:   "List environments",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			format, err := app.outputFormat(flags)
			if err != nil {
				return err
			}
			entries, err := config.ListEnvs(app.opts.HomeDir)
			if err != nil {
				return err
			}
			rows := make([]envRow, 0, len(entries))
			for _, entry := range entries {
				rows = append(rows, envRow{
					Active:        entry.IsActive,
					Name:          entry.Name,
					BaseURL:       entry.Config.BaseURL,
					APIKey:        config.MaskKey(entry.Config.APIKey),
					KubeContext:   entry.Config.KubeContext,
					KubeNamespace: entry.Config.KubeNamespace,
				})
			}
			return emitEnvRows(cmd.OutOrStdout(), format, rows)
		},
	}
}

func newEnvShowCommand(app App, flags *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "show [NAME]",
		Short: "Show one environment",
		Args:  cobra.RangeArgs(0, 1),
		RunE: func(cmd *cobra.Command, args []string) error {
			format, err := app.outputFormat(flags)
			if err != nil {
				return err
			}
			cfg, err := config.Load(app.opts.HomeDir)
			if err != nil {
				return err
			}
			name := ""
			if len(args) == 1 {
				name = args[0]
			} else {
				name = cfg.Active
			}
			if name == "" {
				return usagef("no active environment")
			}
			env, ok := cfg.Envs[name]
			if !ok {
				return fmt.Errorf("environment %q not found", name)
			}
			row := envRow{
				Active:        name == cfg.Active,
				Name:          name,
				BaseURL:       env.BaseURL,
				APIKey:        config.MaskKey(env.APIKey),
				KubeContext:   env.KubeContext,
				KubeNamespace: env.KubeNamespace,
			}
			if format == output.JSON {
				return output.WriteJSON(cmd.OutOrStdout(), row)
			}
			if format == output.Names {
				fmt.Fprintln(cmd.OutOrStdout(), row.Name)
				return nil
			}
			return emitEnvRows(cmd.OutOrStdout(), output.Table, []envRow{row})
		},
	}
}

func newEnvRemoveCommand(app App) *cobra.Command {
	return &cobra.Command{
		Use:     "rm NAME",
		Aliases: []string{"remove"},
		Short:   "Remove an environment",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := config.RemoveEnv(app.opts.HomeDir, args[0]); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Environment %q removed.\n", args[0])
			return nil
		},
	}
}

func (app App) readEnvAdd(cmd *cobra.Command, flags *globalFlags, kubeContext string, kubeNamespace string) (config.EnvConfig, error) {
	baseURL := ""
	if flagChanged(cmd, "base-url") {
		baseURL = strings.TrimSpace(flags.baseURL)
	}
	apiKey := ""
	if flagChanged(cmd, "api-key") {
		apiKey = strings.TrimSpace(flags.apiKey)
	}
	if !app.opts.StdinIsTerminal {
		if baseURL == "" {
			return config.EnvConfig{}, usagef("usage: layer env add NAME --base-url URL --api-key KEY [--kube-context CONTEXT] [--kube-namespace NAMESPACE]")
		}
		if apiKey == "" {
			return config.EnvConfig{}, usagef("usage: layer env add NAME --base-url URL --api-key KEY [--kube-context CONTEXT] [--kube-namespace NAMESPACE]")
		}
		return config.EnvConfig{
			BaseURL:       baseURL,
			APIKey:        apiKey,
			KubeContext:   kubeContext,
			KubeNamespace: kubeNamespace,
		}, nil
	}

	reader := bufio.NewReader(app.opts.Stdin)
	if baseURL == "" {
		prompt := fmt.Sprintf("Gateway base URL [%s]: ", hevlayer.DefaultBaseURL)
		answer, err := promptLine(cmd, reader, prompt)
		if err != nil {
			return config.EnvConfig{}, err
		}
		baseURL = strings.TrimSpace(answer)
		if baseURL == "" {
			baseURL = hevlayer.DefaultBaseURL
		}
	}
	if apiKey == "" {
		answer, err := promptLine(cmd, reader, "API key: ")
		if err != nil {
			return config.EnvConfig{}, err
		}
		apiKey = strings.TrimSpace(answer)
	}
	if apiKey == "" {
		return config.EnvConfig{}, usagef("api key is required")
	}
	if kubeContext == "" {
		answer, err := promptLine(cmd, reader, "Kube context (optional): ")
		if err != nil {
			return config.EnvConfig{}, err
		}
		kubeContext = strings.TrimSpace(answer)
	}
	if kubeNamespace == "" {
		answer, err := promptLine(cmd, reader, "Kube namespace (optional): ")
		if err != nil {
			return config.EnvConfig{}, err
		}
		kubeNamespace = strings.TrimSpace(answer)
	}
	return config.EnvConfig{
		BaseURL:       baseURL,
		APIKey:        apiKey,
		KubeContext:   kubeContext,
		KubeNamespace: kubeNamespace,
	}, nil
}

func promptLine(cmd *cobra.Command, reader *bufio.Reader, prompt string) (string, error) {
	return promptLineW(cmd.OutOrStdout(), reader, prompt)
}

func emitEnvRows(out io.Writer, format string, rows []envRow) error {
	switch format {
	case output.JSON:
		return output.WriteJSON(out, rows)
	case output.Names:
		for _, row := range rows {
			fmt.Fprintln(out, row.Name)
		}
		return nil
	default:
		tableRows := make([][]string, 0, len(rows))
		for _, row := range rows {
			active := ""
			if row.Active {
				active = "*"
			}
			tableRows = append(tableRows, []string{
				active,
				row.Name,
				row.BaseURL,
				row.APIKey,
				row.KubeContext,
				row.KubeNamespace,
			})
		}
		return output.WriteTable(out, []string{"", "NAME", "BASE_URL", "API_KEY", "KUBE_CONTEXT", "KUBE_NAMESPACE"}, tableRows)
	}
}
