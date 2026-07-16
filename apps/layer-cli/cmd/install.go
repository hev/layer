package cmd

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"

	"github.com/spf13/cobra"
)

// installSourceHelp is shown when the command cannot find the deployment
// assets (Terraform, Helm chart, install script) it drives.
const installSourceHelp = `layer install aws runs from a hev/layer source checkout.

Clone the repository and run the command from inside it:

  git clone https://github.com/hev/layer
  cd layer
  layer install aws

Or point --source (or LAYER_SRC) at an existing checkout.`

// defaultSystemNodeType is the wizard default for the always-on system node.
// i4i instances carry NVMe instance store, which the serving path and
// document cache share; see https://hevlayer.com/docs/install/.
const defaultSystemNodeType = "i4i.large"

var systemNodeTypeChoices = []struct{ name, note string }{
	{"i4i.large", "default; NVMe instance store, cost-efficient baseline"},
	{"i4i.xlarge", "more cache and serving headroom"},
	{"i4i.2xlarge", "large document caches / higher query volume"},
	{"m6a.large", "no instance store; not recommended for serving"},
}

type installAWSOptions struct {
	source        string
	profile       string
	region        string
	clusterName   string
	namespace     string
	nodeType      string
	tpufKey       string
	layerVersion  string
	licenseToken  string
	skipTerraform bool
	assumeYes     bool
	dryRun        bool
}

func newInstallCommand(app App, flags *globalFlags) *cobra.Command {
	installCmd := &cobra.Command{
		Use:   "install",
		Short: "Install a Layer environment",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return usagef("usage: layer install aws")
		},
	}
	installCmd.AddCommand(newInstallAWSCommand(app))
	return installCmd
}

func newInstallAWSCommand(app App) *cobra.Command {
	opts := installAWSOptions{}
	cmd := &cobra.Command{
		Use:   "aws",
		Short: "Provision AWS (Terraform) and install the Helm release",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return app.runInstallAWS(cmd, opts)
		},
	}
	cmd.Flags().StringVar(&opts.source, "source", "", "Path to a hev/layer checkout (env LAYER_SRC; default: walk up from the working directory)")
	cmd.Flags().StringVar(&opts.profile, "profile", "", "AWS profile to install with (env AWS_PROFILE)")
	cmd.Flags().StringVar(&opts.region, "region", "", "AWS region (default us-east-1)")
	cmd.Flags().StringVar(&opts.clusterName, "cluster-name", "", "EKS cluster name (default layer)")
	cmd.Flags().StringVar(&opts.namespace, "namespace", "", "Kubernetes namespace for the release (default layer)")
	cmd.Flags().StringVar(&opts.nodeType, "node-type", "", "Base (system) node instance type (default "+defaultSystemNodeType+")")
	cmd.Flags().StringVar(&opts.tpufKey, "turbopuffer-api-key", "", "Upstream store credential (env TURBOPUFFER_API_KEY)")
	cmd.Flags().StringVar(&opts.layerVersion, "version", "", "Layer image tag (default latest)")
	cmd.Flags().StringVar(&opts.licenseToken, "license-token", "", "Signed hev layer license key (env LICENSE_TOKEN)")
	cmd.Flags().BoolVar(&opts.skipTerraform, "skip-terraform", false, "Reuse existing Terraform outputs; run cluster components and Helm only")
	cmd.Flags().BoolVarP(&opts.assumeYes, "yes", "y", false, "Skip the confirmation prompt")
	cmd.Flags().BoolVar(&opts.dryRun, "dry-run", false, "Print the resolved plan and the command it would run, then exit")
	return cmd
}

func (app App) runInstallAWS(cmd *cobra.Command, opts installAWSOptions) error {
	errOut := cmd.ErrOrStderr()
	interactive := app.opts.StdinIsTerminal && app.opts.StdoutIsTerminal
	reader := bufio.NewReader(app.opts.Stdin)

	source, err := app.resolveInstallSource(opts.source)
	if err != nil {
		return err
	}

	// Defaults from the environment first, flags already win by being set.
	if opts.profile == "" {
		opts.profile = strings.TrimSpace(app.opts.Env["AWS_PROFILE"])
	}
	if opts.region == "" {
		opts.region = strings.TrimSpace(app.opts.Env["AWS_REGION"])
	}
	if opts.tpufKey == "" {
		opts.tpufKey = strings.TrimSpace(app.opts.Env["TURBOPUFFER_API_KEY"])
	}
	if opts.licenseToken == "" {
		opts.licenseToken = strings.TrimSpace(app.opts.Env["LICENSE_TOKEN"])
	}

	if interactive {
		if err := app.installWizard(cmd, reader, &opts); err != nil {
			return err
		}
	}
	if opts.region == "" {
		opts.region = "us-east-1"
	}
	if opts.clusterName == "" {
		opts.clusterName = "layer"
	}
	if opts.namespace == "" {
		opts.namespace = "layer"
	}
	if opts.nodeType == "" {
		opts.nodeType = defaultSystemNodeType
	}
	if opts.layerVersion == "" {
		opts.layerVersion = "latest"
	}
	if opts.tpufKey == "" {
		return cliError{message: "a Turbopuffer API key is required: pass --turbopuffer-api-key or set TURBOPUFFER_API_KEY", code: ExitUsage}
	}

	fmt.Fprintf(errOut, "\nInstall plan\n")
	fmt.Fprintf(errOut, "  Source checkout   %s\n", source)
	fmt.Fprintf(errOut, "  AWS profile       %s\n", orLabel(opts.profile, "(ambient credentials)"))
	fmt.Fprintf(errOut, "  Region            %s\n", opts.region)
	fmt.Fprintf(errOut, "  Cluster           %s\n", opts.clusterName)
	fmt.Fprintf(errOut, "  Namespace         %s\n", opts.namespace)
	fmt.Fprintf(errOut, "  Base node type    %s\n", opts.nodeType)
	fmt.Fprintf(errOut, "  Layer version     %s\n", opts.layerVersion)
	fmt.Fprintf(errOut, "  License token     %s\n", orLabel(maskSecret(opts.licenseToken), "(none — open gateway floor)"))
	fmt.Fprintf(errOut, "  Terraform         %s\n", installStageLabel(opts.skipTerraform))
	fmt.Fprintln(errOut)

	env := installScriptEnv(app.opts.Env, opts)
	script := filepath.Join(source, "scripts", "install-layer.sh")

	if opts.dryRun {
		out := cmd.OutOrStdout()
		fmt.Fprintln(out, "dry run — would execute:")
		for _, kv := range installEnvForDisplay(opts) {
			fmt.Fprintf(out, "  %s \\\n", kv)
		}
		fmt.Fprintf(out, "  %s\n", script)
		return nil
	}

	if !opts.assumeYes {
		if !interactive {
			return cliError{message: "refusing to provision without confirmation; pass --yes for non-interactive installs", code: ExitUsage}
		}
		answer, err := promptLineW(errOut, reader, "Proceed? This provisions AWS resources that cost money [y/N]: ")
		if err != nil {
			return err
		}
		switch strings.ToLower(strings.TrimSpace(answer)) {
		case "y", "yes":
		default:
			return cliError{message: "install aborted", code: ExitFailed}
		}
	}

	run := exec.CommandContext(cmd.Context(), "bash", script)
	run.Dir = source
	run.Env = env
	run.Stdin = nil
	run.Stdout = cmd.OutOrStdout()
	run.Stderr = errOut
	if err := run.Run(); err != nil {
		return fmt.Errorf("install failed: %w", err)
	}
	return nil
}

// installWizard fills any unset options by prompting on the attached terminal.
func (app App) installWizard(cmd *cobra.Command, reader *bufio.Reader, opts *installAWSOptions) error {
	errOut := cmd.ErrOrStderr()
	fmt.Fprintln(errOut, "Layer AWS install — Terraform provisions the account footprint, Helm installs the release.")
	fmt.Fprintln(errOut)

	if opts.profile == "" {
		if profiles := awsProfiles(); len(profiles) > 0 {
			fmt.Fprintf(errOut, "AWS profiles found: %s\n", strings.Join(profiles, ", "))
		}
		answer, err := promptLineW(errOut, reader, "AWS profile [use ambient credentials]: ")
		if err != nil {
			return err
		}
		opts.profile = strings.TrimSpace(answer)
	}

	if opts.region == "" {
		answer, err := promptLineW(errOut, reader, "AWS region [us-east-1]: ")
		if err != nil {
			return err
		}
		opts.region = strings.TrimSpace(answer)
	}

	if opts.nodeType == "" {
		fmt.Fprintln(errOut, "Base node — the always-on system node running the gateway, control loops, and document cache:")
		for _, choice := range systemNodeTypeChoices {
			fmt.Fprintf(errOut, "  %-12s %s\n", choice.name, choice.note)
		}
		answer, err := promptLineW(errOut, reader, fmt.Sprintf("Base node instance type [%s]: ", defaultSystemNodeType))
		if err != nil {
			return err
		}
		opts.nodeType = strings.TrimSpace(answer)
	}

	if opts.tpufKey == "" {
		answer, err := promptLineW(errOut, reader, "Turbopuffer API key: ")
		if err != nil {
			return err
		}
		opts.tpufKey = strings.TrimSpace(answer)
	}

	if opts.clusterName == "" {
		answer, err := promptLineW(errOut, reader, "Cluster name [layer]: ")
		if err != nil {
			return err
		}
		opts.clusterName = strings.TrimSpace(answer)
	}
	return nil
}

// resolveInstallSource locates the hev/layer checkout carrying the deployment
// assets: explicit flag, then LAYER_SRC, then a walk up from the working
// directory.
func (app App) resolveInstallSource(flagValue string) (string, error) {
	candidates := []string{}
	if flagValue != "" {
		candidates = append(candidates, flagValue)
	}
	if src := strings.TrimSpace(app.opts.Env["LAYER_SRC"]); src != "" {
		candidates = append(candidates, src)
	}
	for _, dir := range candidates {
		abs, err := filepath.Abs(dir)
		if err != nil {
			return "", err
		}
		if isInstallSource(abs) {
			return abs, nil
		}
	}
	if len(candidates) > 0 {
		return "", cliError{message: fmt.Sprintf("%s does not look like a hev/layer checkout (missing scripts/install-layer.sh or infra/).\n\n%s", candidates[0], installSourceHelp), code: ExitUsage}
	}
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if isInstallSource(dir) {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", cliError{message: installSourceHelp, code: ExitUsage}
		}
		dir = parent
	}
}

func isInstallSource(dir string) bool {
	for _, rel := range []string{
		filepath.Join("scripts", "install-layer.sh"),
		filepath.Join("infra", "terraform"),
		filepath.Join("infra", "helm", "layer"),
	} {
		if _, err := os.Stat(filepath.Join(dir, rel)); err != nil {
			return false
		}
	}
	return true
}

// installScriptEnv builds the child environment for install-layer.sh from the
// process environment plus the wizard's answers.
func installScriptEnv(base map[string]string, opts installAWSOptions) []string {
	merged := make(map[string]string, len(base)+8)
	for key, value := range base {
		merged[key] = value
	}
	for _, kv := range installEnvOverrides(opts) {
		key, value, _ := strings.Cut(kv, "=")
		merged[key] = value
	}
	keys := make([]string, 0, len(merged))
	for key := range merged {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	env := make([]string, 0, len(keys))
	for _, key := range keys {
		env = append(env, key+"="+merged[key])
	}
	return env
}

func installEnvOverrides(opts installAWSOptions) []string {
	env := []string{
		"AWS_REGION=" + opts.region,
		"CLUSTER_NAME=" + opts.clusterName,
		"NAMESPACE=" + opts.namespace,
		"LAYER_VERSION=" + opts.layerVersion,
		"SYSTEM_NODE_INSTANCE_TYPE=" + opts.nodeType,
		"TURBOPUFFER_API_KEY=" + opts.tpufKey,
	}
	if opts.profile != "" {
		env = append(env, "AWS_PROFILE="+opts.profile)
	}
	if opts.licenseToken != "" {
		env = append(env, "LICENSE_TOKEN="+opts.licenseToken)
	}
	if opts.skipTerraform {
		env = append(env, "SKIP_TERRAFORM=1")
	}
	return env
}

// installEnvForDisplay is the dry-run rendering: same overrides, secrets masked.
func installEnvForDisplay(opts installAWSOptions) []string {
	masked := opts
	masked.tpufKey = maskSecret(opts.tpufKey)
	masked.licenseToken = maskSecret(opts.licenseToken)
	return installEnvOverrides(masked)
}

func maskSecret(value string) string {
	if value == "" {
		return ""
	}
	if len(value) <= 8 {
		return "****"
	}
	return value[:4] + "…" + value[len(value)-4:]
}

func orLabel(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func installStageLabel(skip bool) string {
	if skip {
		return "skipped (reusing existing outputs)"
	}
	return "apply (VPC, EKS, IAM/IRSA, S3, ECR)"
}

// awsProfiles lists configured AWS profiles for the wizard hint. Best-effort:
// an absent aws CLI just means no hint.
func awsProfiles() []string {
	out, err := exec.Command("aws", "configure", "list-profiles").Output()
	if err != nil {
		return nil
	}
	var profiles []string
	for _, line := range strings.Split(string(out), "\n") {
		if line = strings.TrimSpace(line); line != "" {
			profiles = append(profiles, line)
		}
	}
	return profiles
}
