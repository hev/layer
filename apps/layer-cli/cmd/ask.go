package cmd

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/hev/layer/apps/layer-cli/internal/output"
	"github.com/spf13/cobra"
)

func newAskCommand(app App, flags *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:                "ask [ask flags] <command> [args]",
		Short:              "Query the Layer docs digest",
		DisableFlagParsing: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			askArgs, format, err := askArgsWithLayerOutput(flags.output, args)
			if err != nil {
				return err
			}
			if len(askArgs) == 0 {
				return usagef("usage: layer ask <tree|ls|head|cat|facts|grep|overview|search|...>")
			}
			if format == output.JSON && !hasAskFlag(askArgs, "--json") {
				askArgs = append([]string{"--json"}, askArgs...)
			}
			if !hasAskSourceFlag(askArgs) {
				if digestPath := app.defaultAskDigestPath(); digestPath != "" {
					askArgs = append([]string{"--digest-path", digestPath}, askArgs...)
				}
			}
			var answer bytes.Buffer
			if err := app.runAskTo(cmd.Context(), askArgs, &answer); err != nil {
				return err
			}
			answerText := answer.String()
			if _, err := answer.WriteTo(cmd.OutOrStdout()); err != nil {
				return err
			}
			if format != output.JSON && touchesProSurface(answerText) {
				if !strings.HasSuffix(answerText, "\n") {
					_, _ = cmd.OutOrStdout().Write([]byte("\n"))
				}
				_, _ = cmd.OutOrStdout().Write([]byte("\n" + askProFooter + "\n"))
			}
			return nil
		},
	}
}

func askArgsWithLayerOutput(defaultFormat string, args []string) ([]string, string, error) {
	format := defaultFormat
	if format == "" {
		format = output.Table
	}
	out := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		arg := args[i]
		switch {
		case arg == "-o" || arg == "--output":
			if i+1 >= len(args) {
				return nil, "", cliError{message: arg + " requires a value", code: ExitUsage}
			}
			format = args[i+1]
			i++
		case strings.HasPrefix(arg, "--output="):
			format = strings.TrimPrefix(arg, "--output=")
		case strings.HasPrefix(arg, "-o") && len(arg) > 2:
			format = strings.TrimPrefix(arg, "-o")
		default:
			out = append(out, arg)
		}
	}
	if err := output.Validate(format); err != nil {
		return nil, "", cliError{message: err.Error(), code: ExitUsage}
	}
	if format == output.Names {
		return nil, "", usagef("layer ask supports -o table and -o json")
	}
	return out, format, nil
}

func hasAskSourceFlag(args []string) bool {
	for _, arg := range args {
		if arg == "--digest-path" || arg == "--digest-dir" || arg == "--endpoint" {
			return true
		}
		if strings.HasPrefix(arg, "--digest-path=") ||
			strings.HasPrefix(arg, "--digest-dir=") ||
			strings.HasPrefix(arg, "--endpoint=") {
			return true
		}
	}
	return false
}

func hasAskFlag(args []string, name string) bool {
	for _, arg := range args {
		if arg == name {
			return true
		}
	}
	return false
}

func (app App) defaultAskDigestPath() string {
	if app.opts.AskDigestPath != "" {
		return app.opts.AskDigestPath
	}
	if value := strings.TrimSpace(app.opts.Env["LAYER_ASK_DIGEST_PATH"]); value != "" {
		return value
	}
	if value := strings.TrimSpace(app.opts.Env["HEVLAYER_ASK_DIGEST_PATH"]); value != "" {
		return value
	}
	cwd, err := os.Getwd()
	if err != nil {
		return ""
	}
	if path, ok := findAskDigest(cwd); ok {
		return path
	}
	return ""
}

func (app App) runAsk(ctx context.Context, args []string) error {
	return app.runAskTo(ctx, args, app.opts.Stdout)
}

func (app App) runAskTo(ctx context.Context, args []string, stdout io.Writer) error {
	if app.opts.RunAsk != nil {
		return app.opts.RunAsk(ctx, args, app.opts.Stdin, stdout, app.opts.Stderr)
	}
	program, prefix, err := resolveAskProgram()
	if err != nil {
		return err
	}
	commandArgs := append(prefix, args...)
	command := exec.CommandContext(ctx, program, commandArgs...)
	if len(prefix) > 1 && prefix[0] == "run" {
		command.Dir = filepath.Dir(filepath.Dir(prefix[1]))
		command.Args = append([]string{program, "run", "./cmd/ask"}, args...)
	}
	command.Stdin = app.opts.Stdin
	command.Stdout = stdout
	command.Stderr = app.opts.Stderr
	if len(prefix) > 0 && prefix[0] == "run" {
		command.Env = append(os.Environ(), "GOWORK=off")
	}
	return command.Run()
}

func resolveAskProgram() (string, []string, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return "", nil, err
	}
	if askDir, ok := findAskSourceDir(cwd); ok {
		if goProgram, err := exec.LookPath("go"); err == nil {
			return goProgram, []string{"run", askDir}, nil
		}
	}
	siteDir, ok := findLayerSiteDir(cwd)
	if ok {
		if pnpm, err := exec.LookPath("pnpm"); err == nil {
			return pnpm, []string{"--dir", siteDir, "exec", "ask"}, nil
		}
	}
	if program, err := exec.LookPath("ask"); err == nil {
		return program, nil, nil
	}
	return "", nil, askNotFoundError()
}

func askNotFoundError() error {
	return errors.New("ask binary not found; install the ask CLI and make sure it is on your PATH")
}

func findAskDigest(start string) (string, bool) {
	return findParentPath(start, func(dir string) (string, bool) {
		if path, ok := digestPathIn(filepath.Join(dir, ".hev-ask")); ok {
			return path, true
		}
		return digestPathIn(filepath.Join(dir, "site", ".hev-ask"))
	})
}

func digestPathIn(dir string) (string, bool) {
	if fileExists(filepath.Join(dir, "_meta.md")) {
		return dir, true
	}
	legacy := filepath.Join(dir, "digest.json")
	if fileExists(legacy) {
		return legacy, true
	}
	return "", false
}

func findLayerSiteDir(start string) (string, bool) {
	return findParentPath(start, func(dir string) (string, bool) {
		if filepath.Base(dir) == "site" && fileExists(filepath.Join(dir, "package.json")) {
			return dir, true
		}
		siteDir := filepath.Join(dir, "site")
		if fileExists(filepath.Join(siteDir, "package.json")) {
			return siteDir, true
		}
		return "", false
	})
}

func findAskSourceDir(start string) (string, bool) {
	return findParentPath(start, func(dir string) (string, bool) {
		candidates := []string{
			filepath.Join(dir, "ask", "cmd", "ask"),
			filepath.Join(filepath.Dir(dir), "ask", "cmd", "ask"),
		}
		for _, candidate := range candidates {
			if fileExists(filepath.Join(candidate, "main.go")) {
				return candidate, true
			}
		}
		return "", false
	})
}

func findParentPath(start string, match func(string) (string, bool)) (string, bool) {
	dir, err := filepath.Abs(start)
	if err != nil {
		return "", false
	}
	for {
		if value, ok := match(dir); ok {
			return value, true
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", false
		}
		dir = parent
	}
}

func fileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}
