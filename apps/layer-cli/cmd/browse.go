package cmd

import (
	"context"
	"fmt"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/hev/layer/apps/layer-cli/internal/tui"
	"github.com/spf13/cobra"
)

func newBrowseCommand(app App, flags *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "browse",
		Short: "Open the operations TUI",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if !app.opts.StdinIsTerminal || !app.opts.StdoutIsTerminal {
				return cliError{message: "layer browse requires a TTY", code: ExitUsage}
			}
			return app.launchTUI(cmd, flags)
		},
	}
}

func launchBubbleTea(ctx context.Context, opts TUIOptions) error {
	model := tui.New(tui.Options{
		HomeDir: opts.HomeDir,
		Env:     opts.Env,
		Request: opts.Request,
	})
	program := tea.NewProgram(model, tea.WithAltScreen())
	_, err := program.Run()
	if err != nil {
		return fmt.Errorf("TUI failed: %w", err)
	}
	_ = ctx
	return nil
}
