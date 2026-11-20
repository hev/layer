package cmd

import (
	"context"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/hev/layer/apps/layer-cli/internal/output"
	hevlayer "github.com/hev/layer/clients/go"
	"github.com/spf13/cobra"
)

const archiveUsage = "usage: layer archive <export|restore|status> NAMESPACE ..."

// The index archive (experimental, Pro): a namespace as an Iceberg table in
// object storage, exported from its engine and restored into an empty one.
func newArchiveCommand(app App, flags *globalFlags) *cobra.Command {
	archiveCmd := &cobra.Command{
		Use:   "archive",
		Short: "Export, restore and inspect the index archive (experimental)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return usagef(archiveUsage)
		},
	}
	archiveCmd.AddCommand(newArchiveExportCommand(app, flags))
	archiveCmd.AddCommand(newArchiveRestoreCommand(app, flags))
	archiveCmd.AddCommand(newArchiveStatusCommand(app, flags))
	return archiveCmd
}

type archiveWaitFlags struct {
	noWait       bool
	pollInterval time.Duration
	timeout      time.Duration
}

func (wait *archiveWaitFlags) register(cmd *cobra.Command) {
	cmd.Flags().BoolVar(&wait.noWait, "no-wait", false, "Return as soon as the run starts")
	cmd.Flags().DurationVar(&wait.pollInterval, "poll-interval", 2*time.Second, "Run polling interval")
	cmd.Flags().DurationVar(&wait.timeout, "timeout", 0, "Stop waiting after this long (the run continues); 0 waits until it ends")
}

func newArchiveExportCommand(app App, flags *globalFlags) *cobra.Command {
	var store, distanceMetric, resume string
	var wait archiveWaitFlags
	cmd := &cobra.Command{
		Use:   "export NAMESPACE",
		Short: "Export a namespace into the archive as a table snapshot",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			resolved, format, err := app.resolve(cmd, flags, runFlags{})
			if err != nil {
				return err
			}
			client := app.clientFor(resolved)
			run, err := client.CreateArchiveExport(cmd.Context(), &hevlayer.ArchiveExportRequest{
				Namespace:      args[0],
				Store:          store,
				DistanceMetric: distanceMetric,
				Resume:         resume,
			})
			if err != nil {
				return err
			}
			return finishArchiveRun(cmd, client, format, run, wait)
		},
	}
	cmd.Flags().StringVar(&store, "store", "", "VectorStore to read (default: the store the namespace routes to)")
	cmd.Flags().StringVar(&distanceMetric, "distance-metric", "", "Distance metric, for an engine whose schema does not report one")
	cmd.Flags().StringVar(&resume, "resume", "", "Continue a failed export run from its checkpoint")
	wait.register(cmd)
	return cmd
}

func newArchiveRestoreCommand(app App, flags *globalFlags) *cobra.Command {
	var store string
	var wait archiveWaitFlags
	cmd := &cobra.Command{
		Use:   "restore NAMESPACE",
		Short: "Rebuild a namespace in an empty engine from its archived snapshot",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			resolved, format, err := app.resolve(cmd, flags, runFlags{})
			if err != nil {
				return err
			}
			client := app.clientFor(resolved)
			run, err := client.CreateArchiveRestore(cmd.Context(), &hevlayer.ArchiveRestoreRequest{
				Namespace: args[0],
				Store:     store,
			})
			if err != nil {
				return err
			}
			return finishArchiveRun(cmd, client, format, run, wait)
		},
	}
	cmd.Flags().StringVar(&store, "store", "", "VectorStore to load (default: the store the namespace routes to)")
	wait.register(cmd)
	return cmd
}

func newArchiveStatusCommand(app App, flags *globalFlags) *cobra.Command {
	var runID string
	cmd := &cobra.Command{
		Use:   "status NAMESPACE",
		Short: "Show a namespace's archived table, engines and recent runs",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			resolved, format, err := app.resolve(cmd, flags, runFlags{})
			if err != nil {
				return err
			}
			client := app.clientFor(resolved)
			if runID != "" {
				run, err := client.GetArchiveRun(cmd.Context(), args[0], runID)
				if err != nil {
					return err
				}
				return emitArchiveRun(cmd.OutOrStdout(), format, *run)
			}
			status, err := client.GetArchiveNamespace(cmd.Context(), args[0])
			if err != nil {
				return err
			}
			return emitArchiveNamespace(cmd.OutOrStdout(), format, *status)
		},
	}
	cmd.Flags().StringVar(&runID, "run", "", "Show one run instead of the namespace")
	return cmd
}

// finishArchiveRun waits for a started run (unless --no-wait), prints it, and
// fails the command when the run failed.
func finishArchiveRun(cmd *cobra.Command, client *hevlayer.Client, format string, run *hevlayer.ArchiveRun, wait archiveWaitFlags) error {
	if !wait.noWait {
		fmt.Fprintf(cmd.ErrOrStderr(), "archive %s %s started for %s (store %s)\n", run.Kind, run.ID, run.Namespace, run.Store)
		finished, err := waitArchiveRun(cmd.Context(), cmd.ErrOrStderr(), client, run, wait)
		if err != nil {
			return err
		}
		run = finished
	}
	if err := emitArchiveRun(cmd.OutOrStdout(), format, *run); err != nil {
		return err
	}
	if run.State == "failed" {
		return cliError{message: fmt.Sprintf("archive %s %s failed: %s", run.Kind, run.ID, run.Error), code: ExitFailed}
	}
	return nil
}

func waitArchiveRun(ctx context.Context, stderr io.Writer, client *hevlayer.Client, run *hevlayer.ArchiveRun, wait archiveWaitFlags) (*hevlayer.ArchiveRun, error) {
	interval := wait.pollInterval
	if interval <= 0 {
		interval = 2 * time.Second
	}
	var deadline <-chan time.Time
	if wait.timeout > 0 {
		deadline = time.After(wait.timeout)
	}
	phases := len(run.Phases)
	for run.State == "running" {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-deadline:
			return nil, cliError{
				message: fmt.Sprintf("stopped waiting after %s; the run continues: layer archive status %s --run %s", wait.timeout, run.Namespace, run.ID),
				code:    ExitFailed,
			}
		case <-time.After(interval):
		}
		next, err := client.GetArchiveRun(ctx, run.Namespace, run.ID)
		if err != nil {
			return nil, err
		}
		for _, phase := range next.Phases[min(phases, len(next.Phases)):] {
			fmt.Fprintf(stderr, "  %s: %s\n", phase.Name, formatSeconds(phase.Seconds))
		}
		phases = len(next.Phases)
		run = next
	}
	return run, nil
}

func formatSeconds(seconds float64) string {
	return fmt.Sprintf("%.1fs", seconds)
}

func emitArchiveRun(out io.Writer, format string, run hevlayer.ArchiveRun) error {
	switch format {
	case output.JSON:
		return output.WriteJSON(out, run)
	case output.Names:
		fmt.Fprintln(out, run.ID)
		return nil
	}
	rows := [][]string{
		{"RUN", run.ID},
		{"KIND", run.Kind},
		{"NAMESPACE", run.Namespace},
		{"STORE", run.Store},
		{"STATE", run.State},
		{"ROWS", output.FormatInt(run.Rows)},
		{"STARTED", dash(run.StartedAt)},
		{"FINISHED", dash(run.FinishedAt)},
	}
	if run.Error != "" {
		rows = append(rows, []string{"ERROR", run.Error})
	}
	for _, phase := range run.Phases {
		rows = append(rows, []string{"PHASE " + phase.Name, formatSeconds(phase.Seconds)})
	}
	// The report: scalars as they are, the restore's verification flattened.
	keys := make([]string, 0, len(run.Result))
	for key := range run.Result {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		switch value := run.Result[key].(type) {
		case map[string]interface{}:
			nested := make([]string, 0, len(value))
			for name := range value {
				nested = append(nested, name)
			}
			sort.Strings(nested)
			for _, name := range nested {
				rows = append(rows, []string{key + "." + name, archiveValue(value[name])})
			}
		default:
			rows = append(rows, []string{key, archiveValue(value)})
		}
	}
	return output.WriteFields(out, rows)
}

func archiveValue(value interface{}) string {
	switch typed := value.(type) {
	case nil:
		return "—"
	case string:
		return dash(typed)
	case float64:
		if typed == float64(int64(typed)) {
			return output.FormatInt(int64(typed))
		}
		return output.FormatFloat(typed)
	case []interface{}:
		if len(typed) == 0 {
			return "—"
		}
		return fmt.Sprint(typed...)
	default:
		return fmt.Sprint(typed)
	}
}

func emitArchiveNamespace(out io.Writer, format string, status hevlayer.ArchiveNamespace) error {
	switch format {
	case output.JSON:
		return output.WriteJSON(out, status)
	case output.Names:
		fmt.Fprintln(out, status.Namespace)
		return nil
	}
	rows := [][]string{
		{"NAMESPACE", status.Namespace},
		{"ARCHIVED", fmt.Sprintf("%t", status.Archived)},
		{"MANIFEST", status.ManifestUrl},
	}
	// The generated types are values: an absent manifest or table is zero.
	if manifest := status.Manifest; manifest.Incarnation != "" {
		rows = append(rows,
			[]string{"MODE", manifest.Mode},
			[]string{"SCHEMA_VERSION", output.FormatInt(manifest.SchemaVersion)},
			[]string{"ATTRIBUTES", output.FormatInt(int64(manifest.Attributes))},
		)
		if table := manifest.Table; table.Location != "" {
			rows = append(rows,
				[]string{"TABLE", table.Location},
				[]string{"SNAPSHOT", output.FormatInt(table.SnapshotID)},
				[]string{"METADATA_VERSION", output.FormatInt(table.MetadataVersion)},
				[]string{"ROWS", output.FormatInt(table.RowCount)},
				[]string{"LOGICAL_BYTES", output.FormatInt(table.LogicalBytes)},
			)
		}
		for _, engine := range manifest.Engines {
			rows = append(rows, []string{
				"ENGINE " + engine.Store,
				fmt.Sprintf("%s %s, snapshot %d, loaded %s", engine.Kind, engine.EngineVersion, engine.LoadedFromSnapshot, engine.LoadedAt),
			})
		}
	}
	if err := output.WriteFields(out, rows); err != nil {
		return err
	}
	if len(status.Runs) == 0 {
		return nil
	}
	fmt.Fprintln(out)
	runs := make([][]string, 0, len(status.Runs))
	for _, run := range status.Runs {
		runs = append(runs, []string{run.ID, run.Kind, run.Store, run.State, output.FormatInt(run.Rows), dash(run.StartedAt), dash(run.FinishedAt)})
	}
	return output.WriteTable(out, []string{"RUN", "KIND", "STORE", "STATE", "ROWS", "STARTED", "FINISHED"}, runs)
}
