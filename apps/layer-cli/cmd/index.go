package cmd

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/hev/layer/apps/layer-cli/internal/output"
	hevlayer "github.com/hev/layer/clients/go"
	"github.com/spf13/cobra"
)

const (
	// snapshotWindow bounds the global snapshot-activity feed used to derive
	// each index's last-snapshot timestamp, mirroring the TUI. Indexes that
	// have not snapshotted within this window show no snapshot in `index list`;
	// `index get` reads full per-index history regardless.
	snapshotWindow = 90 * 24 * time.Hour
	historyLimit   = 500
)

// indexListRow is a namespace list entry augmented with its last-snapshot
// timestamp so the non-interactive list carries the same datum as the TUI
// SNAP column. The embedded entry's fields are promoted in JSON output.
type indexListRow struct {
	hevlayer.NamespaceListEntry
	LastSnapshotMs int64 `json:"last_snapshot_ms"`
}

// indexDetail mirrors the TUI index detail view: the list entry plus snapshot
// history (count, whether the fetch hit its cap, the latest watermark, and the
// recent tail).
type indexDetail struct {
	hevlayer.NamespaceListEntry
	Snapshots        int                             `json:"snapshots"`
	SnapshotsAtLimit bool                            `json:"snapshots_at_limit"`
	LastSnapshotMs   int64                           `json:"last_snapshot_ms"`
	RecentSnapshots  []hevlayer.SnapshotHistoryEntry `json:"recent_snapshots,omitempty"`
}

func newIndexCommand(app App, flags *globalFlags) *cobra.Command {
	indexCmd := &cobra.Command{
		Use:   "index",
		Short: "Operate on indexes",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return usagef("usage: layer index <list|get|delete> ...")
		},
	}
	indexCmd.AddCommand(newIndexListCommand(app, flags))
	indexCmd.AddCommand(newIndexGetCommand(app, flags))
	indexCmd.AddCommand(newIndexSnapshotCommand(app, flags))
	indexCmd.AddCommand(newIndexPolicyCommand(app, flags))
	indexCmd.AddCommand(newIndexDeleteCommand(app, flags))
	return indexCmd
}

func newIndexListCommand(app App, flags *globalFlags) *cobra.Command {
	var prefix string
	var pageSize int64
	cmd := &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List indexes",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			resolved, format, err := app.resolve(cmd, flags, runFlags{})
			if err != nil {
				return err
			}
			client := app.clientFor(resolved)
			entries, err := listNamespaces(cmd.Context(), client, prefix, pageSize)
			if err != nil {
				return err
			}
			// The last-snapshot column needs the global activity feed. Skip it
			// for -o names (names never render it) and treat it as best-effort
			// elsewhere so a feed outage still lists indexes.
			lastSnap := map[string]int64{}
			if format != output.Names {
				lastSnap = lastSnapshots(cmd.Context(), client)
			}
			rows := make([]indexListRow, 0, len(entries))
			for _, entry := range entries {
				rows = append(rows, indexListRow{NamespaceListEntry: entry, LastSnapshotMs: lastSnap[entry.Name]})
			}
			return emitNamespaces(cmd.OutOrStdout(), format, rows)
		},
	}
	cmd.Flags().StringVar(&prefix, "prefix", "", "Index name prefix")
	cmd.Flags().Int64Var(&pageSize, "page-size", 0, "Page size")
	return cmd
}

func newIndexGetCommand(app App, flags *globalFlags) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "get NAME",
		Short: "Show one index with snapshot history",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			resolved, format, err := app.resolve(cmd, flags, runFlags{})
			if err != nil {
				return err
			}
			client := app.clientFor(resolved)
			detail, err := getIndexDetail(cmd.Context(), client, args[0])
			if err != nil {
				return err
			}
			return emitIndexDetail(cmd.OutOrStdout(), format, detail)
		},
	}
	return cmd
}

func newIndexSnapshotCommand(app App, flags *globalFlags) *cobra.Command {
	var field string
	var source string
	var pollInterval time.Duration
	cmd := &cobra.Command{
		Use:   "snapshot NAME --field FIELD",
		Short: "Create and wait for a snapshot job",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			resolved, format, err := app.resolve(cmd, flags, runFlags{})
			if err != nil {
				return err
			}
			field = strings.TrimSpace(field)
			if field == "" {
				return usagef("index snapshot requires --field")
			}
			client := app.clientFor(resolved)
			job, err := client.CreateSnapshot(cmd.Context(), args[0], &hevlayer.CreateSnapshotRequest{
				Field:  field,
				Source: hevlayer.SnapshotSource(source),
			})
			if err != nil {
				return err
			}
			job, err = waitSnapshotJob(cmd.Context(), client, args[0], job, pollInterval)
			if err != nil {
				return err
			}
			return emitSnapshotJob(cmd.OutOrStdout(), format, *job)
		},
	}
	cmd.Flags().StringVar(&field, "field", "", "Facet field to materialize")
	cmd.Flags().StringVar(&source, "source", "origin", "Snapshot source: auto, snapshot, cache, or origin")
	cmd.Flags().DurationVar(&pollInterval, "poll-interval", time.Second, "Polling interval while the snapshot job runs")
	return cmd
}

func newIndexPolicyCommand(app App, flags *globalFlags) *cobra.Command {
	var facetField []string
	var facetFields string
	var interval string
	var retention string
	cmd := &cobra.Command{
		Use:   "policy NAME --facet-field FIELD [--interval 5m] [--retention never]",
		Short: "Set snapshot policy for an index",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			resolved, format, err := app.resolve(cmd, flags, runFlags{})
			if err != nil {
				return err
			}
			fields := policyFields(facetField, facetFields)
			client := app.clientFor(resolved)
			policy, err := client.PutSnapshotPolicy(cmd.Context(), args[0], &hevlayer.SnapshotPolicy{
				FacetFields: fields,
				Interval:    interval,
				Retention:   retention,
			})
			if err != nil {
				return err
			}
			return emitSnapshotPolicy(cmd.OutOrStdout(), format, *policy)
		},
	}
	cmd.Flags().StringArrayVar(&facetField, "facet-field", nil, "Facet field to histogram; repeatable")
	cmd.Flags().StringVar(&facetFields, "facet-fields", "", "Comma-separated facet fields")
	cmd.Flags().StringVar(&interval, "interval", "5m", "Minimum interval between automatic snapshots")
	cmd.Flags().StringVar(&retention, "retention", "never", "Snapshot retention duration, or never")
	return cmd
}

func newIndexDeleteCommand(app App, flags *globalFlags) *cobra.Command {
	var yes bool
	var prefix string
	cmd := &cobra.Command{
		Use:   "delete [NAME...] [--prefix PREFIX]",
		Short: "Delete indexes and their hevlayer state",
		Args:  cobra.ArbitraryArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			resolved, format, err := app.resolve(cmd, flags, runFlags{})
			if err != nil {
				return err
			}
			prefixSet := flagChanged(cmd, "prefix")
			if prefixSet && len(args) > 0 {
				return usagef("index delete takes either NAME arguments or --prefix, not both")
			}
			if !prefixSet && len(args) == 0 {
				return usagef("usage: layer index delete NAME [NAME...] | layer index delete --prefix PREFIX")
			}
			client := app.clientFor(resolved)

			names := args
			if prefixSet {
				if strings.TrimSpace(prefix) == "" {
					return usagef("index delete --prefix requires a non-empty prefix")
				}
				names, err = matchIndexPrefix(cmd.Context(), client, prefix)
				if err != nil {
					return err
				}
				if len(names) == 0 {
					fmt.Fprintf(cmd.ErrOrStderr(), "No indexes match prefix %q.\n", prefix)
					return nil
				}
			}

			if !yes {
				if !app.opts.StdinIsTerminal {
					return usagef("index delete requires --yes when stdin is not a TTY")
				}
				fmt.Fprintln(cmd.ErrOrStderr(), "This purges the upstream Turbopuffer namespace, document cache rows and snapshot mirrors, S3 snapshots/search history/clickstream/shard metadata, in-memory job state, and the operator-discovered Index CR where GC is enabled.")
				if prefixSet {
					fmt.Fprintf(cmd.ErrOrStderr(), "Prefix %q matches %d index(es): %s\n", prefix, len(names), strings.Join(names, ", "))
					fmt.Fprint(cmd.ErrOrStderr(), "Delete them? [y/N] ")
				} else {
					fmt.Fprintf(cmd.ErrOrStderr(), "Delete %s? [y/N] ", strings.Join(names, ", "))
				}
				if !confirmed(app.opts.Stdin) {
					return usagef("aborted")
				}
			}
			return deleteNamespaces(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), client, format, names)
		},
	}
	cmd.Flags().BoolVar(&yes, "yes", false, "Skip confirmation")
	cmd.Flags().StringVar(&prefix, "prefix", "", "Delete every index whose name starts with this prefix")
	return cmd
}

// matchIndexPrefix returns the names of indexes whose name starts with prefix.
// The gateway forwards the prefix to the upstream namespace list, but we
// re-check HasPrefix client-side so the set the operator confirms is exactly
// the set we delete — never broadened by looser upstream prefix semantics.
func matchIndexPrefix(ctx context.Context, client *hevlayer.Client, prefix string) ([]string, error) {
	entries, err := listNamespaces(ctx, client, prefix, 0)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name, prefix) {
			names = append(names, entry.Name)
		}
	}
	return names, nil
}

func listNamespaces(ctx context.Context, client *hevlayer.Client, prefix string, pageSize int64) ([]hevlayer.NamespaceListEntry, error) {
	var entries []hevlayer.NamespaceListEntry
	cursor := ""
	for {
		page, err := client.ListNamespaces(ctx, &hevlayer.ListNamespacesParams{
			Prefix:   prefix,
			Cursor:   cursor,
			PageSize: pageSize,
		})
		if err != nil {
			return nil, err
		}
		entries = append(entries, page.Namespaces...)
		if page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
	}
	return entries, nil
}

// lastSnapshots returns the latest snapshot timestamp per namespace from one
// global activity feed call, newest first. Best-effort: a feed error yields an
// empty map so callers still render the rest of the data.
func lastSnapshots(ctx context.Context, client *hevlayer.Client) map[string]int64 {
	out := map[string]int64{}
	since := time.Now().Add(-snapshotWindow).UnixMilli()
	activity, err := client.ListSnapshotActivity(ctx, &hevlayer.ListSnapshotActivityParams{
		Since: since,
		Limit: historyLimit,
	})
	if err != nil {
		return out
	}
	for _, event := range activity.Events {
		if _, seen := out[event.Namespace]; !seen {
			out[event.Namespace] = event.TsMs
		}
	}
	return out
}

func getIndexDetail(ctx context.Context, client *hevlayer.Client, name string) (indexDetail, error) {
	// ListNamespaces with the exact name as prefix is the only path to the
	// summary fields (rows, size, stability, cache); filter to the exact match.
	entries, err := listNamespaces(ctx, client, name, 0)
	if err != nil {
		return indexDetail{}, err
	}
	var entry *hevlayer.NamespaceListEntry
	for i := range entries {
		if entries[i].Name == name {
			entry = &entries[i]
			break
		}
	}
	if entry == nil {
		return indexDetail{}, fmt.Errorf("index %q not found", name)
	}
	detail := indexDetail{NamespaceListEntry: *entry}
	history, err := client.ListNamespaceHistory(ctx, name, &hevlayer.ListNamespaceHistoryParams{Limit: historyLimit})
	if err != nil {
		return indexDetail{}, err
	}
	detail.RecentSnapshots = history
	detail.Snapshots = len(history)
	detail.SnapshotsAtLimit = len(history) >= historyLimit
	if len(history) > 0 {
		detail.LastSnapshotMs = history[0].WatermarkMs
	}
	return detail, nil
}

func waitSnapshotJob(ctx context.Context, client *hevlayer.Client, namespace string, job *hevlayer.SnapshotJob, pollInterval time.Duration) (*hevlayer.SnapshotJob, error) {
	if pollInterval <= 0 {
		pollInterval = time.Second
	}
	for {
		switch string(job.Status) {
		case "completed":
			return job, nil
		case "failed":
			if job.Error != "" {
				return job, fmt.Errorf("snapshot job %s failed: %s", job.ID, job.Error)
			}
			return job, fmt.Errorf("snapshot job %s failed", job.ID)
		}
		select {
		case <-ctx.Done():
			return job, ctx.Err()
		case <-time.After(pollInterval):
		}
		next, err := client.GetSnapshotJob(ctx, namespace, job.ID)
		if err != nil {
			return job, err
		}
		job = next
	}
}

func policyFields(repeated []string, csv string) []string {
	var fields []string
	for _, raw := range repeated {
		for _, field := range strings.Split(raw, ",") {
			if field = strings.TrimSpace(field); field != "" {
				fields = append(fields, field)
			}
		}
	}
	for _, field := range strings.Split(csv, ",") {
		if field = strings.TrimSpace(field); field != "" {
			fields = append(fields, field)
		}
	}
	return fields
}

func emitSnapshotJob(out io.Writer, format string, job hevlayer.SnapshotJob) error {
	switch format {
	case output.JSON:
		return output.WriteJSON(out, job)
	case output.Names:
		fmt.Fprintln(out, job.ID)
		return nil
	default:
		return output.WriteFields(out, [][]string{
			{"ID", job.ID},
			{"NAMESPACE", job.Namespace},
			{"FIELD", job.Field},
			{"SOURCE", string(job.Source)},
			{"STATUS", string(job.Status)},
			{"DOCUMENTS_SCANNED", output.FormatInt(job.DocumentsScanned)},
			{"SHA", job.Sha},
		})
	}
}

func emitSnapshotPolicy(out io.Writer, format string, policy hevlayer.SnapshotPolicy) error {
	switch format {
	case output.JSON:
		return output.WriteJSON(out, policy)
	case output.Names:
		fmt.Fprintln(out, strings.Join(policy.FacetFields, ","))
		return nil
	default:
		return output.WriteFields(out, [][]string{
			{"FACET_FIELDS", strings.Join(policy.FacetFields, ",")},
			{"INTERVAL", policy.Interval},
			{"RETENTION", policy.Retention},
		})
	}
}

func emitNamespaces(out io.Writer, format string, rows []indexListRow) error {
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
			tableRows = append(tableRows, []string{
				row.Name,
				output.FormatInt(row.RowCount),
				output.FormatInt(row.SizeBytes),
				output.FormatInt(row.LastWriteMs),
				output.FormatInt(row.StableAsOfMs),
				fmt.Sprintf("%t", row.IsStable),
				output.FormatInt(row.Index.UnindexedBytes),
				output.FormatInt(row.LastSnapshotMs),
				row.CacheState.State,
			})
		}
		return output.WriteTable(out, []string{"NAME", "ROWS", "SIZE_BYTES", "LAST_WRITE_MS", "STABLE_MS", "STABLE", "UNINDEXED_BYTES", "LAST_SNAPSHOT_MS", "CACHE"}, tableRows)
	}
}

func emitIndexDetail(out io.Writer, format string, detail indexDetail) error {
	switch format {
	case output.JSON:
		return output.WriteJSON(out, detail)
	case output.Names:
		fmt.Fprintln(out, detail.Name)
		return nil
	default:
		schema := fmt.Sprintf("%d fields", len(detail.SchemaSummary.Fields))
		if detail.SchemaSummary.VectorDim > 0 {
			schema += fmt.Sprintf(", vector dim %d", detail.SchemaSummary.VectorDim)
		}
		indexStatus := detail.Index.Status
		if indexStatus == "" {
			indexStatus = "—"
		}
		if detail.Index.Status == "updating" {
			indexStatus = fmt.Sprintf("updating (%d unindexed bytes)", detail.Index.UnindexedBytes)
		}
		cache := detail.CacheState.State
		if cache == "" {
			cache = "—"
		}
		rows := [][]string{
			{"NAME", detail.Name},
			{"ROWS", output.FormatInt(detail.RowCount)},
			{"SIZE_BYTES", output.FormatInt(detail.SizeBytes)},
			{"SCHEMA", schema},
			{"LAST_WRITE_MS", output.FormatInt(detail.LastWriteMs)},
			{"STABLE_MS", output.FormatInt(detail.StableAsOfMs)},
			{"STABLE", fmt.Sprintf("%t", detail.IsStable)},
			{"STABLE_LAG_MS", output.FormatInt(stableLagMs(detail.NamespaceListEntry))},
			{"INDEX_STATUS", indexStatus},
			{"CACHE", cache},
			{"SNAPSHOTS", snapshotCount(detail)},
			{"LAST_SNAPSHOT_MS", output.FormatInt(detail.LastSnapshotMs)},
		}
		if err := output.WriteFields(out, rows); err != nil {
			return err
		}
		if len(detail.RecentSnapshots) > 0 {
			fmt.Fprintln(out, "\nRECENT SNAPSHOTS (watermark_ms, sha)")
			for _, snap := range detail.RecentSnapshots {
				fmt.Fprintf(out, "  %d  %s\n", snap.WatermarkMs, snap.Sha)
			}
		}
		return nil
	}
}

func snapshotCount(detail indexDetail) string {
	count := output.FormatInt(int64(detail.Snapshots))
	if detail.SnapshotsAtLimit {
		count += "+"
	}
	return count
}

// stableLagMs reports how far the stable watermark trails the last write, in ms;
// 0 when caught up or either timestamp is unknown.
func stableLagMs(entry hevlayer.NamespaceListEntry) int64 {
	if entry.LastWriteMs <= 0 || entry.StableAsOfMs <= 0 {
		return 0
	}
	delta := entry.LastWriteMs - entry.StableAsOfMs
	if delta < 0 {
		return 0
	}
	return delta
}

func deleteNamespaces(ctx context.Context, out io.Writer, errOut io.Writer, client *hevlayer.Client, format string, names []string) error {
	type result struct {
		Name   string `json:"name"`
		Status string `json:"status"`
	}
	results := make([]result, 0, len(names))
	for _, name := range names {
		if _, err := client.DeleteNamespace(ctx, name); err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		results = append(results, result{Name: name, Status: "deleted"})
	}
	if format == output.JSON {
		return output.WriteJSON(out, results)
	}
	for _, result := range results {
		if format == output.Names {
			fmt.Fprintln(out, result.Name)
		} else {
			fmt.Fprintf(out, "%s\t%s\n", result.Name, result.Status)
		}
		fmt.Fprintf(errOut, "If Index CR garbage collection is disabled, delete or inspect the Index CR separately: %s\n", result.Name)
	}
	return nil
}

func confirmed(stdin io.Reader) bool {
	scanner := bufio.NewScanner(stdin)
	if !scanner.Scan() {
		return false
	}
	answer := strings.TrimSpace(strings.ToLower(scanner.Text()))
	return answer == "y" || answer == "yes"
}
