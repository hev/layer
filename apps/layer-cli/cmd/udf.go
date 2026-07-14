package cmd

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/hev/layer/apps/layer-cli/internal/output"
	hevlayer "github.com/hev/layer/clients/go"
	"github.com/spf13/cobra"
)

type udfRow struct {
	ID                string   `json:"id"`
	Paused            bool     `json:"paused"`
	TargetNamespaces  []string `json:"target_namespaces,omitempty"`
	Version           string   `json:"version,omitempty"`
	PendingCount      int64    `json:"pending_count"`
	ProcessingCount   int64    `json:"processing_count"`
	FailedCount       int64    `json:"failed_count"`
	SweepsCompleted   int64    `json:"sweeps_completed"`
	IndexedRatePerMin float64  `json:"indexed_rate_per_min"`
}

func newUdfCommand(app App, flags *globalFlags) *cobra.Command {
	udfCmd := &cobra.Command{
		Use:   "udf",
		Short: "Observe UDFs",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return usagef("usage: layer udf <list|get> ...")
		},
	}
	udfCmd.AddCommand(newUdfListCommand(app, flags))
	udfCmd.AddCommand(newUdfStatusCommand(app, flags))
	return udfCmd
}

func newUdfListCommand(app App, flags *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List registered UDFs with queue status",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			resolved, format, err := app.resolve(cmd, flags, runFlags{})
			if err != nil {
				return err
			}
			client := app.clientFor(resolved)
			rows, err := listUdfsWithStatus(cmd.Context(), client)
			if err != nil {
				return err
			}
			return emitUdfRows(cmd.OutOrStdout(), format, rows)
		},
	}
}

func newUdfStatusCommand(app App, flags *globalFlags) *cobra.Command {
	var watch bool
	var pollInterval time.Duration
	cmd := &cobra.Command{
		Use:     "get UDF_ID",
		Aliases: []string{"status"},
		Short:   "Show one UDF's queue counts and discovery progress",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			resolved, format, err := app.resolve(cmd, flags, runFlags{})
			if err != nil {
				return err
			}
			client := app.clientFor(resolved)
			if watch {
				status, err := watchUdf(cmd.Context(), cmd.ErrOrStderr(), client, args[0], -1, pollInterval)
				if err != nil {
					return err
				}
				return emitUdfStatus(cmd.OutOrStdout(), format, status)
			}
			status, err := client.GetUdfStatus(cmd.Context(), args[0])
			if err != nil {
				return err
			}
			return emitUdfStatus(cmd.OutOrStdout(), format, status)
		},
	}
	cmd.Flags().BoolVar(&watch, "watch", false, "Poll until pending and processing counts drain")
	cmd.Flags().DurationVar(&pollInterval, "poll-interval", 2*time.Second, "Status polling interval")
	return cmd
}

func listUdfsWithStatus(ctx context.Context, client *hevlayer.Client) ([]udfRow, error) {
	list, err := client.ListUdfs(ctx)
	if err != nil {
		return nil, err
	}
	rows := make([]udfRow, 0, len(list.Udfs))
	for _, udf := range list.Udfs {
		status, err := client.GetUdfStatus(ctx, udf.ID)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", udf.ID, err)
		}
		rows = append(rows, udfRow{
			ID:                udf.ID,
			Paused:            status.Paused,
			TargetNamespaces:  udf.Spec.TargetNamespaces,
			Version:           udf.Spec.Version,
			PendingCount:      status.PendingCount,
			ProcessingCount:   status.ProcessingCount,
			FailedCount:       status.FailedCount,
			SweepsCompleted:   status.Discovery.SweepsCompleted,
			IndexedRatePerMin: status.IndexedRatePerMin,
		})
	}
	return rows, nil
}

func emitUdfRows(out io.Writer, format string, rows []udfRow) error {
	switch format {
	case output.JSON:
		return output.WriteJSON(out, rows)
	case output.Names:
		for _, row := range rows {
			fmt.Fprintln(out, row.ID)
		}
		return nil
	default:
		tableRows := make([][]string, 0, len(rows))
		for _, row := range rows {
			tableRows = append(tableRows, []string{
				row.ID,
				fmt.Sprintf("%t", row.Paused),
				output.FormatInt(row.PendingCount),
				output.FormatInt(row.ProcessingCount),
				output.FormatInt(row.FailedCount),
				output.FormatInt(row.SweepsCompleted),
				output.FormatFloat(row.IndexedRatePerMin),
			})
		}
		return output.WriteTable(out, []string{"UDF", "PAUSED", "PENDING", "RUNNING", "FAILED", "SWEEPS", "RATE/MIN"}, tableRows)
	}
}
