package cmd

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/hev/layer/apps/layer-cli/internal/output"
	hevlayer "github.com/hev/layer/clients/go"
	"github.com/spf13/cobra"
)

// pipelineRow is a pipeline list entry joined with its live gateway queue
// status. The gateway owns runtime queue depth, so list/get fan out to the
// pipeline-status API best-effort; a pipeline with no worker registered yet
// has no queue.
type pipelineRow struct {
	hevlayer.Pipeline
	Queue *hevlayer.PipelineStatus `json:"queue,omitempty"`
}

func newPipelineCommand(app App, flags *globalFlags) *cobra.Command {
	pipelineCmd := &cobra.Command{
		Use:   "pipeline",
		Short: "Observe pipelines",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return usagef("usage: layer pipeline <list|get> ...")
		},
	}
	pipelineCmd.AddCommand(newPipelineListCommand(app, flags))
	pipelineCmd.AddCommand(newPipelineGetCommand(app, flags))
	return pipelineCmd
}

func newPipelineListCommand(app App, flags *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List pipelines with live queue status",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			resolved, format, err := app.resolve(cmd, flags, runFlags{})
			if err != nil {
				return err
			}
			client := app.clientFor(resolved)
			rows, err := listPipelinesWithStatus(cmd.Context(), client)
			if err != nil {
				return err
			}
			return emitPipelines(cmd.OutOrStdout(), format, rows)
		},
	}
}

func newPipelineGetCommand(app App, flags *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "get ID",
		Short: "Show one pipeline with live queue status",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			resolved, format, err := app.resolve(cmd, flags, runFlags{})
			if err != nil {
				return err
			}
			client := app.clientFor(resolved)
			row, err := getPipelineDetail(cmd.Context(), client, args[0])
			if err != nil {
				return err
			}
			return emitPipelineDetail(cmd.OutOrStdout(), format, row)
		},
	}
}

func listPipelinesWithStatus(ctx context.Context, client *hevlayer.Client) ([]pipelineRow, error) {
	list, err := client.ListPipelines(ctx)
	if err != nil {
		return nil, fmt.Errorf("list pipelines: %w", err)
	}
	rows := make([]pipelineRow, 0, len(list.Pipelines))
	for _, p := range list.Pipelines {
		rows = append(rows, pipelineRow{Pipeline: p, Queue: pipelineQueue(ctx, client, p.ID)})
	}
	return rows, nil
}

func getPipelineDetail(ctx context.Context, client *hevlayer.Client, id string) (pipelineRow, error) {
	list, err := client.ListPipelines(ctx)
	if err != nil {
		return pipelineRow{}, fmt.Errorf("list pipelines: %w", err)
	}
	for _, p := range list.Pipelines {
		if p.ID == id {
			return pipelineRow{Pipeline: p, Queue: pipelineQueue(ctx, client, p.ID)}, nil
		}
	}
	return pipelineRow{}, fmt.Errorf("pipeline %q not found", id)
}

// pipelineQueue fetches a pipeline's gateway queue status, best-effort: a
// pipeline with no worker registered yet has no queue, which is not an error.
func pipelineQueue(ctx context.Context, client *hevlayer.Client, id string) *hevlayer.PipelineStatus {
	if status, err := client.GetPipelineStatus(ctx, id); err == nil {
		return status
	}
	return nil
}

func emitPipelines(out io.Writer, format string, rows []pipelineRow) error {
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
			pending, running, failed, rate := "—", "—", "—", "—"
			if q := row.Queue; q != nil {
				pending = output.FormatInt(q.PendingCount)
				running = output.FormatInt(q.ProcessingCount)
				failed = output.FormatInt(q.FailedCount)
				rate = output.FormatFloat(q.IndexedRatePerMin)
			}
			tableRows = append(tableRows, []string{
				row.ID,
				dash(row.TargetNamespace),
				pending, running, failed, rate,
			})
		}
		return output.WriteTable(out, []string{"ID", "TARGET", "PENDING", "RUNNING", "FAILED", "RATE/MIN"}, tableRows)
	}
}

func emitPipelineDetail(out io.Writer, format string, detail pipelineRow) error {
	switch format {
	case output.JSON:
		return output.WriteJSON(out, detail)
	case output.Names:
		fmt.Fprintln(out, detail.ID)
		return nil
	default:
		rows := [][]string{
			{"ID", detail.ID},
			{"TARGET", dash(detail.TargetNamespace)},
			{"DISTANCE_METRIC", dash(detail.DistanceMetric)},
			{"CREATED_AT", dash(detail.CreatedAt)},
		}
		if q := detail.Queue; q != nil {
			rows = append(rows,
				[]string{"QUEUE_PENDING", output.FormatInt(q.PendingCount)},
				[]string{"QUEUE_PROCESSING", output.FormatInt(q.ProcessingCount)},
				[]string{"QUEUE_FAILED", output.FormatInt(q.FailedCount)},
				[]string{"QUEUE_RATE_PER_MIN", output.FormatFloat(q.IndexedRatePerMin)},
			)
		}
		if err := output.WriteFields(out, rows); err != nil {
			return err
		}
		if detail.Queue == nil {
			fmt.Fprintln(out, "\nNo gateway queue registered yet for this pipeline.")
		}
		return nil
	}
}

func dash(value string) string {
	if strings.TrimSpace(value) == "" {
		return "—"
	}
	return value
}
