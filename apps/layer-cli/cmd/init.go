package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/hev/layer/apps/layer-cli/internal/output"
	hevlayer "github.com/hev/layer/clients/go"
	"github.com/spf13/cobra"
)

type initRow struct {
	Namespace           string `json:"namespace"`
	SchemaVersion       int64  `json:"schema_version"`
	InitState           string `json:"init_state"`
	InitLagRows         int64  `json:"init_lag_rows"`
	ShardCount          int64  `json:"shard_count,omitempty"`
	ShardState          string `json:"shard_state"`
	ShardLagRows        int64  `json:"shard_lag_rows"`
	ScatterGatherActive bool   `json:"scatter_gather_active"`
}

func newInitCommand(app App, flags *globalFlags) *cobra.Command {
	var shardCount int64
	var watch bool
	var pollInterval time.Duration
	cmd := &cobra.Command{
		Use:   "init NAMESPACE --shards N",
		Short: "Initialize namespace sharding",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if shardCount <= 0 {
				return usagef("--shards must be > 0")
			}
			resolved, format, err := app.resolve(cmd, flags, runFlags{})
			if err != nil {
				return err
			}
			client := app.clientFor(resolved)
			resp, err := client.InitNamespace(cmd.Context(), args[0], &hevlayer.InitNamespaceRequest{
				SchemaVersion: 1,
				ShardCount:    shardCount,
			})
			if err != nil {
				return initNamespaceError(args[0], shardCount, err)
			}
			row := initResponseRow(*resp)
			if watch && !row.ScatterGatherActive {
				watched, err := watchInit(cmd.Context(), cmd.ErrOrStderr(), client, args[0], pollInterval)
				if err != nil {
					return err
				}
				row = watched
			}
			return emitInitRow(cmd.OutOrStdout(), format, row)
		},
	}
	cmd.Flags().Int64Var(&shardCount, "shards", 0, "Number of hash buckets to initialize")
	cmd.Flags().BoolVar(&watch, "watch", true, "Poll until scatter/gather activates")
	cmd.Flags().DurationVar(&pollInterval, "poll-interval", 2*time.Second, "Status polling interval")
	return cmd
}

func initNamespaceError(namespace string, shardCount int64, err error) error {
	var hevlayerErr *hevlayer.HevlayerError
	if errors.As(err, &hevlayerErr) && hevlayerErr.StatusCode == 409 {
		return fmt.Errorf("namespace %q is already initialized with a different shard count; requested %d shards. Re-run with the existing shard count or use a new namespace", namespace, shardCount)
	}
	return err
}

func initResponseRow(resp hevlayer.InitNamespaceResponse) initRow {
	return initRow{
		Namespace:           resp.Namespace,
		SchemaVersion:       resp.Layer.SchemaVersion,
		InitState:           resp.Layer.InitState,
		InitLagRows:         resp.Layer.InitLagRows,
		ShardCount:          resp.Layer.ShardCount,
		ShardState:          resp.Layer.ShardState,
		ShardLagRows:        resp.Layer.ShardLagRows,
		ScatterGatherActive: resp.Layer.ScatterGatherActive,
	}
}

func metadataInitRow(namespace string, metadata *hevlayer.NamespaceMetadata) initRow {
	return initRow{
		Namespace:           namespace,
		SchemaVersion:       metadata.Layer.SchemaVersion,
		InitState:           metadata.Layer.InitState,
		InitLagRows:         metadata.Layer.InitLagRows,
		ShardCount:          metadata.Layer.ShardCount,
		ShardState:          metadata.Layer.ShardState,
		ShardLagRows:        metadata.Layer.ShardLagRows,
		ScatterGatherActive: metadata.Layer.ScatterGatherActive,
	}
}

func watchInit(ctx context.Context, out io.Writer, client *hevlayer.Client, namespace string, interval time.Duration) (initRow, error) {
	if interval <= 0 {
		interval = 2 * time.Second
	}
	for {
		metadata, err := client.GetNamespaceMetadata(ctx, namespace)
		if err != nil {
			return initRow{}, err
		}
		row := metadataInitRow(namespace, metadata)
		fmt.Fprintf(
			out,
			"shard_lag_rows=%d shard_state=%s scatter_gather_active=%t\n",
			row.ShardLagRows,
			row.ShardState,
			row.ScatterGatherActive,
		)
		if row.ScatterGatherActive {
			fmt.Fprintln(out, "scatter/gather active")
			return row, nil
		}
		select {
		case <-ctx.Done():
			return initRow{}, ctx.Err()
		case <-time.After(interval):
		}
	}
}

func emitInitRow(out io.Writer, format string, row initRow) error {
	switch format {
	case output.JSON:
		return output.WriteJSON(out, row)
	case output.Names:
		fmt.Fprintln(out, row.Namespace)
		return nil
	default:
		rows := [][]string{
			{"NAMESPACE", row.Namespace},
			{"SCHEMA_VERSION", output.FormatInt(row.SchemaVersion)},
			{"INIT_STATE", row.InitState},
			{"INIT_LAG_ROWS", output.FormatInt(row.InitLagRows)},
			{"SHARD_COUNT", output.FormatInt(row.ShardCount)},
			{"SHARD_STATE", row.ShardState},
			{"SHARD_LAG_ROWS", output.FormatInt(row.ShardLagRows)},
			{"SCATTER_GATHER_ACTIVE", fmt.Sprintf("%t", row.ScatterGatherActive)},
		}
		return output.WriteFields(out, rows)
	}
}
