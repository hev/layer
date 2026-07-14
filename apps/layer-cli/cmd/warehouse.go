package cmd

import (
	"fmt"
	"io"

	"github.com/hev/layer/apps/layer-cli/internal/output"
	hevlayer "github.com/hev/layer/clients/go"
	"github.com/spf13/cobra"
)

func newWarehouseCommand(app App, flags *globalFlags) *cobra.Command {
	warehouseCmd := &cobra.Command{
		Use:   "warehouse",
		Short: "Observe warehouses",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return usagef("usage: layer warehouse <list|get> ...")
		},
	}
	warehouseCmd.AddCommand(newWarehouseListCommand(app, flags))
	warehouseCmd.AddCommand(newWarehouseGetCommand(app, flags))
	return warehouseCmd
}

func newWarehouseListCommand(app App, flags *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List warehouses",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			resolved, format, err := app.resolve(cmd, flags, runFlags{})
			if err != nil {
				return err
			}
			list, err := app.clientFor(resolved).ListWarehouses(cmd.Context())
			if err != nil {
				return err
			}
			return emitWarehouses(cmd.OutOrStdout(), format, list.Warehouses)
		},
	}
}

func newWarehouseGetCommand(app App, flags *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "get NAME",
		Short: "Show one warehouse",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			resolved, format, err := app.resolve(cmd, flags, runFlags{})
			if err != nil {
				return err
			}
			warehouse, err := app.clientFor(resolved).GetWarehouse(cmd.Context(), args[0])
			if err != nil {
				return err
			}
			return emitWarehouseDetail(cmd.OutOrStdout(), format, *warehouse)
		},
	}
}

func emitWarehouses(out io.Writer, format string, warehouses []hevlayer.Warehouse) error {
	switch format {
	case output.JSON:
		return output.WriteJSON(out, warehouses)
	case output.Names:
		for _, warehouse := range warehouses {
			fmt.Fprintln(out, warehouse.Name)
		}
		return nil
	default:
		tableRows := make([][]string, 0, len(warehouses))
		for _, warehouse := range warehouses {
			tableRows = append(tableRows, []string{
				warehouse.Name,
				warehouse.Kind,
				dash(string(warehouse.Status.Phase)),
				dash(warehouse.Status.VerifiedAt),
				output.FormatInt(warehouse.Status.Consumers.Pipelines),
				output.FormatInt(warehouse.Status.Consumers.ApiKeys),
			})
		}
		return output.WriteTable(out, []string{"NAME", "KIND", "PHASE", "VERIFIED_AT", "PIPELINES", "API_KEYS"}, tableRows)
	}
}

func emitWarehouseDetail(out io.Writer, format string, warehouse hevlayer.Warehouse) error {
	switch format {
	case output.JSON:
		return output.WriteJSON(out, warehouse)
	case output.Names:
		fmt.Fprintln(out, warehouse.Name)
		return nil
	default:
		snowflake := warehouse.Snowflake
		rows := [][]string{
			{"NAME", warehouse.Name},
			{"KIND", warehouse.Kind},
			{"ACCOUNT", dash(snowflake.Account)},
			{"USER", dash(snowflake.User)},
			{"ROLE", dash(snowflake.Role)},
			{"WAREHOUSE", dash(snowflake.Warehouse)},
			{"KEY_PAIR_SECRET", dash(snowflake.KeyPairSecretRef.Name)},
			{"VERIFY_INTERVAL", dash(warehouse.VerifyInterval)},
			{"PHASE", dash(string(warehouse.Status.Phase))},
			{"VERIFIED_AT", dash(warehouse.Status.VerifiedAt)},
			{"FAILURE_REASON", dash(warehouse.Status.FailureReason)},
			{"CONSUMER_PIPELINES", output.FormatInt(warehouse.Status.Consumers.Pipelines)},
			{"CONSUMER_API_KEYS", output.FormatInt(warehouse.Status.Consumers.ApiKeys)},
			{"OBSERVED_GENERATION", output.FormatInt(warehouse.Status.ObservedGeneration)},
		}
		return output.WriteFields(out, rows)
	}
}
