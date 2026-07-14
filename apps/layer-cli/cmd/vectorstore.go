package cmd

import (
	"context"
	"fmt"
	"io"

	"github.com/hev/layer/apps/layer-cli/internal/output"
	hevlayer "github.com/hev/layer/clients/go"
	"github.com/spf13/cobra"
)

func newVectorstoreCommand(app App, flags *globalFlags) *cobra.Command {
	vectorstoreCmd := &cobra.Command{
		Use:   "vectorstore",
		Short: "Observe vector stores",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return usagef("usage: layer vectorstore <list|get> ...")
		},
	}
	vectorstoreCmd.AddCommand(newVectorstoreListCommand(app, flags))
	vectorstoreCmd.AddCommand(newVectorstoreGetCommand(app, flags))
	return vectorstoreCmd
}

func newVectorstoreListCommand(app App, flags *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List vector stores",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			resolved, format, err := app.resolve(cmd, flags, runFlags{})
			if err != nil {
				return err
			}
			list, err := app.clientFor(resolved).ListVectorstores(cmd.Context())
			if err != nil {
				return err
			}
			return emitVectorstores(cmd.OutOrStdout(), format, list.Vectorstores)
		},
	}
}

func newVectorstoreGetCommand(app App, flags *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "get [NAME]",
		Short: "Show one vector store",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			resolved, format, err := app.resolve(cmd, flags, runFlags{})
			if err != nil {
				return err
			}
			client := app.clientFor(resolved)
			var store *hevlayer.VectorStore
			if len(args) == 0 {
				store, err = defaultVectorstore(cmd.Context(), client)
			} else {
				store, err = client.GetVectorstore(cmd.Context(), args[0])
			}
			if err != nil {
				return err
			}
			return emitVectorstoreDetail(cmd.OutOrStdout(), format, *store)
		},
	}
}

func defaultVectorstore(ctx context.Context, client *hevlayer.Client) (*hevlayer.VectorStore, error) {
	list, err := client.ListVectorstores(ctx)
	if err != nil {
		return nil, err
	}
	if len(list.Vectorstores) == 0 {
		return nil, fmt.Errorf("no vector stores found")
	}
	for i := range list.Vectorstores {
		if list.Vectorstores[i].Default {
			return &list.Vectorstores[i], nil
		}
	}
	if len(list.Vectorstores) == 1 {
		return &list.Vectorstores[0], nil
	}
	return nil, fmt.Errorf("no default vector store found; pass a name")
}

func emitVectorstores(out io.Writer, format string, stores []hevlayer.VectorStore) error {
	switch format {
	case output.JSON:
		return output.WriteJSON(out, stores)
	case output.Names:
		for _, store := range stores {
			fmt.Fprintln(out, store.Name)
		}
		return nil
	default:
		tableRows := make([][]string, 0, len(stores))
		for _, store := range stores {
			tableRows = append(tableRows, []string{
				store.Name,
				store.Kind,
				fmt.Sprintf("%t", store.Default),
				store.Endpoint.Region,
				fmt.Sprintf("%t", store.Status.Reachable),
				dash(store.Turbopuffer.OrgID),
			})
		}
		return output.WriteTable(out, []string{"NAME", "KIND", "DEFAULT", "REGION", "REACHABLE", "ORG"}, tableRows)
	}
}

func emitVectorstoreDetail(out io.Writer, format string, store hevlayer.VectorStore) error {
	switch format {
	case output.JSON:
		return output.WriteJSON(out, store)
	case output.Names:
		fmt.Fprintln(out, store.Name)
		return nil
	default:
		rows := [][]string{
			{"NAME", store.Name},
			{"KIND", store.Kind},
			{"DEFAULT", fmt.Sprintf("%t", store.Default)},
			{"ENDPOINT_URL", dash(store.Endpoint.Url)},
			{"REGION", dash(store.Endpoint.Region)},
			{"ORG_ID", dash(store.Turbopuffer.OrgID)},
			{"CREDENTIAL_SECRET", dash(store.Credential.SecretRef.Name)},
			{"CREDENTIAL_KEY", dash(store.Credential.SecretRef.Key)},
			{"INBOUND_AUTH", dash(store.InboundAuth.Mode)},
			{"REACHABLE", fmt.Sprintf("%t", store.Status.Reachable)},
			{"OBSERVED_GENERATION", output.FormatInt(store.Status.ObservedGeneration)},
			{"TURBOPUFFER_URL", dash(store.TurbopufferUrl)},
		}
		return output.WriteFields(out, rows)
	}
}
