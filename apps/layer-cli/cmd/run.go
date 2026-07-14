package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/hev/layer/apps/layer-cli/internal/kube"
	"github.com/hev/layer/apps/layer-cli/internal/output"
	hevlayer "github.com/hev/layer/clients/go"
	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"sigs.k8s.io/yaml"
)

func newRunCommand(app App, flags *globalFlags) *cobra.Command {
	var filename string
	var index string
	var detach bool
	var noApply bool
	var rm bool
	var pollInterval time.Duration
	var contextName string
	var kubeNamespace string
	cmd := &cobra.Command{
		Use:   "run -f FUNCTION.yaml",
		Short: "Apply a Function CR and run it to drained",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if filename == "" {
				return usagef("usage: layer run -f FUNCTION.yaml [--index INDEX] [--detach] [--no-apply]")
			}
			resolved, format, err := app.resolve(cmd, flags, runFlags{
				contextName:   contextName,
				kubeNamespace: kubeNamespace,
			})
			if err != nil {
				return err
			}
			client := app.clientFor(resolved)
			return app.runFunction(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), client, runOptions{
				filename:      filename,
				index:         index,
				detach:        detach,
				noApply:       noApply,
				rm:            rm,
				pollInterval:  pollInterval,
				contextName:   resolved.KubeContext,
				kubeNamespace: resolved.KubeNamespace,
				outputFormat:  format,
			})
		},
	}
	cmd.Flags().StringVarP(&filename, "filename", "f", "", "Function CR manifest")
	cmd.Flags().StringVar(&index, "index", "", "Override target index")
	cmd.Flags().BoolVar(&detach, "detach", false, "Return after discovery is triggered")
	cmd.Flags().BoolVar(&noApply, "no-apply", false, "Skip Kubernetes server-side apply")
	cmd.Flags().StringVar(&contextName, "context", "", "Kubeconfig context")
	cmd.Flags().StringVar(&kubeNamespace, "kube-namespace", "", "Kubernetes namespace")
	cmd.Flags().BoolVar(&rm, "rm", false, "Delete gateway registration and Function CR after a clean drain")
	cmd.Flags().DurationVar(&pollInterval, "poll-interval", 2*time.Second, "Status polling interval")
	return cmd
}

type runOptions struct {
	filename      string
	index         string
	detach        bool
	noApply       bool
	rm            bool
	pollInterval  time.Duration
	contextName   string
	kubeNamespace string
	outputFormat  string
}

func (app App) runFunction(ctx context.Context, stdout, stderr interface {
	Write([]byte) (int, error)
}, client *hevlayer.Client, opts runOptions) error {
	manifest, err := loadFunctionManifest(opts.filename, opts.index)
	if err != nil {
		return err
	}

	if !opts.noApply {
		if err := applyFunction(ctx, manifest, opts.contextName, opts.kubeNamespace); err != nil {
			return err
		}
		fmt.Fprintf(stderr, "applied Function CR %s\n", manifest.name)
	}

	spec, err := manifest.gatewaySpec()
	if err != nil {
		return err
	}

	before, err := statusSweeps(ctx, client, manifest.name)
	if err != nil {
		before = 0
	}
	if err := createOrConfirmUdf(ctx, client, manifest.name, spec); err != nil {
		return err
	}
	if before == 0 {
		before, _ = statusSweeps(ctx, client, manifest.name)
	}
	if _, err := client.DiscoverUdf(ctx, manifest.name, &hevlayer.UdfDiscoverRequest{}); err != nil {
		return err
	}
	fmt.Fprintf(stderr, "triggered discovery sweep for UDF %s\n", manifest.name)

	if opts.detach {
		fmt.Fprintf(stdout, "UDF %s submitted. Watch with: layer udf get %s --watch\n", manifest.name, manifest.name)
		return nil
	}

	status, err := watchUdf(ctx, stderr, client, manifest.name, before, opts.pollInterval)
	if err != nil {
		return err
	}
	if status.FailedCount > 0 {
		return fmt.Errorf("UDF %s drained with %d failed rows; fix inputs or worker output, then run layer udf reset-failed when that verb lands", manifest.name, status.FailedCount)
	}
	if opts.rm {
		if _, err := client.DeleteUdf(ctx, manifest.name); err != nil {
			return err
		}
		if !opts.noApply {
			if err := deleteFunction(ctx, manifest, opts.contextName, opts.kubeNamespace); err != nil {
				return err
			}
		}
	}
	return emitUdfStatus(stdout, opts.outputFormat, status)
}

func watchUdf(ctx context.Context, out interface {
	Write([]byte) (int, error)
}, client *hevlayer.Client, id string, beforeSweeps int64, interval time.Duration) (*hevlayer.UdfStatus, error) {
	for {
		status, err := client.GetUdfStatus(ctx, id)
		if err != nil {
			return nil, err
		}
		fmt.Fprintf(
			out,
			"pending=%d processing=%d failed=%d rate=%.2f sweeps=%d\n",
			status.PendingCount,
			status.ProcessingCount,
			status.FailedCount,
			status.IndexedRatePerMin,
			status.Discovery.SweepsCompleted,
		)
		if status.Discovery.SweepsCompleted > beforeSweeps && status.PendingCount == 0 && status.ProcessingCount == 0 {
			return status, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(interval):
		}
	}
}

type functionManifest struct {
	name      string
	namespace string
	object    map[string]interface{}
}

func loadFunctionManifest(filename string, indexOverride string) (*functionManifest, error) {
	raw, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	var object map[string]interface{}
	if err := yaml.Unmarshal(raw, &object); err != nil {
		return nil, err
	}
	if strings.EqualFold(stringValue(object["kind"]), "List") {
		return nil, fmt.Errorf("layer run accepts one Function manifest, not a List")
	}
	if kind := stringValue(object["kind"]); kind != "Function" {
		return nil, fmt.Errorf("layer run requires kind: Function, got %q", kind)
	}
	metadata := ensureMap(object, "metadata")
	name := stringValue(metadata["name"])
	if name == "" {
		return nil, fmt.Errorf("metadata.name is required on the Function manifest")
	}
	spec := ensureMap(object, "spec")
	if indexOverride != "" {
		spec["targetNamespaces"] = []interface{}{indexOverride}
	}
	return &functionManifest{
		name:      name,
		namespace: stringValue(metadata["namespace"]),
		object:    object,
	}, nil
}

func (manifest *functionManifest) gatewaySpec() (hevlayer.UdfSpec, error) {
	spec := camelToSnake(ensureMap(manifest.object, "spec")).(map[string]interface{})
	delete(spec, "paused")
	delete(spec, "scaling")
	delete(spec, "index_selector")
	delete(spec, "pod_spec")
	if worker, ok := spec["worker"].(map[string]interface{}); ok {
		delete(worker, "pod_spec")
	}

	raw, err := json.Marshal(spec)
	if err != nil {
		return hevlayer.UdfSpec{}, err
	}
	var typed hevlayer.UdfSpec
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if err := decoder.Decode(&typed); err != nil {
		return hevlayer.UdfSpec{}, err
	}
	return typed, nil
}

func createOrConfirmUdf(ctx context.Context, client *hevlayer.Client, id string, spec hevlayer.UdfSpec) error {
	_, err := client.CreateUdf(ctx, &hevlayer.CreateUdfRequest{ID: id, Spec: spec})
	if err == nil {
		return nil
	}
	var layerErr *hevlayer.HevlayerError
	if !errors.As(err, &layerErr) || layerErr.StatusCode != 409 {
		return err
	}
	existing, getErr := client.GetUdf(ctx, id)
	if getErr != nil {
		return getErr
	}
	if canonicalJSON(existing.Udf.Spec) == canonicalJSON(spec) {
		return nil
	}
	return fmt.Errorf("UDF %s already exists with a different spec\nexisting: %s\ndesired: %s\ndelete it and re-run, or bump spec.version", id, canonicalJSON(existing.Udf.Spec), canonicalJSON(spec))
}

func statusSweeps(ctx context.Context, client *hevlayer.Client, id string) (int64, error) {
	status, err := client.GetUdfStatus(ctx, id)
	if err != nil {
		return 0, err
	}
	return status.Discovery.SweepsCompleted, nil
}

func applyFunction(ctx context.Context, manifest *functionManifest, contextName string, kubeNamespace string) error {
	clientConfig, namespace, err := kube.RestConfig(contextName, kubeNamespace)
	if err != nil {
		return err
	}
	if manifest.namespace != "" && kubeNamespace == "" {
		namespace = manifest.namespace
	}
	if namespace == "" {
		namespace = "default"
	}
	metadata := ensureMap(manifest.object, "metadata")
	metadata["namespace"] = namespace
	dyn, err := dynamic.NewForConfig(clientConfig)
	if err != nil {
		return err
	}
	payload, err := json.Marshal(manifest.object)
	if err != nil {
		return err
	}
	force := true
	_, err = dyn.Resource(kube.FunctionGVR()).Namespace(namespace).Patch(
		ctx,
		manifest.name,
		types.ApplyPatchType,
		payload,
		metav1.PatchOptions{FieldManager: "layer-cli", Force: &force},
	)
	if err == nil {
		manifest.namespace = namespace
	}
	return err
}

func deleteFunction(ctx context.Context, manifest *functionManifest, contextName string, kubeNamespace string) error {
	clientConfig, namespace, err := kube.RestConfig(contextName, kubeNamespace)
	if err != nil {
		return err
	}
	if manifest.namespace != "" && kubeNamespace == "" {
		namespace = manifest.namespace
	}
	if namespace == "" {
		namespace = "default"
	}
	dyn, err := dynamic.NewForConfig(clientConfig)
	if err != nil {
		return err
	}
	return dyn.Resource(kube.FunctionGVR()).Namespace(namespace).Delete(ctx, manifest.name, metav1.DeleteOptions{})
}

func camelToSnake(value interface{}) interface{} {
	switch typed := value.(type) {
	case map[string]interface{}:
		out := make(map[string]interface{}, len(typed))
		for key, item := range typed {
			out[toSnake(key)] = camelToSnake(item)
		}
		return out
	case []interface{}:
		out := make([]interface{}, len(typed))
		for i, item := range typed {
			out[i] = camelToSnake(item)
		}
		return out
	default:
		return typed
	}
}

func toSnake(value string) string {
	var out strings.Builder
	for i, r := range value {
		if r >= 'A' && r <= 'Z' {
			if i > 0 {
				out.WriteByte('_')
			}
			out.WriteRune(r + ('a' - 'A'))
		} else {
			out.WriteRune(r)
		}
	}
	return out.String()
}

func ensureMap(parent map[string]interface{}, key string) map[string]interface{} {
	value, ok := parent[key].(map[string]interface{})
	if !ok {
		value = map[string]interface{}{}
		parent[key] = value
	}
	return value
}

func stringValue(value interface{}) string {
	switch typed := value.(type) {
	case string:
		return typed
	default:
		return ""
	}
}

func canonicalJSON(value interface{}) string {
	encoded, err := json.Marshal(value)
	if err != nil {
		return fmt.Sprint(value)
	}
	var normalized interface{}
	if err := json.Unmarshal(encoded, &normalized); err != nil {
		return string(encoded)
	}
	encoded, err = json.Marshal(normalized)
	if err != nil {
		return fmt.Sprint(value)
	}
	return string(encoded)
}

func emitUdfStatus(out interface {
	Write([]byte) (int, error)
}, format string, status *hevlayer.UdfStatus) error {
	if format == output.JSON {
		return output.WriteJSON(out, status)
	}
	if format == output.Names {
		fmt.Fprintln(out, status.UdfID)
		return nil
	}
	rows := [][]string{{
		status.UdfID,
		output.FormatInt(status.PendingCount),
		output.FormatInt(status.ProcessingCount),
		output.FormatInt(status.FailedCount),
		output.FormatInt(status.Discovery.SweepsCompleted),
		output.FormatFloat(status.IndexedRatePerMin),
	}}
	return output.WriteTable(out, []string{"UDF", "PENDING", "RUNNING", "FAILED", "SWEEPS", "RATE/MIN"}, rows)
}
