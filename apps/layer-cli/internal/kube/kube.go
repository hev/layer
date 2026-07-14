// Package kube applies the Function CRD that `layer run` manages, reading the
// kubeconfig directly. The gateway owns all runtime/queue state and serves it
// over HTTP, so the only cluster object the CLI still touches is the Function
// CR it server-side applies. Pipeline and index reads go through the gateway
// API; infra rules have no API and are not surfaced by the CLI.
package kube

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

const group = "hevlayer.com"

// FunctionGVR identifies the Function CRD applied by `layer run`.
func FunctionGVR() schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: group, Version: "v1alpha1", Resource: "functions"}
}

// RestConfig builds a client-go rest config from the default kubeconfig loading
// rules, honoring an explicit context override, and resolves the namespace
// (explicit > the context's default > ""). It backs the Function apply/delete
// path in the run command.
func RestConfig(contextName, namespace string) (*rest.Config, string, error) {
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	overrides := &clientcmd.ConfigOverrides{}
	if contextName != "" {
		overrides.CurrentContext = contextName
	}
	clientConfig := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, overrides)
	if namespace == "" {
		if resolved, _, err := clientConfig.Namespace(); err == nil {
			namespace = resolved
		}
	}
	cfg, err := clientConfig.ClientConfig()
	return cfg, namespace, err
}
