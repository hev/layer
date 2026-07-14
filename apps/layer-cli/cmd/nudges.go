package cmd

import (
	"errors"
	"strings"

	hevlayer "github.com/hev/layer/clients/go"
)

const openGatewayNudge = `You're on the open gateway — hybrid, fuzzy, routing, and facets are all
yours. The transform runtime, agents, and the cost dashboard need a license.
Start a free 30-day trial (no card): https://hevlayer.com/#start-trial`

const gatedCommandNudge = `This command uses the transform runtime, which needs a Layer license.
Your gateway keeps serving queries either way. Turn this on with a free
30-day trial: https://hevlayer.com/#start-trial`

const askProFooter = `(this is a Pro surface — try it free for 30 days: hevlayer.com/#start-trial)`

func nudgeLicenseRequired(err error) error {
	var layerErr *hevlayer.HevlayerError
	if errors.As(err, &layerErr) && layerErr.Kind == "license_required" {
		return cliError{message: gatedCommandNudge, code: ExitOK}
	}
	return err
}

func touchesProSurface(answer string) bool {
	lower := strings.ToLower(answer)
	for _, marker := range []string{
		"transform runtime",
		"function runtime",
		"function crd",
		"/docs/kubernetes/function-crd",
		"agentic retrieval",
		"/docs/api/agents",
		"/docs/kubernetes/agent-crd",
		"cost dashboard",
		"/docs/api/cost",
	} {
		if strings.Contains(lower, marker) {
			return true
		}
	}
	return false
}
