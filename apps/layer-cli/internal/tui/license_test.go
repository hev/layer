package tui

import (
	"strings"
	"testing"

	hevlayer "github.com/hev/layer/clients/go"
)

func TestLicenseBannerCopyAndDays(t *testing.T) {
	tests := []struct {
		state   string
		seconds int64
		grace   int64
		want    string
	}{
		{"licensed", 18*86400 - 1, 0, "● licensed · trial · 18 days left"},
		{"grace", 0, 12*86400 - 1, "● grace · trial expired · 12 days of grace left · renew: hevlayer.com"},
		{"floor", 0, 0, "● open gateway · Pro surfaces off · start a trial: hevlayer.com/#start-trial"},
	}
	for _, test := range tests {
		model := Model{license: &hevlayer.LicenseState{Gateway: hevlayer.LicenseSurfaceState{
			State: test.state, SecondsToDeadline: test.seconds, GraceSecondsRemaining: test.grace,
		}}}
		if got := model.licenseBanner(); !strings.Contains(got, test.want) {
			t.Fatalf("state=%s banner=%q want %q", test.state, got, test.want)
		}
	}
}
