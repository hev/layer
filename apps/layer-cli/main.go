package main

import (
	"context"
	"os"

	"github.com/hev/layer/apps/layer-cli/cmd"
	"golang.org/x/term"
)

// version is overridden at build time with -ldflags "-X main.version=...".
// Without it, the command package falls back to the module version stamped by
// go install.
var version = "dev"

func main() {
	os.Exit(cmd.Execute(context.Background(), os.Args[1:], cmd.Options{
		Stdin:            os.Stdin,
		Stdout:           os.Stdout,
		Stderr:           os.Stderr,
		Env:              cmd.EnvironMap(os.Environ()),
		StdinIsTerminal:  term.IsTerminal(int(os.Stdin.Fd())),
		StdoutIsTerminal: term.IsTerminal(int(os.Stdout.Fd())),
		Version:          version,
	}))
}
