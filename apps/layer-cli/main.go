package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/hev/layer/apps/layer-cli/cmd"
	"golang.org/x/term"
)

// version is overridden at build time with -ldflags "-X main.version=...".
// Without it, the command package falls back to the module version stamped by
// go install.
var version = "dev"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(cmd.Execute(ctx, os.Args[1:], cmd.Options{
		Stdin:            os.Stdin,
		Stdout:           os.Stdout,
		Stderr:           os.Stderr,
		Env:              cmd.EnvironMap(os.Environ()),
		StdinIsTerminal:  term.IsTerminal(int(os.Stdin.Fd())),
		StdoutIsTerminal: term.IsTerminal(int(os.Stdout.Fd())),
		Version:          version,
	}))
}
