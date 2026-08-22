// Command connector runs the WhatsApp session connector.
//
// Two subcommands in this build: `serve` runs the instance, `healthcheck` is what the
// container's HEALTHCHECK calls. `migrate`, `doctor` and the session subcommands
// arrive with the store, in M1.
package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/fazer-ai/whatsapp-connector/internal/app"
	"github.com/fazer-ai/whatsapp-connector/internal/observability"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "connector: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	command := "serve"
	if len(args) > 0 {
		command = args[0]
	}

	switch command {
	case "serve":
		return serve()
	case "healthcheck":
		return healthcheck()
	case "version":
		fmt.Println(app.Version)
		return nil
	case "help", "-h", "--help":
		usage()
		return nil
	default:
		usage()
		return fmt.Errorf("unknown command %q", command)
	}
}

func serve() error {
	cfg, err := app.LoadConfig(app.Hostname())
	if err != nil {
		return err
	}
	log := observability.NewLogger(os.Stdout, cfg.LogLevel, cfg.Instance, app.Version)

	connector, err := app.New(&cfg, log)
	if err != nil {
		return err
	}
	return connector.Run(context.Background())
}

// healthcheck asks the local instance, not the fleet: it answers "should this container
// be restarted", and a fleet-wide question would restart every replica at once.
func healthcheck() error {
	cfg, err := app.LoadConfig(app.Hostname())
	if err != nil {
		return err
	}
	addr := cfg.HTTPAddr
	if addr[0] == ':' {
		addr = "127.0.0.1" + addr
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/healthz", http.NoBody)
	if err != nil {
		return err
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return err
	}
	defer func() { _ = response.Body.Close() }()

	if response.StatusCode != http.StatusOK {
		return errors.New("healthz answered " + response.Status)
	}
	return nil
}

func usage() {
	fmt.Fprint(os.Stderr, `connector - WhatsApp session connector

Usage:
  connector serve         run the instance (default)
  connector healthcheck   ask the local instance whether it is up
  connector version       print the build version

Configuration is read from the environment; see the README.
`)
}
