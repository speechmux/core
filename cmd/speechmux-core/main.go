// Command speechmux-core is the SpeechMux Core process.
// It starts the gRPC, HTTP, and WebSocket servers and handles graceful shutdown.
//
// Usage:
//
//	speechmux-core [--config ...] [--plugins ...]
//	speechmux-core ctl start  [--workspace ...]
//	speechmux-core ctl status [--workspace ...]
//	speechmux-core ctl stop   [--workspace ...]
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/speechmux/core/internal/config"
	"github.com/speechmux/core/internal/ctl"
	"github.com/speechmux/core/internal/runtime"
)

func main() {
	// Dispatch ctl subcommand before flag.Parse so the ctl flags are handled
	// by their own FlagSet.
	if len(os.Args) > 1 && os.Args[1] == "ctl" {
		if err := runCtl(os.Args[2:]); err != nil {
			slog.Error("ctl error", "err", err)
			os.Exit(1)
		}
		return
	}

	cfgPath := flag.String("config", "config/core.yaml", "path to core.yaml")
	pluginsPath := flag.String("plugins", "config/plugins.yaml", "path to plugins.yaml")
	flag.Parse()

	// Bootstrap structured logging.
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	if err := run(*cfgPath, *pluginsPath); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(cfgPath, pluginsPath string) error {
	cfgLoader, err := config.NewLoader(cfgPath)
	if err != nil {
		return err
	}

	plugins, err := config.LoadPlugins(pluginsPath)
	if err != nil {
		return err
	}

	app, err := runtime.New(cfgLoader, plugins)
	if err != nil {
		return err
	}

	slog.Info("speechmux-core starting",
		"grpc_port", cfgLoader.Load().Server.GRPCPort,
		"http_port", cfgLoader.Load().Server.HTTPPort,
		"ws_port", cfgLoader.Load().Server.WSPort,
	)

	return app.Run(context.Background())
}

// runCtl dispatches ctl subcommands: start, status, stop.
func runCtl(args []string) error {
	fs := flag.NewFlagSet("ctl", flag.ExitOnError)
	workspacePath := fs.String("workspace", "config/workspace.yaml", "path to workspace.yaml")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: speechmux-core ctl <start|status|stop> [flags]\n\n")
		fs.PrintDefaults()
	}

	if len(args) == 0 {
		fs.Usage()
		os.Exit(2)
	}

	// Parse flags after the subcommand word.
	subcommand := args[0]
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}

	// Bootstrap structured logging for ctl commands.
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	cfg, err := ctl.LoadWorkspace(*workspacePath)
	if err != nil {
		return err
	}
	mgr := ctl.NewManager(cfg)

	switch subcommand {
	case "start":
		ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
		defer cancel()
		slog.Info("ctl: starting all processes", "workspace", *workspacePath)
		return mgr.Start(ctx)

	case "status":
		mgr.PrintStatus()
		return nil

	case "stop":
		return mgr.Stop()

	default:
		return fmt.Errorf("ctl: unknown subcommand %q (want: start | status | stop)", subcommand)
	}
}
