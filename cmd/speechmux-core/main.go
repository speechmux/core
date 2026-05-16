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
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/lmittmann/tint"
	"github.com/speechmux/core/internal/config"
	"github.com/speechmux/core/internal/ctl"
	"github.com/speechmux/core/internal/runtime"
)

// newLogHandler creates a slog.Handler for the given format string.
// Accepted formats: "json" (default), "text", "color".
// Unknown values fall back to "json".
func newLogHandler(format string, level slog.Leveler, out io.Writer) slog.Handler {
	opts := &slog.HandlerOptions{Level: level, AddSource: true}
	switch format {
	case "text":
		return slog.NewTextHandler(out, opts)
	case "color":
		return tint.NewHandler(out, &tint.Options{
			Level:      level,
			TimeFormat: "15:04:05.000",
			AddSource:  true,
		})
	default:
		return slog.NewJSONHandler(out, opts)
	}
}

// profileFlags implements flag.Value for collecting repeated --profile flags.
type profileFlags []string

func (f *profileFlags) String() string { return strings.Join(*f, ",") }
func (f *profileFlags) Set(v string) error {
	*f = append(*f, v)
	return nil
}

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

	// Bootstrap with INFO so config-load errors are visible, then re-apply
	// the level from logging.level in core.yaml.
	logLevel := new(slog.LevelVar)
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		Level: logLevel,
	})))

	if err := run(*cfgPath, *pluginsPath, logLevel); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(cfgPath, pluginsPath string, logLevel *slog.LevelVar) error {
	cfgLoader, err := config.NewLoader(cfgPath)
	if err != nil {
		return err
	}

	cfg := cfgLoader.Load()

	// Apply log level from config now that core.yaml is loaded.
	if err := logLevel.UnmarshalText([]byte(cfg.Logging.Level)); err != nil {
		slog.Warn("invalid logging.level in config; defaulting to info", "value", cfg.Logging.Level)
	}

	// Re-apply handler with configured format (bootstrap used json; this may switch to text/color).
	slog.SetDefault(slog.New(newLogHandler(cfg.Logging.Format, logLevel, os.Stderr)))

	plugins, err := config.LoadPlugins(pluginsPath)
	if err != nil {
		return err
	}

	app, err := runtime.New(cfgLoader, plugins)
	if err != nil {
		return err
	}

	slog.Info("speechmux-core starting",
		"grpc_port", cfg.Server.GRPCPort,
		"http_port", cfg.Server.HTTPPort,
		"ws_port", cfg.Server.WSPort,
	)

	return app.Run(context.Background())
}

// runCtl dispatches ctl subcommands: start, status, stop.
func runCtl(args []string) error {
	fs := flag.NewFlagSet("ctl", flag.ExitOnError)
	workspacePath := fs.String("workspace", "workspace.yaml", "path to workspace.yaml")
	logFormat := fs.String("log-format", "json", "log format: json | text | color")
	var profiles profileFlags
	fs.Var(&profiles, "profile", "activate a workspace profile (repeatable; e.g. --profile silero --profile sherpa-onnx)")
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

	slog.SetDefault(slog.New(newLogHandler(*logFormat, slog.LevelInfo, os.Stderr)))

	cfg, err := ctl.LoadWorkspace(*workspacePath)
	if err != nil {
		return err
	}

	switch subcommand {
	case "start":
		filtered := cfg.FilterByProfiles(profiles)
		mgr := ctl.NewManager(filtered)
		ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
		defer cancel()
		slog.Info("ctl: starting processes", "workspace", *workspacePath, "profiles", []string(profiles))
		return mgr.Start(ctx)

	case "status":
		// Scan PID files written by `ctl start` — no profile flags needed.
		ctl.PrintStatuses(ctl.StatusFromDir(cfg.StateDir))
		return nil

	case "stop":
		mgr := ctl.NewManager(cfg.FilterByProfiles(profiles))
		return mgr.Stop()

	default:
		return fmt.Errorf("ctl: unknown subcommand %q (want: start | status | stop)", subcommand)
	}
}
