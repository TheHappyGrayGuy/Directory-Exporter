package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	// Built-in healthcheck mode: called by Docker as /directory-exporter -healthcheck
	if len(os.Args) == 2 && os.Args[1] == "-healthcheck" {
		addr := os.Getenv("LISTEN_ADDR")
		if addr == "" {
			addr = ":9200"
		}
		if addr[0] == ':' {
			addr = "localhost" + addr
		}
		resp, err := http.Get("http://" + addr + "/-/healthy")
		if err != nil || resp.StatusCode != 200 {
			os.Exit(1)
		}
		os.Exit(0)
	}

	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	cfg, err := LoadConfig(log)
	if err != nil {
		log.Error("configuration error", "error", err)
		os.Exit(1)
	}

	// Log each configured target so the operator can verify the config at a glance
	for _, t := range cfg.Targets {
		if len(t.Dirs) > 0 {
			log.Info("target configured",
				"base", t.Base,
				"dirs", fmt.Sprintf("%v", t.Dirs),
				"mode", "explicit")
		} else {
			log.Info("target configured",
				"base", t.Base,
				"max_depth", t.MaxDepth,
				"mode", "auto-discover")
		}
	}
	log.Info("directory-exporter starting",
		"targets", len(cfg.Targets),
		"scan_interval", cfg.ScanInterval,
		"scan_workers", cfg.ScanWorkers,
		"scan_timeout", cfg.ScanTimeout,
		"max_files_per_dir", cfg.MaxFilesPerDir,
		"max_stat_files", cfg.MaxStatFiles,
		"listen_addr", cfg.ListenAddr,
	)

	exp := NewExporter(cfg, log)
	exp.ValidateTargets()

	if err := exp.Discover(); err != nil {
		log.Warn("initial discovery errors (non-fatal)", "error", err)
	}

	go exp.RunScanLoop()
	go exp.RunDiscoveryLoop()

	srv := &http.Server{
		Addr:         cfg.ListenAddr,
		Handler:      NewMux(exp, log),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	go func() {
		log.Info("HTTP server listening", "addr", cfg.ListenAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("HTTP server error", "error", err)
			stop()
		}
	}()

	<-ctx.Done()
	log.Info("shutting down")
	stop()

	exp.Stop()

	shutCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutCtx)

	log.Info("stopped")
}
