package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type Exporter struct {
	cfg   *Config
	log   *slog.Logger
	cache *Cache
	wl    *WatchList

	scanMu   sync.Mutex
	stopCh   chan struct{}
	wg       sync.WaitGroup
	reloadCh chan struct{}
}

func NewExporter(cfg *Config, log *slog.Logger) *Exporter {
	return &Exporter{
		cfg:      cfg,
		log:      log,
		cache:    NewCache(),
		wl:       NewWatchList(),
		stopCh:   make(chan struct{}),
		reloadCh: make(chan struct{}, 1),
	}
}

// ValidateTargets checks that every configured base path exists and is readable.
// Logs a detailed error but does not abort startup — missing mounts are reported
// via scrape_success=0 in the metrics rather than crashing the process.
func (e *Exporter) ValidateTargets() {
	for _, t := range e.cfg.Targets {
		info, err := os.Stat(t.Base)
		if err != nil {
			e.log.Error("target base path not accessible — check your volume mount",
				"base", t.Base, "error", err)
			continue
		}
		if !info.IsDir() {
			e.log.Error("target base path is not a directory", "base", t.Base)
			continue
		}
		msg := fmt.Sprintf("base %q OK", t.Base)
		if len(t.Dirs) > 0 {
			msg += fmt.Sprintf(", watching %d explicit dirs", len(t.Dirs))
		} else {
			msg += fmt.Sprintf(", auto-discover up to depth %d", t.MaxDepth)
		}
		e.log.Info("target validated", "detail", msg)
	}
}

func (e *Exporter) Discover() error {
	entries, err := discoverTargets(e.cfg.Targets)
	e.wl.Set(entries)
	e.cache.SetWatchedTotal(len(entries))
	if err != nil {
		e.log.Error("discovery error", "error", err)
	}
	e.log.Info("watch list updated", "directories", len(entries))
	return err
}

func (e *Exporter) TriggerReload() {
	e.cache.IncrReload()
	select {
	case e.reloadCh <- struct{}{}:
	default:
	}
}

func (e *Exporter) Stop() {
	close(e.stopCh)
	e.wg.Wait()
}

func (e *Exporter) IsReady() bool           { return e.cache.IsReady() }
func (e *Exporter) Snapshot() CacheSnapshot { return e.cache.Snapshot() }

func (e *Exporter) RunScanLoop() {
	e.wg.Add(1)
	defer e.wg.Done()

	e.ScanAll()

	ticker := time.NewTicker(e.cfg.ScanInterval)
	defer ticker.Stop()

	for {
		select {
		case <-e.stopCh:
			return
		case <-ticker.C:
			e.ScanAll()
		case <-e.reloadCh:
			e.log.Info("reload signal — re-discovering")
			if err := e.Discover(); err != nil {
				e.log.Error("reload discovery failed", "error", err)
			}
			e.ScanAll()
		}
	}
}

func (e *Exporter) RunDiscoveryLoop() {
	e.wg.Add(1)
	defer e.wg.Done()

	ticker := time.NewTicker(e.cfg.DiscoveryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-e.stopCh:
			return
		case <-ticker.C:
			e.log.Info("periodic re-discovery starting")
			if err := e.Discover(); err != nil {
				e.log.Error("periodic discovery failed", "error", err)
			}
			select {
			case e.reloadCh <- struct{}{}:
			default:
			}
		}
	}
}

// ScanAll fans out workers with a hard wall-clock timeout (SCAN_TIMEOUT).
func (e *Exporter) ScanAll() {
	e.scanMu.Lock()
	defer e.scanMu.Unlock()

	watched := e.wl.Get()
	if len(watched) == 0 {
		e.cache.SetBatch(make(map[string]DirMetrics), time.Now(), 0)
		e.log.Warn("watch list is empty — check your targets config and volume mounts")
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), e.cfg.ScanTimeout)
	defer cancel()

	cycleStart := time.Now()
	results := make(map[string]DirMetrics, len(watched))
	var resultsMu sync.Mutex
	var errCount atomic.Uint64
	var truncCount atomic.Uint64

	sem := make(chan struct{}, e.cfg.ScanWorkers)
	var wg sync.WaitGroup

	for _, we := range watched {
		if ctx.Err() != nil {
			break
		}

		wg.Add(1)
		sem <- struct{}{}

		go func(entry WatchEntry) {
			defer wg.Done()
			defer func() { <-sem }()

			res := scanDir(ctx, entry.AbsPath, e.cfg.MaxFilesPerDir, e.cfg.MaxStatFiles)
			if res.Err != nil {
				errCount.Add(1)
				e.log.Debug("scan error", "path", entry.AbsPath, "error", res.Err)
			}
			if res.Truncated {
				truncCount.Add(1)
				e.log.Warn("scan truncated",
					"path", entry.AbsPath,
					"files_read", res.FileCount,
					"reason", "timeout or MAX_FILES_PER_DIR cap")
			}

			dm := DirMetrics{
				FileCount:          res.FileCount,
				OldestTimestampSec: res.OldestTimestamp,
				NewestTimestampSec: res.NewestTimestamp,
				ScanDurationSec:    res.ScanDuration,
				ScanSuccess:        res.ScanSuccess,
				Truncated:          res.Truncated,
				Labels:             entry.Labels,
			}
			resultsMu.Lock()
			results[entry.AbsPath] = dm
			resultsMu.Unlock()
		}(we)
	}

	wg.Wait()

	elapsed := time.Since(cycleStart)
	timedOut := ctx.Err() != nil
	errors := errCount.Load()
	truncs := truncCount.Load()

	e.cache.SetBatch(results, time.Now(), errors)

	if timedOut {
		e.log.Warn("scan cycle timed out — partial results written to cache",
			"timeout", e.cfg.ScanTimeout,
			"completed", len(results),
			"total", len(watched),
			"truncated_dirs", truncs,
			"duration", elapsed.Round(time.Millisecond),
		)
	} else {
		e.log.Info("scan cycle complete",
			"directories", len(watched),
			"errors", errors,
			"truncated_dirs", truncs,
			"duration", elapsed.Round(time.Millisecond),
		)
	}
}
