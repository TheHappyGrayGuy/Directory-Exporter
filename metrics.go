package main

import (
	"fmt"
	"io"
	"sort"
	"strings"
)

// prometheusContentType is the value of the Content-Type header expected
// by Prometheus scrapers for the text exposition format v0.0.4.
const prometheusContentType = "text/plain; version=0.0.4; charset=utf-8"

// RenderMetrics writes the current CacheSnapshot to w using the Prometheus
// text exposition format (https://prometheus.io/docs/instrumenting/exposition_formats/).
//
// Zero disk I/O is performed — all data comes from the snapshot.
//
// Cold-start behaviour: when snap.Ready is false, only directory_cache_ready{0}
// is emitted. This prevents Prometheus alert rules from firing on zero-valued
// metrics during the warm-up window before the first scan completes.
func RenderMetrics(w io.Writer, snap CacheSnapshot) {
	// ── directory_cache_ready (always present) ────────────────────────────────
	help(w, "directory_cache_ready",
		"1 = first scan complete, 0 = cache is still warming up (cold start).")
	typ(w, "directory_cache_ready", "gauge")

	readyVal := 0.0
	if snap.Ready {
		readyVal = 1.0
	}
	fmt.Fprintf(w, "directory_cache_ready %g\n", readyVal)

	if !snap.Ready {
		// Omit all other metric families until the cache is warm.
		// The /-/ready endpoint will return 503 during this window, so
		// Kubernetes / load balancers can hold traffic accordingly.
		return
	}

	// ── Control-plane metrics ─────────────────────────────────────────────────

	help(w, "directory_last_scan_timestamp_seconds",
		"Unix timestamp of when the metrics cache was last refreshed.")
	typ(w, "directory_last_scan_timestamp_seconds", "gauge")
	fmt.Fprintf(w, "directory_last_scan_timestamp_seconds %g\n",
		float64(snap.LastScanTime.Unix()))

	help(w, "directory_watched_total",
		"Total number of directories currently on the Watch List.")
	typ(w, "directory_watched_total", "gauge")
	fmt.Fprintf(w, "directory_watched_total %d\n", snap.WatchedTotal)

	help(w, "directory_scan_errors_total",
		"Cumulative count of scan errors (permission denied, vanished directories, etc.).")
	typ(w, "directory_scan_errors_total", "counter")
	fmt.Fprintf(w, "directory_scan_errors_total %d\n", snap.ScanErrors)

	help(w, "directory_reload_total",
		"Cumulative count of /-/reload triggers received by the exporter.")
	typ(w, "directory_reload_total", "counter")
	fmt.Fprintf(w, "directory_reload_total %d\n", snap.ReloadTotal)

	if len(snap.Entries) == 0 {
		return
	}

	// ✅ FIX: sort entries by (base, stream, type) so metric output is
	// deterministic across scrapes. snap.Entries comes from map iteration
	// in Snapshot(), which is random in Go. Non-deterministic ordering makes
	// `curl /metrics | diff` noisy and complicates debugging.
	sorted := make([]DirMetrics, len(snap.Entries))
	copy(sorted, snap.Entries)
	sort.Slice(sorted, func(i, j int) bool {
		ki := sorted[i].Labels.Base + "/" + sorted[i].Labels.Stream + "/" + sorted[i].Labels.Type
		kj := sorted[j].Labels.Base + "/" + sorted[j].Labels.Stream + "/" + sorted[j].Labels.Type
		return ki < kj
	})

	// ── Data-plane metrics (one time-series per watched directory) ────────────
	// Each metric family is emitted with a single HELP + TYPE header followed
	// by all instances, as required by the text format specification.

	help(w, "directory_file_count",
		"Total number of regular files directly in the monitored directory (non-recursive).")
	typ(w, "directory_file_count", "gauge")
	for _, dm := range sorted {
		fmt.Fprintf(w, "directory_file_count{base=%s,stream=%s,type=%s} %d\n",
			lv(dm.Labels.Base), lv(dm.Labels.Stream), lv(dm.Labels.Type),
			dm.FileCount)
	}

	// Oldest and newest timestamps are only meaningful when files exist.
	// Omitting them for empty directories prevents false "stuck queue" alerts.
	help(w, "directory_oldest_file_timestamp_seconds",
		"Modification time (Unix epoch) of the oldest file. Detects stuck queues.")
	typ(w, "directory_oldest_file_timestamp_seconds", "gauge")
	for _, dm := range sorted {
		if dm.ScanSuccess == 1 && dm.FileCount > 0 {
			fmt.Fprintf(w, "directory_oldest_file_timestamp_seconds{base=%s,stream=%s,type=%s} %g\n",
				lv(dm.Labels.Base), lv(dm.Labels.Stream), lv(dm.Labels.Type),
				dm.OldestTimestampSec)
		}
	}

	help(w, "directory_newest_file_timestamp_seconds",
		"Modification time (Unix epoch) of the newest file. Detects halted ingest.")
	typ(w, "directory_newest_file_timestamp_seconds", "gauge")
	for _, dm := range sorted {
		if dm.ScanSuccess == 1 && dm.FileCount > 0 {
			fmt.Fprintf(w, "directory_newest_file_timestamp_seconds{base=%s,stream=%s,type=%s} %g\n",
				lv(dm.Labels.Base), lv(dm.Labels.Stream), lv(dm.Labels.Type),
				dm.NewestTimestampSec)
		}
	}

	help(w, "directory_scrape_duration_seconds",
		"Wall-clock duration of the last background scan for this directory.")
	typ(w, "directory_scrape_duration_seconds", "gauge")
	for _, dm := range sorted {
		fmt.Fprintf(w, "directory_scrape_duration_seconds{base=%s,stream=%s,type=%s} %g\n",
			lv(dm.Labels.Base), lv(dm.Labels.Stream), lv(dm.Labels.Type),
			dm.ScanDurationSec)
	}

	help(w, "directory_scrape_success",
		"Accessibility state: 1 = directory is readable, 0 = permission error or vanished.")
	typ(w, "directory_scrape_success", "gauge")
	for _, dm := range sorted {
		fmt.Fprintf(w, "directory_scrape_success{base=%s,stream=%s,type=%s} %g\n",
			lv(dm.Labels.Base), lv(dm.Labels.Stream), lv(dm.Labels.Type),
			dm.ScanSuccess)
	}

	help(w, "directory_scan_truncated",
		"1 if the last scan was cut short by MAX_FILES_PER_DIR or scan timeout; 0 otherwise.")
	typ(w, "directory_scan_truncated", "gauge")
	for _, dm := range sorted {
		truncVal := 0.0
		if dm.Truncated {
			truncVal = 1.0
		}
		fmt.Fprintf(w, "directory_scan_truncated{base=%s,stream=%s,type=%s} %g\n",
			lv(dm.Labels.Base), lv(dm.Labels.Stream), lv(dm.Labels.Type), truncVal)
	}
}

// ── helpers ───────────────────────────────────────────────────────────────────

func help(w io.Writer, name, text string) {
	fmt.Fprintf(w, "# HELP %s %s\n", name, text)
}

func typ(w io.Writer, name, metricType string) {
	fmt.Fprintf(w, "# TYPE %s %s\n", name, metricType)
}

// lv formats a string as a quoted Prometheus label value, applying the
// required escape rules from the text format spec:
//
//	\  →  \\
//	\n →  \n   (literal backslash-n in output)
//	"  →  \"
func lv(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, "\n", `\n`)
	s = strings.ReplaceAll(s, `"`, `\"`)
	return `"` + s + `"`
}
