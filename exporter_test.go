package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// ── config ────────────────────────────────────────────────────────────────────

func TestLoadConfig_RequiredMissing(t *testing.T) {
	t.Setenv("BASE_PATH", "")
	t.Setenv("RELOAD_SECRET", "")
	if _, err := LoadConfig(); err == nil {
		t.Fatal("expected error for missing BASE_PATH")
	}
}

func TestLoadConfig_Defaults(t *testing.T) {
	t.Setenv("BASE_PATH", "/tmp")
	t.Setenv("RELOAD_SECRET", "secret")
	cfg, err := LoadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MaxDepth != 3 {
		t.Errorf("MaxDepth: got %d want 3", cfg.MaxDepth)
	}
	if cfg.ScanWorkers != 2 {
		t.Errorf("ScanWorkers: got %d want 2", cfg.ScanWorkers)
	}
}

func TestLoadConfig_InvalidMaxDepth(t *testing.T) {
	t.Setenv("BASE_PATH", "/tmp")
	t.Setenv("RELOAD_SECRET", "secret")
	t.Setenv("MAX_DEPTH", "0")
	if _, err := LoadConfig(); err == nil {
		t.Fatal("expected error for MAX_DEPTH=0")
	}
}

// ── scanner ───────────────────────────────────────────────────────────────────

func TestScanDir_Empty(t *testing.T) {
	dir := t.TempDir()
	r := scanDir(context.Background(), dir, 0, 0)
	if r.ScanSuccess != 1 {
		t.Errorf("empty dir: ScanSuccess=%g want 1", r.ScanSuccess)
	}
	if r.FileCount != 0 {
		t.Errorf("empty dir: FileCount=%d want 0", r.FileCount)
	}
	if r.OldestTimestamp != 0 || r.NewestTimestamp != 0 {
		t.Error("empty dir: timestamps should be zero")
	}
}

func TestScanDir_WithFiles(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"a.txt", "b.txt", "c.txt"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("x"), 0644); err != nil {
			t.Fatal(err)
		}
	}
	r := scanDir(context.Background(), dir, 0, 0)
	if r.ScanSuccess != 1 {
		t.Errorf("ScanSuccess=%g want 1", r.ScanSuccess)
	}
	if r.FileCount != 3 {
		t.Errorf("FileCount=%d want 3", r.FileCount)
	}
	if r.OldestTimestamp == 0 || r.NewestTimestamp == 0 {
		t.Error("timestamps should be non-zero when files exist")
	}
}

func TestScanDir_SubdirNotCounted(t *testing.T) {
	dir := t.TempDir()
	_ = os.Mkdir(filepath.Join(dir, "subdir"), 0755)
	os.WriteFile(filepath.Join(dir, "file.txt"), []byte("x"), 0644)
	r := scanDir(context.Background(), dir, 0, 0)
	if r.FileCount != 1 {
		t.Errorf("FileCount=%d want 1 (subdirs must not be counted)", r.FileCount)
	}
}

func TestScanDir_Nonexistent(t *testing.T) {
	r := scanDir(context.Background(), "/nonexistent/path/that/cannot/exist", 0, 0)
	if r.ScanSuccess != 0 {
		t.Errorf("ScanSuccess=%g want 0 for missing dir", r.ScanSuccess)
	}
	if r.Err == nil {
		t.Error("Err should be non-nil for missing dir")
	}
}

// ── watchlist / discovery ─────────────────────────────────────────────────────

func TestDiscoverDirectories_Depth1(t *testing.T) {
	base := t.TempDir()
	os.MkdirAll(filepath.Join(base, "orders"), 0755)
	os.MkdirAll(filepath.Join(base, "payments"), 0755)

	entries, err := discoverDirectories(base, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Errorf("got %d entries want 2", len(entries))
	}
	for _, e := range entries {
		if e.Labels.Type != "" {
			t.Errorf("depth-1 entry should have empty Type, got %q", e.Labels.Type)
		}
	}
}

func TestDiscoverDirectories_Depth2Labels(t *testing.T) {
	base := t.TempDir()
	os.MkdirAll(filepath.Join(base, "orders", "buffer"), 0755)

	entries, err := discoverDirectories(base, 2)
	if err != nil {
		t.Fatal(err)
	}
	// Expects two entries: "orders" (depth 1) and "orders/buffer" (depth 2).
	if len(entries) != 2 {
		t.Fatalf("got %d entries want 2", len(entries))
	}
	// Find the depth-2 entry.
	var leaf *WatchEntry
	for i := range entries {
		if entries[i].Labels.Type != "" {
			leaf = &entries[i]
		}
	}
	if leaf == nil {
		t.Fatal("no depth-2 entry found")
	}
	if leaf.Labels.Stream != "orders" {
		t.Errorf("Stream=%q want orders", leaf.Labels.Stream)
	}
	if leaf.Labels.Type != "buffer" {
		t.Errorf("Type=%q want buffer", leaf.Labels.Type)
	}
}

// ── metrics rendering ─────────────────────────────────────────────────────────

func TestRenderMetrics_NotReady(t *testing.T) {
	var buf bytes.Buffer
	RenderMetrics(&buf, CacheSnapshot{Ready: false})
	out := buf.String()
	if !strings.Contains(out, "directory_cache_ready 0") {
		t.Error("expected directory_cache_ready 0 when not ready")
	}
	// No other metric families should appear during cold start.
	if strings.Contains(out, "directory_file_count") {
		t.Error("unexpected directory_file_count in cold-start output")
	}
}

func TestRenderMetrics_Ready(t *testing.T) {
	snap := CacheSnapshot{
		Ready:        true,
		LastScanTime: time.Now(),
		WatchedTotal: 1,
		Entries: []DirMetrics{
			{
				FileCount:          42,
				OldestTimestampSec: 1_700_000_000,
				NewestTimestampSec: 1_700_001_000,
				ScanDurationSec:    0.005,
				ScanSuccess:        1,
				Labels:             DirLabels{Base: "/streams", Stream: "orders", Type: "buffer"},
			},
		},
	}
	var buf bytes.Buffer
	RenderMetrics(&buf, snap)
	out := buf.String()

	checks := []string{
		"directory_cache_ready 1",
		"directory_watched_total 1",
		`directory_file_count{base="/streams",stream="orders",type="buffer"} 42`,
		`directory_scrape_success{base="/streams",stream="orders",type="buffer"} 1`,
	}
	for _, want := range checks {
		if !strings.Contains(out, want) {
			t.Errorf("missing expected line:\n  want: %s\n  in:\n%s", want, out)
		}
	}
}

func TestRenderMetrics_EmptyDirOmitsTimestamps(t *testing.T) {
	snap := CacheSnapshot{
		Ready:        true,
		LastScanTime: time.Now(),
		Entries: []DirMetrics{
			{FileCount: 0, ScanSuccess: 1, Labels: DirLabels{Stream: "orders"}},
		},
	}
	var buf bytes.Buffer
	RenderMetrics(&buf, snap)
	out := buf.String()
	if strings.Contains(out, "directory_oldest_file_timestamp_seconds{") {
		t.Error("oldest timestamp should be omitted for empty directory")
	}
	if strings.Contains(out, "directory_newest_file_timestamp_seconds{") {
		t.Error("newest timestamp should be omitted for empty directory")
	}
}

func TestRenderMetrics_SortedOutput(t *testing.T) {
	// Entries added in reverse alphabetical order — output must be sorted.
	snap := CacheSnapshot{
		Ready:        true,
		LastScanTime: time.Now(),
		Entries: []DirMetrics{
			{ScanSuccess: 1, Labels: DirLabels{Stream: "z-stream"}},
			{ScanSuccess: 1, Labels: DirLabels{Stream: "a-stream"}},
		},
	}
	var buf bytes.Buffer
	RenderMetrics(&buf, snap)
	out := buf.String()
	idxA := strings.Index(out, "a-stream")
	idxZ := strings.Index(out, "z-stream")
	if idxA > idxZ {
		t.Error("metric output is not sorted: a-stream should appear before z-stream")
	}
}

// ── label value escaping ──────────────────────────────────────────────────────

func TestLv_Escaping(t *testing.T) {
	cases := []struct{ in, want string }{
		{`plain`, `"plain"`},
		{`with "quotes"`, `"with \"quotes\""`},
		{`with\backslash`, `"with\\backslash"`},
		{"with\nnewline", `"with\nnewline"`},
	}
	for _, tc := range cases {
		if got := lv(tc.in); got != tc.want {
			t.Errorf("lv(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// ── log output ────────────────────────────────────────────────────────────────

// logLines parses a multi-line JSON log buffer into a slice of maps so tests
// can assert on specific fields without fragile string matching.
func logLines(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var lines []map[string]any
	for _, raw := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if raw == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(raw), &m); err != nil {
			t.Fatalf("log line is not valid JSON: %s\nerror: %v", raw, err)
		}
		lines = append(lines, m)
	}
	return lines
}

// hasLog returns true if any log line has the given key=value pair.
func hasLog(lines []map[string]any, key, value string) bool {
	for _, l := range lines {
		if v, ok := l[key]; ok && fmt.Sprint(v) == value {
			return true
		}
	}
	return false
}

func TestScanAll_LogsScanComplete(t *testing.T) {
	base := t.TempDir()
	os.MkdirAll(filepath.Join(base, "orders"), 0755)
	os.WriteFile(filepath.Join(base, "orders", "a.txt"), []byte("x"), 0644)

	var buf bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	cfg := &Config{
		BasePath:       base,
		MaxDepth:       1,
		ScanWorkers:    1,
		ScanTimeout:    10 * time.Second,
		MaxFilesPerDir: 0,
	}
	exp := NewExporter(cfg, log)
	if err := exp.Discover(); err != nil {
		t.Fatal(err)
	}
	exp.ScanAll()

	lines := logLines(t, &buf)
	if !hasLog(lines, "msg", "scan cycle complete") {
		t.Errorf("expected 'scan cycle complete' log line; got:\n%s", buf.String())
	}
}

func TestScanAll_LogsTruncation(t *testing.T) {
	base := t.TempDir()
	dir := filepath.Join(base, "big")
	os.MkdirAll(dir, 0755)
	for i := range 5 {
		os.WriteFile(filepath.Join(dir, fmt.Sprintf("%d.txt", i)), []byte("x"), 0644)
	}

	var buf bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	cfg := &Config{
		BasePath:       base,
		MaxDepth:       1,
		ScanWorkers:    1,
		ScanTimeout:    10 * time.Second,
		MaxFilesPerDir: 2, // cap at 2 out of 5 — must trigger truncation warning
	}
	exp := NewExporter(cfg, log)
	if err := exp.Discover(); err != nil {
		t.Fatal(err)
	}
	exp.ScanAll()

	lines := logLines(t, &buf)
	if !hasLog(lines, "msg", "scan truncated") {
		t.Errorf("expected 'scan truncated' warning; got:\n%s", buf.String())
	}
}

func TestScanAll_MetricsTruncatedAfterCap(t *testing.T) {
	base := t.TempDir()
	dir := filepath.Join(base, "stream")
	os.MkdirAll(dir, 0755)
	for i := range 10 {
		os.WriteFile(filepath.Join(dir, fmt.Sprintf("%d.txt", i)), []byte("x"), 0644)
	}

	var buf bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&buf, nil))

	cfg := &Config{
		BasePath:       base,
		MaxDepth:       1,
		ScanWorkers:    1,
		ScanTimeout:    10 * time.Second,
		MaxFilesPerDir: 3,
	}
	exp := NewExporter(cfg, log)
	if err := exp.Discover(); err != nil {
		t.Fatal(err)
	}
	exp.ScanAll()

	snap := exp.Snapshot()
	if len(snap.Entries) == 0 {
		t.Fatal("no entries in snapshot")
	}
	entry := snap.Entries[0]
	if !entry.Truncated {
		t.Error("expected Truncated=true when MaxFilesPerDir cap is hit")
	}
	if entry.FileCount != 3 {
		t.Errorf("FileCount=%d want 3 (only capped entries counted)", entry.FileCount)
	}

	var metricsBuf bytes.Buffer
	RenderMetrics(&metricsBuf, snap)
	if !strings.Contains(metricsBuf.String(), "directory_scan_truncated") {
		t.Error("expected directory_scan_truncated in /metrics output")
	}
}

func TestScanAll_MaxStatFilesCountsAllButStatsLimited(t *testing.T) {
	base := t.TempDir()
	dir := filepath.Join(base, "stream")
	os.MkdirAll(dir, 0755)
	for i := range 10 {
		os.WriteFile(filepath.Join(dir, fmt.Sprintf("%d.txt", i)), []byte("x"), 0644)
	}

	var buf bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&buf, nil))

	cfg := &Config{
		BasePath:     base,
		MaxDepth:     1,
		ScanWorkers:  1,
		ScanTimeout:  10 * time.Second,
		MaxStatFiles: 3, // stat only 3 of the 10 files
	}
	exp := NewExporter(cfg, log)
	if err := exp.Discover(); err != nil {
		t.Fatal(err)
	}
	exp.ScanAll()

	snap := exp.Snapshot()
	if len(snap.Entries) == 0 {
		t.Fatal("no entries in snapshot")
	}
	entry := snap.Entries[0]
	// All 10 files must be counted even though only 3 were stat-ed.
	if entry.FileCount != 10 {
		t.Errorf("FileCount=%d want 10 — all files counted regardless of stat cap", entry.FileCount)
	}
	// Truncated should be true because the stat cap was hit.
	if !entry.Truncated {
		t.Error("expected Truncated=true when MaxStatFiles cap is hit")
	}
	// Timestamps must still be present (from the 3 stat-ed files).
	if entry.OldestTimestampSec == 0 || entry.NewestTimestampSec == 0 {
		t.Error("expected non-zero timestamps from stat-ed sample")
	}
}
