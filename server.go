package main

import (
	"bytes"
	"crypto/subtle"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

// NewMux constructs and returns the HTTP request multiplexer with all four
// endpoints registered. Go 1.22's enhanced ServeMux pattern syntax is used
// to enforce HTTP methods at the routing layer.
func NewMux(exp *Exporter, log *slog.Logger) http.Handler {
	mux := http.NewServeMux()

	// Data plane: served entirely from the in-memory cache — zero disk I/O.
	mux.HandleFunc("GET /metrics", handleMetrics(exp, log))

	// Control plane: authenticated reload + health probes.
	mux.HandleFunc("POST /-/reload", handleReload(exp, log))
	mux.HandleFunc("GET /-/healthy", handleHealthy())
	mux.HandleFunc("GET /-/ready", handleReady(exp))

	return mux
}

// ── Handlers ──────────────────────────────────────────────────────────────────

// GET /metrics
//
// Renders the current cache snapshot in Prometheus text exposition format.
// No filesystem access is performed; the response is assembled entirely from
// in-memory data. Response time is sub-millisecond regardless of file count.
func handleMetrics(exp *Exporter, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		t0 := time.Now()
		snap := exp.Snapshot()

		// Pre-size the buffer to avoid re-allocations for large watch lists.
		// Rough estimate: ~200 bytes per directory entry × 5 metric families.
		initialCap := (len(snap.Entries)*5*200 + 2048)
		buf := bytes.NewBuffer(make([]byte, 0, initialCap))
		RenderMetrics(buf, snap)

		w.Header().Set("Content-Type", prometheusContentType)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(buf.Bytes())

		log.Debug("metrics served",
			"ready", snap.Ready,
			"entries", len(snap.Entries),
			"bytes", buf.Len(),
			"latency_us", time.Since(t0).Microseconds(),
			"remote_addr", r.RemoteAddr,
		)
	}
}

// POST /-/reload
//
// Queues an asynchronous re-discover + re-scan cycle. The endpoint requires a
// Bearer token matching RELOAD_SECRET in the Authorization header; requests
// with missing or invalid credentials are rejected with 401 before any
// filesystem access is performed (guards against self-inflicted DoS).
//
// The response is 202 Accepted — the actual reload happens asynchronously in
// RunScanLoop so the HTTP call is never blocked by disk latency.
func handleReload(exp *Exporter, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Discard body but cap it — prevents a slow-loris or giant-body DoS.
		r.Body = http.MaxBytesReader(w, r.Body, 1024)
		defer r.Body.Close()

		// ── Auth check — no I/O before this point ─────────────────────────────
		token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")

		// ✅ FIX: use constant-time comparison to prevent timing side-channel
		// attacks that could allow brute-forcing the reload secret one byte at
		// a time over a low-latency network connection.
		secretOK := token != "" &&
			subtle.ConstantTimeCompare([]byte(token), []byte(exp.cfg.ReloadSecret)) == 1

		if !secretOK {
			log.Warn("reload rejected: invalid or missing token",
				"remote_addr", r.RemoteAddr)
			http.Error(w, "Unauthorized\n", http.StatusUnauthorized)
			return
		}

		// ── Queue the reload (non-blocking) ───────────────────────────────────
		exp.TriggerReload()

		log.Info("reload queued", "remote_addr", r.RemoteAddr)
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte("Reload queued\n"))
	}
}

// GET /-/healthy
//
// Liveness probe. Returns 200 OK unconditionally as long as the process is
// running. Kubernetes uses this to decide whether to restart the container.
func handleHealthy() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK\n"))
	}
}

// GET /-/ready
//
// Readiness probe. Returns 503 Service Unavailable until the first scan cycle
// has completed (directory_cache_ready == 0). Once the cache is warm it returns
// 200 OK. Kubernetes uses this to hold traffic during the cold-start window
// so that Prometheus doesn't scrape stale zeros.
func handleReady(exp *Exporter) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !exp.IsReady() {
			http.Error(w, "Service Unavailable — cache warming up\n",
				http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("Ready\n"))
	}
}
