package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"

	"gopkg.in/yaml.v3"
)

// Target defines one watched base path and which subdirectories within it to monitor.
// If Dirs is empty the exporter auto-discovers all subdirectories up to MaxDepth.
// If Dirs is non-empty only those explicit paths (relative to Base) are monitored.
type Target struct {
	Base     string   `yaml:"base"`
	Dirs     []string `yaml:"dirs"`
	MaxDepth int      `yaml:"max_depth"`
}

// fileConfig mirrors the YAML config file structure.
// All fields are optional; zero values mean "use default or env var".
type fileConfig struct {
	Targets           []Target `yaml:"targets"`
	ListenAddr        string   `yaml:"listen_addr"`
	ReloadSecret      string   `yaml:"reload_secret"`
	ScanInterval      string   `yaml:"scan_interval"`
	ScanWorkers       int      `yaml:"scan_workers"`
	ScanTimeout       string   `yaml:"scan_timeout"`
	DiscoveryInterval string   `yaml:"discovery_interval"`
	MaxFilesPerDir    *int     `yaml:"max_files_per_dir"`
	MaxStatFiles      *int     `yaml:"max_stat_files"`
}

// Config holds every runtime tunable for the exporter.
type Config struct {
	Targets           []Target
	ScanInterval      time.Duration
	ScanWorkers       int
	ReloadSecret      string
	DiscoveryInterval time.Duration
	ListenAddr        string
	ScanTimeout       time.Duration
	MaxFilesPerDir    int
	MaxStatFiles      int
}

// LoadConfig builds Config in three layers (each overrides the previous):
//  1. Built-in defaults
//  2. YAML config file (CONFIG_FILE env var points to the file path)
//  3. Individual env vars (SCAN_INTERVAL, RELOAD_SECRET, …)
//
// Target list comes from the config file. Falls back to BASE_PATH env var
// for backward compatibility when no config file is used.
//
// RELOAD_SECRET is auto-generated when not provided anywhere, with a logged warning.
func LoadConfig(log *slog.Logger) (*Config, error) {
	cfg := &Config{
		ScanInterval:      2 * time.Minute,
		ScanWorkers:       2,
		DiscoveryInterval: 6 * time.Hour,
		ListenAddr:        ":9200",
		ScanTimeout:       90 * time.Second,
		MaxFilesPerDir:    0,
		MaxStatFiles:      5000,
	}

	// Layer 2: YAML config file
	if configFile := os.Getenv("CONFIG_FILE"); configFile != "" {
		fc, err := loadYAMLConfig(configFile)
		if err != nil {
			return nil, fmt.Errorf("CONFIG_FILE %q: %w", configFile, err)
		}
		applyFileConfig(cfg, fc)
	}

	// Layer 3: env vars override file and defaults
	applyEnvOverrides(cfg)

	// Backward compat: if no targets loaded from file, use BASE_PATH env var
	if len(cfg.Targets) == 0 {
		basePath := os.Getenv("BASE_PATH")
		if basePath == "" {
			return nil, fmt.Errorf("no targets configured — set CONFIG_FILE pointing to a targets YAML, or set BASE_PATH")
		}
		maxDepth := 1
		if v := os.Getenv("MAX_DEPTH"); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n >= 1 {
				maxDepth = n
			}
		}
		cfg.Targets = []Target{{Base: basePath, MaxDepth: maxDepth}}
	}

	// Ensure every target has a sane MaxDepth
	for i := range cfg.Targets {
		if cfg.Targets[i].MaxDepth == 0 {
			cfg.Targets[i].MaxDepth = 1
		}
	}

	// RELOAD_SECRET: auto-generate if missing, warn loudly
	if cfg.ReloadSecret == "" {
		secret, err := generateSecret()
		if err != nil {
			return nil, fmt.Errorf("failed to generate RELOAD_SECRET: %w", err)
		}
		cfg.ReloadSecret = secret
		if log != nil {
			// Log a hint but never the secret itself — logs may be stored in
			// plain-text files readable by other processes on the host.
			log.Warn("RELOAD_SECRET not configured — generated a random one for this run",
				"hint", "set reload_secret in targets.yml or RELOAD_SECRET env var; the generated secret is NOT logged and will change on restart")
		}
	}

	return cfg, nil
}

func loadYAMLConfig(path string) (*fileConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read: %w", err)
	}
	var fc fileConfig
	if err := yaml.Unmarshal(data, &fc); err != nil {
		return nil, fmt.Errorf("parse: %w", err)
	}
	return &fc, nil
}

func applyFileConfig(cfg *Config, fc *fileConfig) {
	if fc == nil {
		return
	}
	if len(fc.Targets) > 0 {
		cfg.Targets = fc.Targets
	}
	if fc.ListenAddr != "" {
		cfg.ListenAddr = fc.ListenAddr
	}
	if fc.ReloadSecret != "" {
		cfg.ReloadSecret = fc.ReloadSecret
	}
	if fc.ScanInterval != "" {
		if d, err := time.ParseDuration(fc.ScanInterval); err == nil && d > 0 {
			cfg.ScanInterval = d
		}
	}
	if fc.ScanWorkers > 0 {
		cfg.ScanWorkers = fc.ScanWorkers
	}
	if fc.ScanTimeout != "" {
		if d, err := time.ParseDuration(fc.ScanTimeout); err == nil && d > 0 {
			cfg.ScanTimeout = d
		}
	}
	if fc.DiscoveryInterval != "" {
		if d, err := time.ParseDuration(fc.DiscoveryInterval); err == nil && d > 0 {
			cfg.DiscoveryInterval = d
		}
	}
	if fc.MaxFilesPerDir != nil {
		cfg.MaxFilesPerDir = *fc.MaxFilesPerDir
	}
	if fc.MaxStatFiles != nil {
		cfg.MaxStatFiles = *fc.MaxStatFiles
	}
}

func applyEnvOverrides(cfg *Config) {
	if v := os.Getenv("RELOAD_SECRET"); v != "" {
		cfg.ReloadSecret = v
	}
	if v := os.Getenv("LISTEN_ADDR"); v != "" {
		cfg.ListenAddr = v
	}
	if v := os.Getenv("SCAN_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			cfg.ScanInterval = d
		}
	}
	if v := os.Getenv("SCAN_WORKERS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 1 {
			cfg.ScanWorkers = n
		}
	}
	if v := os.Getenv("DISCOVERY_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			cfg.DiscoveryInterval = d
		}
	}
	if v := os.Getenv("SCAN_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			cfg.ScanTimeout = d
		}
	}
	if v := os.Getenv("MAX_FILES_PER_DIR"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			cfg.MaxFilesPerDir = n
		}
	}
	if v := os.Getenv("MAX_STAT_FILES"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			cfg.MaxStatFiles = n
		}
	}
}

func generateSecret() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
