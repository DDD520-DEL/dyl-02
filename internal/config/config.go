package config

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

// Config holds the tunable parameters for the taskflow daemon.
type Config struct {
	ListenAddr      string        `json:"listen_addr"`
	BucketCount     int           `json:"bucket_count"`
	TickInterval    time.Duration `json:"tick_interval"`
	LeaseDuration   time.Duration `json:"lease_duration"`
	RetentionWindow time.Duration `json:"retention_window"`
	WALDir          string        `json:"wal_dir"`
	WALSegmentBytes int           `json:"wal_segment_bytes"`
	WorkerCount     int           `json:"worker_count"`
}

// Default returns the standard configuration for a standalone node.
func Default() Config {
	return Config{
		ListenAddr:      ":8080",
		BucketCount:     64,
		TickInterval:    time.Second,
		LeaseDuration:   30 * time.Second,
		RetentionWindow: 24 * time.Hour,
		WALDir:          "./data/wal",
		WALSegmentBytes: 4 << 20,
		WorkerCount:     4,
	}
}

// Load reads a JSON config file, applying defaults for zero fields.
func Load(path string) (Config, error) {
	cfg := Default()
	if path == "" {
		return cfg, nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return cfg, fmt.Errorf("read config: %w", err)
	}
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return cfg, fmt.Errorf("parse config: %w", err)
	}
	if cfg.BucketCount <= 0 {
		cfg.BucketCount = Default().BucketCount
	}
	if cfg.TickInterval <= 0 {
		cfg.TickInterval = Default().TickInterval
	}
	if cfg.LeaseDuration <= 0 {
		cfg.LeaseDuration = Default().LeaseDuration
	}
	return cfg, nil
}
