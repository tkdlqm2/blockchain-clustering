// Package config loads configs/config.yaml, substituting ${VAR} placeholders
// from the process environment (populated from .env in local dev).
package config

import (
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	App struct {
		Env      string `yaml:"env"`
		Port     string `yaml:"port"`
		LogLevel string `yaml:"log_level"`
	} `yaml:"app"`

	Database struct {
		Host     string `yaml:"host"`
		Port     string `yaml:"port"`
		Name     string `yaml:"name"`
		User     string `yaml:"user"`
		Password string `yaml:"password"`
		Schema   string `yaml:"schema"`
		SSLMode  string `yaml:"sslmode"`
	} `yaml:"database"`

	Kafka struct {
		Brokers           string `yaml:"brokers"`
		TopicBalanceDelta string `yaml:"topic_balance_delta"`
		ConsumerGroupID   string `yaml:"consumer_group_id"`
		FlushInterval     string `yaml:"flush_interval"`
	} `yaml:"kafka"`

	// LabelMaintenance drives LabelStore.Maintain (docs/03 §8, FR-20/21) —
	// externalized here rather than hardcoded (docs/03 §10) since there's
	// no chain_heuristic-style per-chain registry entry for label policy.
	LabelMaintenance struct {
		Interval    string  `yaml:"interval"`
		StaleTTL    string  `yaml:"stale_ttl"`
		DecayFactor float64 `yaml:"decay_factor"`
	} `yaml:"label_maintenance"`
}

var envPlaceholder = regexp.MustCompile(`\$\{([A-Z0-9_]+)\}`)

// Load reads path, substitutes ${VAR} with values from the environment, and
// parses the result as YAML.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}

	substituted := envPlaceholder.ReplaceAllStringFunc(string(raw), func(m string) string {
		name := envPlaceholder.FindStringSubmatch(m)[1]
		return os.Getenv(name)
	})

	var cfg Config
	if err := yaml.Unmarshal([]byte(substituted), &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	return &cfg, nil
}

// DSN returns a PostgreSQL connection string for the app user.
func (c *Config) DSN() string {
	return fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s?sslmode=%s&search_path=%s",
		c.Database.User, c.Database.Password, c.Database.Host, c.Database.Port,
		c.Database.Name, c.Database.SSLMode, c.Database.Schema,
	)
}

// LabelMaintenanceInterval is how often LabelStore.Maintain runs.
func (c *Config) LabelMaintenanceInterval() (time.Duration, error) {
	return time.ParseDuration(c.LabelMaintenance.Interval)
}

// LabelMaintenanceStaleTTL is how long a label can go unverified before
// LabelStore.Maintain decays it (FR-20).
func (c *Config) LabelMaintenanceStaleTTL() (time.Duration, error) {
	return time.ParseDuration(c.LabelMaintenance.StaleTTL)
}

// KafkaBrokerList splits the comma-separated KAFKA_BROKERS value into the
// []string kafka-go expects.
func (c *Config) KafkaBrokerList() []string {
	return strings.Split(c.Kafka.Brokers, ",")
}

// ConsumerFlushInterval is consumer.Consumer's periodic safety-net flush —
// how long a batch may sit unprocessed before being flushed even without a
// new block arriving to trigger it.
func (c *Config) ConsumerFlushInterval() (time.Duration, error) {
	return time.ParseDuration(c.Kafka.FlushInterval)
}
