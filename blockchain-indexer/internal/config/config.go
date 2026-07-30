// Package config는 configs/config.yaml을 읽고 ${VAR} 형태의 플레이스홀더를 환경변수로 치환한다.
package config

import (
	"fmt"
	"os"
	"regexp"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Kafka    KafkaConfig    `yaml:"kafka"`
	Postgres PostgresConfig `yaml:"postgres"`
	Metrics  MetricsConfig  `yaml:"metrics"`
}

type KafkaConfig struct {
	Brokers []string `yaml:"brokers"`
	Topic   string   `yaml:"topic"` // "balance-deltas" (docs/02 §A, 계약 고정)
	SASL    *struct {
		Mechanism string `yaml:"mechanism"`
		Username  string `yaml:"username"`
		Password  string `yaml:"password"`
	} `yaml:"sasl,omitempty"` // 로컬은 nil(평문), 운영은 SASL/TLS (docs/06 §7)
}

type PostgresConfig struct {
	DSN string `yaml:"dsn"`
}

type MetricsConfig struct {
	ListenAddr string `yaml:"listen_addr"` // Prometheus /metrics
}

var envPlaceholder = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)

// Load는 path의 YAML을 읽어 ${VAR} 플레이스홀더를 os.Getenv로 치환한 뒤 파싱한다.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config 파일 읽기 실패: %w", err)
	}

	expanded := envPlaceholder.ReplaceAllFunc(raw, func(match []byte) []byte {
		name := envPlaceholder.FindSubmatch(match)[1]
		return []byte(os.Getenv(string(name)))
	})

	var cfg Config
	if err := yaml.Unmarshal(expanded, &cfg); err != nil {
		return nil, fmt.Errorf("config 파싱 실패: %w", err)
	}
	return &cfg, nil
}
