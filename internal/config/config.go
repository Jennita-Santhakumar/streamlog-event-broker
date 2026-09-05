package config

import (
	"github.com/caarlos0/env/v11"
	"github.com/joho/godotenv"
)

type Config struct {
	DatabaseURL               string `env:"DATABASE_URL,required"`
	HTTPPort                  string `env:"HTTP_PORT" envDefault:"8080"`
	DataDir                   string `env:"DATA_DIR" envDefault:"/data"`
	CORSOrigins               string `env:"CORS_ORIGINS" envDefault:"*"`
	LogLevel                  string `env:"LOG_LEVEL" envDefault:"info"`
	BrokerID                  int    `env:"BROKER_ID" envDefault:"1"`
	RetentionCheckIntervalSec int    `env:"RETENTION_CHECK_INTERVAL_SECONDS" envDefault:"60"`
	SegmentMaxBytes           int64  `env:"SEGMENT_MAX_BYTES" envDefault:"16777216"`
	ShutdownGraceSec          int    `env:"SHUTDOWN_GRACE_SECONDS" envDefault:"15"`
}

func Load() (*Config, error) {
	_ = godotenv.Load()

	cfg := &Config{}
	if err := env.Parse(cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}
