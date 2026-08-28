package config

import (
	"fmt"
	"time"

	"github.com/spf13/viper"
)

type Config struct {
	DatabaseURL             string
	Port                    string
	LogLevel                string
	Environment             string
	ReporterURL             string
	DailyReportTime         string
	HighSeverityThreshold   string
	CheckInterval           time.Duration
	EnableDailyReport       bool
	EnableHighSeverityAlert bool
	Mode                    string
	DefaultTimezone         string
}

func Load() Config {
	v := viper.New()
	v.SetConfigName("config")
	v.SetConfigType("yaml")
	v.AddConfigPath(".")
	v.AddConfigPath("./config")
	v.AddConfigPath("/etc/pamawas/")
	v.SetEnvPrefix("PAMAWAS_SCHEDULER")
	v.AutomaticEnv()

	// Defaults
	v.SetDefault("port", "8080")
	v.SetDefault("log_level", "info")
	v.SetDefault("environment", "development")
	v.SetDefault("reporter_url", "http://pamawas-reporter:8080")
	v.SetDefault("daily_report_time", "07:00")
	v.SetDefault("high_severity_threshold", "high")
	v.SetDefault("check_interval", "30s")
	v.SetDefault("enable_daily_report", true)
	v.SetDefault("enable_high_severity_alert", true)
	v.SetDefault("mode", "auto")
	v.SetDefault("default_timezone", "Asia/Jakarta")

	if err := v.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			panic(fmt.Sprintf("failed to read config: %v", err))
		}
	}

	checkInterval, err := time.ParseDuration(v.GetString("check_interval"))
	if err != nil {
		panic(fmt.Sprintf("invalid check_interval: %v", err))
	}

	cfg := Config{
		DatabaseURL:             v.GetString("database_url"),
		Port:                    v.GetString("port"),
		LogLevel:                v.GetString("log_level"),
		Environment:             v.GetString("environment"),
		ReporterURL:             v.GetString("reporter_url"),
		DailyReportTime:         v.GetString("daily_report_time"),
		HighSeverityThreshold:   v.GetString("high_severity_threshold"),
		CheckInterval:           checkInterval,
		EnableDailyReport:       v.GetBool("enable_daily_report"),
		EnableHighSeverityAlert: v.GetBool("enable_high_severity_alert"),
		Mode:                    v.GetString("mode"),
		DefaultTimezone:         v.GetString("default_timezone"),
	}

	if cfg.DatabaseURL == "" {
		panic("DATABASE_URL not set (config file or PAMAWAS_SCHEDULER_DATABASE_URL env var)")
	}
	return cfg
}

func (c Config) Validate() error {
	if c.DatabaseURL == "" {
		return fmt.Errorf("database_url is required")
	}
	if c.Port == "" {
		return fmt.Errorf("port is required")
	}
	if c.CheckInterval <= 0 {
		return fmt.Errorf("check_interval must be positive")
	}
	return nil
}
