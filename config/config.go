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
	// Don't use AutomaticEnv() - Kubernetes injects PAMAWAS_SCHEDULER_PORT=tcp://IP:PORT
	// which would override our config. Explicitly bind only what we need.
	if err := v.BindEnv("database_url", "DATABASE_URL"); err != nil {
		panic(fmt.Sprintf("failed to bind DATABASE_URL env: %v", err))
	}
	if err := v.BindEnv("port", "PORT"); err != nil {
		panic(fmt.Sprintf("failed to bind PORT env: %v", err))
	}
	if err := v.BindEnv("log_level", "LOG_LEVEL"); err != nil {
		panic(fmt.Sprintf("failed to bind LOG_LEVEL env: %v", err))
	}
	if err := v.BindEnv("environment", "ENVIRONMENT"); err != nil {
		panic(fmt.Sprintf("failed to bind ENVIRONMENT env: %v", err))
	}
	if err := v.BindEnv("reporter_url", "REPORTER_URL"); err != nil {
		panic(fmt.Sprintf("failed to bind REPORTER_URL env: %v", err))
	}
	if err := v.BindEnv("daily_report_time", "DAILY_REPORT_TIME"); err != nil {
		panic(fmt.Sprintf("failed to bind DAILY_REPORT_TIME env: %v", err))
	}
	if err := v.BindEnv("high_severity_threshold", "HIGH_SEVERITY_THRESHOLD"); err != nil {
		panic(fmt.Sprintf("failed to bind HIGH_SEVERITY_THRESHOLD env: %v", err))
	}
	if err := v.BindEnv("check_interval", "CHECK_INTERVAL"); err != nil {
		panic(fmt.Sprintf("failed to bind CHECK_INTERVAL env: %v", err))
	}
	if err := v.BindEnv("enable_daily_report", "ENABLE_DAILY_REPORT"); err != nil {
		panic(fmt.Sprintf("failed to bind ENABLE_DAILY_REPORT env: %v", err))
	}
	if err := v.BindEnv("enable_high_severity_alert", "ENABLE_HIGH_SEVERITY_ALERT"); err != nil {
		panic(fmt.Sprintf("failed to bind ENABLE_HIGH_SEVERITY_ALERT env: %v", err))
	}
	if err := v.BindEnv("mode", "SCHEDULER_MODE"); err != nil {
		panic(fmt.Sprintf("failed to bind SCHEDULER_MODE env: %v", err))
	}
	if err := v.BindEnv("default_timezone", "DEFAULT_TIMEZONE"); err != nil {
		panic(fmt.Sprintf("failed to bind DEFAULT_TIMEZONE env: %v", err))
	}
	if err := v.BindEnv("otel_exporter_otlp_endpoint", "OTEL_EXPORTER_OTLP_ENDPOINT"); err != nil {
		panic(fmt.Sprintf("failed to bind OTEL_EXPORTER_OTLP_ENDPOINT env: %v", err))
	}

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
		panic("DATABASE_URL not set (config file or PAMAWAS_SCHEDULER_DATABASE_URL / DATABASE_URL env var)")
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
