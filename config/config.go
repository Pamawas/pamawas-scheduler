package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/viper"
)

type Config struct {
	DatabaseURL string
	Port        string
	LogLevel    string
	Environment string
	ReporterURL string
	// ServiceToken authenticates outbound calls to the reporter, which rejects
	// unauthenticated callers on every /api/v1 route.
	ServiceToken            string
	DailyReportTime         string
	HighSeverityThreshold   string
	CheckInterval           time.Duration
	EnableDailyReport       bool
	EnableHighSeverityAlert bool
	Mode                    string
	DefaultTimezone         string

	// Inbound HTTP rate limiting (design review P5). Applies per route;
	// /healthz, /ready and /metrics are exempt.
	RateLimitRPS   float64
	RateLimitBurst int

	// Stale-incident verification (see stale-incident-verification.md)
	EnableStaleVerification  bool
	IncidentStaleAfter       time.Duration
	StaleReverifyBaseBackoff time.Duration
	StaleReverifyMaxBackoff  time.Duration
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
	if err := v.BindEnv("database_url", "DATABASE_URL", "PAMAWAS_SCHEDULER_DATABASE_URL"); err != nil {
		panic(fmt.Sprintf("failed to bind DATABASE_URL env: %v", err))
	}
	if err := v.BindEnv("port", "PORT", "PAMAWAS_SCHEDULER_PORT"); err != nil {
		panic(fmt.Sprintf("failed to bind PORT env: %v", err))
	}
	if err := v.BindEnv("log_level", "LOG_LEVEL", "PAMAWAS_SCHEDULER_LOG_LEVEL"); err != nil {
		panic(fmt.Sprintf("failed to bind LOG_LEVEL env: %v", err))
	}
	if err := v.BindEnv("environment", "ENVIRONMENT", "PAMAWAS_SCHEDULER_ENVIRONMENT"); err != nil {
		panic(fmt.Sprintf("failed to bind ENVIRONMENT env: %v", err))
	}
	if err := v.BindEnv("service_token", "PAMAWAS_SERVICE_TOKEN"); err != nil {
		panic(fmt.Sprintf("failed to bind PAMAWAS_SERVICE_TOKEN env: %v", err))
	}
	if err := v.BindEnv("reporter_url", "REPORTER_URL", "PAMAWAS_SCHEDULER_REPORTER_URL"); err != nil {
		panic(fmt.Sprintf("failed to bind REPORTER_URL env: %v", err))
	}
	if err := v.BindEnv("daily_report_time", "DAILY_REPORT_TIME", "PAMAWAS_SCHEDULER_DAILY_REPORT_TIME"); err != nil {
		panic(fmt.Sprintf("failed to bind DAILY_REPORT_TIME env: %v", err))
	}
	if err := v.BindEnv("high_severity_threshold", "HIGH_SEVERITY_THRESHOLD", "PAMAWAS_SCHEDULER_HIGH_SEVERITY_THRESHOLD"); err != nil {
		panic(fmt.Sprintf("failed to bind HIGH_SEVERITY_THRESHOLD env: %v", err))
	}
	if err := v.BindEnv("check_interval", "CHECK_INTERVAL", "PAMAWAS_SCHEDULER_CHECK_INTERVAL"); err != nil {
		panic(fmt.Sprintf("failed to bind CHECK_INTERVAL env: %v", err))
	}
	if err := v.BindEnv("enable_daily_report", "ENABLE_DAILY_REPORT", "PAMAWAS_SCHEDULER_ENABLE_DAILY_REPORT"); err != nil {
		panic(fmt.Sprintf("failed to bind ENABLE_DAILY_REPORT env: %v", err))
	}
	if err := v.BindEnv("enable_high_severity_alert", "ENABLE_HIGH_SEVERITY_ALERT", "PAMAWAS_SCHEDULER_ENABLE_HIGH_SEVERITY_ALERT"); err != nil {
		panic(fmt.Sprintf("failed to bind ENABLE_HIGH_SEVERITY_ALERT env: %v", err))
	}
	if err := v.BindEnv("mode", "SCHEDULER_MODE", "PAMAWAS_SCHEDULER_MODE"); err != nil {
		panic(fmt.Sprintf("failed to bind SCHEDULER_MODE env: %v", err))
	}
	if err := v.BindEnv("default_timezone", "DEFAULT_TIMEZONE", "PAMAWAS_SCHEDULER_DEFAULT_TIMEZONE"); err != nil {
		panic(fmt.Sprintf("failed to bind DEFAULT_TIMEZONE env: %v", err))
	}
	if err := v.BindEnv("otel_exporter_otlp_endpoint", "OTEL_EXPORTER_OTLP_ENDPOINT", "PAMAWAS_SCHEDULER_OTEL_EXPORTER_OTLP_ENDPOINT"); err != nil {
		panic(fmt.Sprintf("failed to bind OTEL_EXPORTER_OTLP_ENDPOINT env: %v", err))
	}
	if err := v.BindEnv("rate_limit_rps", "RATE_LIMIT_RPS"); err != nil {
		panic(fmt.Sprintf("failed to bind RATE_LIMIT_RPS env: %v", err))
	}
	if err := v.BindEnv("rate_limit_burst", "RATE_LIMIT_BURST"); err != nil {
		panic(fmt.Sprintf("failed to bind RATE_LIMIT_BURST env: %v", err))
	}
	if err := v.BindEnv("enable_stale_verification", "ENABLE_STALE_VERIFICATION"); err != nil {
		panic(fmt.Sprintf("failed to bind ENABLE_STALE_VERIFICATION env: %v", err))
	}
	if err := v.BindEnv("incident_stale_after", "INCIDENT_STALE_AFTER"); err != nil {
		panic(fmt.Sprintf("failed to bind INCIDENT_STALE_AFTER env: %v", err))
	}
	if err := v.BindEnv("stale_reverify_base_backoff", "STALE_REVERIFY_BASE_BACKOFF"); err != nil {
		panic(fmt.Sprintf("failed to bind STALE_REVERIFY_BASE_BACKOFF env: %v", err))
	}
	if err := v.BindEnv("stale_reverify_max_backoff", "STALE_REVERIFY_MAX_BACKOFF"); err != nil {
		panic(fmt.Sprintf("failed to bind STALE_REVERIFY_MAX_BACKOFF env: %v", err))
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
	v.SetDefault("rate_limit_rps", 10)
	v.SetDefault("rate_limit_burst", 20)
	v.SetDefault("enable_stale_verification", true)
	v.SetDefault("incident_stale_after", "6h")
	v.SetDefault("stale_reverify_base_backoff", "1h")
	v.SetDefault("stale_reverify_max_backoff", "24h")

	if err := v.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			panic(fmt.Sprintf("failed to read config: %v", err))
		}
	}

	checkInterval, err := time.ParseDuration(v.GetString("check_interval"))
	if err != nil {
		panic(fmt.Sprintf("invalid check_interval: %v", err))
	}
	incidentStaleAfter, err := time.ParseDuration(v.GetString("incident_stale_after"))
	if err != nil {
		panic(fmt.Sprintf("invalid incident_stale_after: %v", err))
	}
	staleReverifyBaseBackoff, err := time.ParseDuration(v.GetString("stale_reverify_base_backoff"))
	if err != nil {
		panic(fmt.Sprintf("invalid stale_reverify_base_backoff: %v", err))
	}
	staleReverifyMaxBackoff, err := time.ParseDuration(v.GetString("stale_reverify_max_backoff"))
	if err != nil {
		panic(fmt.Sprintf("invalid stale_reverify_max_backoff: %v", err))
	}

	cfg := Config{
		DatabaseURL:              v.GetString("database_url"),
		Port:                     v.GetString("port"),
		LogLevel:                 v.GetString("log_level"),
		Environment:              v.GetString("environment"),
		ReporterURL:              v.GetString("reporter_url"),
		ServiceToken:             v.GetString("service_token"),
		DailyReportTime:          v.GetString("daily_report_time"),
		HighSeverityThreshold:    v.GetString("high_severity_threshold"),
		CheckInterval:            checkInterval,
		EnableDailyReport:        v.GetBool("enable_daily_report"),
		EnableHighSeverityAlert:  v.GetBool("enable_high_severity_alert"),
		Mode:                     v.GetString("mode"),
		DefaultTimezone:          v.GetString("default_timezone"),
		RateLimitRPS:             v.GetFloat64("rate_limit_rps"),
		RateLimitBurst:           v.GetInt("rate_limit_burst"),
		EnableStaleVerification:  v.GetBool("enable_stale_verification"),
		IncidentStaleAfter:       incidentStaleAfter,
		StaleReverifyBaseBackoff: staleReverifyBaseBackoff,
		StaleReverifyMaxBackoff:  staleReverifyMaxBackoff,
	}

	if err := cfg.Validate(); err != nil {
		panic(fmt.Sprintf("invalid configuration: %v", err))
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
	if c.ServiceToken == "" {
		return fmt.Errorf("service_token is required (set PAMAWAS_SERVICE_TOKEN): the reporter rejects unauthenticated callers")
	}
	if c.ReporterURL == "" {
		return fmt.Errorf("reporter_url is required")
	}
	if _, err := time.Parse("15:04", c.DailyReportTime); err != nil {
		return fmt.Errorf("daily_report_time must be HH:MM, got %q", c.DailyReportTime)
	}
	switch strings.ToLower(c.HighSeverityThreshold) {
	// "unknown" is the absence of a gate: it ranks below every real severity,
	// so every event passes. Use it when all alerts must flow through.
	case "unknown", "debug", "info", "warning", "high", "critical":
	default:
		return fmt.Errorf("high_severity_threshold must be one of unknown|debug|info|warning|high|critical, got %q", c.HighSeverityThreshold)
	}
	if c.DefaultTimezone == "" {
		return fmt.Errorf("default_timezone is required")
	}
	if _, err := time.LoadLocation(c.DefaultTimezone); err != nil {
		return fmt.Errorf("default_timezone %q is not a valid IANA zone: %w", c.DefaultTimezone, err)
	}
	if c.RateLimitRPS <= 0 {
		return fmt.Errorf("rate_limit_rps must be positive, got %v", c.RateLimitRPS)
	}
	if c.RateLimitBurst < 1 {
		return fmt.Errorf("rate_limit_burst must be >= 1, got %d", c.RateLimitBurst)
	}
	return nil
}
