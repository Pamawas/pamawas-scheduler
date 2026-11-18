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

	// Stalled delivery recovery (operational)
	EnableStalledDeliveryRecovery   bool
	StalledDeliveryRecoveryInterval time.Duration
}

// envBindings maps each config key to the environment variables that may set
// it, in precedence order. It is a table rather than a sequence of BindEnv
// calls so Load stays readable and the linter's cognitive-complexity budget is
// spent on parsing, not on twenty identical error checks.
var envBindings = map[string][]string{
	"database_url":                       {"DATABASE_URL", "PAMAWAS_SCHEDULER_DATABASE_URL"},
	"port":                               {"PORT", "PAMAWAS_SCHEDULER_PORT"},
	"log_level":                          {"LOG_LEVEL", "PAMAWAS_SCHEDULER_LOG_LEVEL"},
	"environment":                        {"ENVIRONMENT", "PAMAWAS_SCHEDULER_ENVIRONMENT"},
	"service_token":                      {"PAMAWAS_SERVICE_TOKEN"},
	"reporter_url":                       {"REPORTER_URL", "PAMAWAS_SCHEDULER_REPORTER_URL"},
	"daily_report_time":                  {"DAILY_REPORT_TIME", "PAMAWAS_SCHEDULER_DAILY_REPORT_TIME"},
	"high_severity_threshold":            {"HIGH_SEVERITY_THRESHOLD", "PAMAWAS_SCHEDULER_HIGH_SEVERITY_THRESHOLD"},
	"check_interval":                     {"CHECK_INTERVAL", "PAMAWAS_SCHEDULER_CHECK_INTERVAL"},
	"enable_daily_report":                {"ENABLE_DAILY_REPORT", "PAMAWAS_SCHEDULER_ENABLE_DAILY_REPORT"},
	"enable_high_severity_alert":         {"ENABLE_HIGH_SEVERITY_ALERT", "PAMAWAS_SCHEDULER_ENABLE_HIGH_SEVERITY_ALERT"},
	"mode":                               {"SCHEDULER_MODE", "PAMAWAS_SCHEDULER_MODE"},
	"default_timezone":                   {"DEFAULT_TIMEZONE", "PAMAWAS_SCHEDULER_DEFAULT_TIMEZONE"},
	"otel_exporter_otlp_endpoint":        {"OTEL_EXPORTER_OTLP_ENDPOINT", "PAMAWAS_SCHEDULER_OTEL_EXPORTER_OTLP_ENDPOINT"},
	"rate_limit_rps":                     {"RATE_LIMIT_RPS"},
	"rate_limit_burst":                   {"RATE_LIMIT_BURST"},
	"enable_stale_verification":          {"ENABLE_STALE_VERIFICATION"},
	"incident_stale_after":               {"INCIDENT_STALE_AFTER"},
	"stale_reverify_base_backoff":        {"STALE_REVERIFY_BASE_BACKOFF"},
	"stale_reverify_max_backoff":         {"STALE_REVERIFY_MAX_BACKOFF"},
	"enable_stalled_delivery_recovery":   {"ENABLE_STALLED_DELIVERY_RECOVERY"},
	"stalled_delivery_recovery_interval": {"STALLED_DELIVERY_RECOVERY_INTERVAL"},
}

// configDefaults are the values used when neither the config file nor the
// environment supplies a key.
var configDefaults = map[string]interface{}{
	"port":                               "8080",
	"log_level":                          "info",
	"environment":                        "development",
	"reporter_url":                       "http://pamawas-reporter:8080",
	"daily_report_time":                  "07:00",
	"high_severity_threshold":            "high",
	"check_interval":                     "30s",
	"enable_daily_report":                true,
	"enable_high_severity_alert":         true,
	"mode":                               "auto",
	"default_timezone":                   "Asia/Jakarta",
	"rate_limit_rps":                     10,
	"rate_limit_burst":                   20,
	"enable_stale_verification":          true,
	"incident_stale_after":               "6h",
	"stale_reverify_base_backoff":        "1h",
	"stale_reverify_max_backoff":         "24h",
	"enable_stalled_delivery_recovery":   true,
	"stalled_delivery_recovery_interval": "1h",
}

// bindEnv wires every table-driven binding into v.
func bindEnv(v *viper.Viper) {
	for key, envs := range envBindings {
		if err := v.BindEnv(append([]string{key}, envs...)...); err != nil {
			panic(fmt.Sprintf("failed to bind %s env: %v", key, err))
		}
	}
}

// setDefaults installs the documented fallback for every key.
func setDefaults(v *viper.Viper) {
	for key, value := range configDefaults {
		v.SetDefault(key, value)
	}
}

// mustDuration parses a duration-valued key, panicking with the key name on a
// bad value. Every duration is validated at startup because a typo here would
// otherwise surface as a zero interval deep inside a background loop.
func mustDuration(v *viper.Viper, key string) time.Duration {
	d, err := time.ParseDuration(v.GetString(key))
	if err != nil {
		panic(fmt.Sprintf("invalid %s: %v", key, err))
	}
	return d
}

// durationKeys are the keys parsed with mustDuration when building Config.
var durationKeys = []string{
	"check_interval",
	"incident_stale_after",
	"stale_reverify_base_backoff",
	"stale_reverify_max_backoff",
	"stalled_delivery_recovery_interval",
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
	bindEnv(v)
	setDefaults(v)

	if err := v.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			panic(fmt.Sprintf("failed to read config: %v", err))
		}
	}

	durations := make(map[string]time.Duration, len(durationKeys))
	for _, key := range durationKeys {
		durations[key] = mustDuration(v, key)
	}

	cfg := Config{
		DatabaseURL:                     v.GetString("database_url"),
		Port:                            v.GetString("port"),
		LogLevel:                        v.GetString("log_level"),
		Environment:                     v.GetString("environment"),
		ReporterURL:                     v.GetString("reporter_url"),
		ServiceToken:                    v.GetString("service_token"),
		DailyReportTime:                 v.GetString("daily_report_time"),
		HighSeverityThreshold:           v.GetString("high_severity_threshold"),
		CheckInterval:                   durations["check_interval"],
		EnableDailyReport:               v.GetBool("enable_daily_report"),
		EnableHighSeverityAlert:         v.GetBool("enable_high_severity_alert"),
		Mode:                            v.GetString("mode"),
		DefaultTimezone:                 v.GetString("default_timezone"),
		RateLimitRPS:                    v.GetFloat64("rate_limit_rps"),
		RateLimitBurst:                  v.GetInt("rate_limit_burst"),
		EnableStaleVerification:         v.GetBool("enable_stale_verification"),
		IncidentStaleAfter:              durations["incident_stale_after"],
		StaleReverifyBaseBackoff:        durations["stale_reverify_base_backoff"],
		StaleReverifyMaxBackoff:         durations["stale_reverify_max_backoff"],
		EnableStalledDeliveryRecovery:   v.GetBool("enable_stalled_delivery_recovery"),
		StalledDeliveryRecoveryInterval: durations["stalled_delivery_recovery_interval"],
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
	if c.EnableStalledDeliveryRecovery && c.StalledDeliveryRecoveryInterval <= 0 {
		return fmt.Errorf("stalled_delivery_recovery_interval must be positive when enabled")
	}
	return nil
}
