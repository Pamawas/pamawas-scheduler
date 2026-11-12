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
	// Notification channel fields for real-time high-severity alerts.
	DiscordWebhookURL  string
	TelegramBotToken   string
	TelegramChatID     string
	TelegramAPIBaseURL string
	EmailSMTPHost      string
	EmailSMTPPort      int
	EmailUsername      string
	EmailPassword      string
	EmailFrom          string
	EmailTo            string
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
	if err := v.BindEnv("discord_webhook_url", "DISCORD_WEBHOOK_URL", "PAMAWAS_SCHEDULER_DISCORD_WEBHOOK_URL"); err != nil {
		panic(fmt.Sprintf("failed to bind DISCORD_WEBHOOK_URL env: %v", err))
	}
	if err := v.BindEnv("telegram_bot_token", "TELEGRAM_BOT_TOKEN", "PAMAWAS_SCHEDULER_TELEGRAM_BOT_TOKEN"); err != nil {
		panic(fmt.Sprintf("failed to bind TELEGRAM_BOT_TOKEN env: %v", err))
	}
	if err := v.BindEnv("telegram_chat_id", "TELEGRAM_CHAT_ID", "PAMAWAS_SCHEDULER_TELEGRAM_CHAT_ID"); err != nil {
		panic(fmt.Sprintf("failed to bind TELEGRAM_CHAT_ID env: %v", err))
	}
	if err := v.BindEnv("telegram_api_base_url", "TELEGRAM_API_BASE_URL", "PAMAWAS_SCHEDULER_TELEGRAM_API_BASE_URL"); err != nil {
		panic(fmt.Sprintf("failed to bind TELEGRAM_API_BASE_URL env: %v", err))
	}
	if err := v.BindEnv("email_smtp_host", "EMAIL_SMTP_HOST", "PAMAWAS_SCHEDULER_EMAIL_SMTP_HOST"); err != nil {
		panic(fmt.Sprintf("failed to bind EMAIL_SMTP_HOST env: %v", err))
	}
	if err := v.BindEnv("email_smtp_port", "EMAIL_SMTP_PORT", "PAMAWAS_SCHEDULER_EMAIL_SMTP_PORT"); err != nil {
		panic(fmt.Sprintf("failed to bind EMAIL_SMTP_PORT env: %v", err))
	}
	if err := v.BindEnv("email_username", "EMAIL_USERNAME", "PAMAWAS_SCHEDULER_EMAIL_USERNAME"); err != nil {
		panic(fmt.Sprintf("failed to bind EMAIL_USERNAME env: %v", err))
	}
	if err := v.BindEnv("email_password", "EMAIL_PASSWORD", "PAMAWAS_SCHEDULER_EMAIL_PASSWORD"); err != nil {
		panic(fmt.Sprintf("failed to bind EMAIL_PASSWORD env: %v", err))
	}
	if err := v.BindEnv("email_from", "EMAIL_FROM", "PAMAWAS_SCHEDULER_EMAIL_FROM"); err != nil {
		panic(fmt.Sprintf("failed to bind EMAIL_FROM env: %v", err))
	}
	if err := v.BindEnv("email_to", "EMAIL_TO", "PAMAWAS_SCHEDULER_EMAIL_TO"); err != nil {
		panic(fmt.Sprintf("failed to bind EMAIL_TO env: %v", err))
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
