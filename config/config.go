package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	DatabaseURL           string
	Port                  string
	ReporterURL           string
	DailyReportTime       string
	HighSeverityThreshold string
	CheckInterval         time.Duration
	EnableDailyReport     bool
	EnableHighSeverityAlert bool
	Mode                  string
}

func Load() Config {
	port, _ := strconv.Atoi(getEnv("PORT", "8080"))
	intervalStr := getEnv("CHECK_INTERVAL", "30s")
	interval, _ := time.ParseDuration(intervalStr)

	cfg := Config{
		DatabaseURL:           getEnv("DATABASE_URL", ""),
		Port:                  getEnv("PORT", "8080"),
		ReporterURL:           getEnv("REPORTER_URL", "http://localhost:8081"),
		DailyReportTime:       getEnv("DAILY_REPORT_TIME", "07:00"),
		HighSeverityThreshold: getEnv("HIGH_SEVERITY_THRESHOLD", "high"),
		CheckInterval:         interval,
		EnableDailyReport:     getEnv("ENABLE_DAILY_REPORT", "true") == "true",
		EnableHighSeverityAlert: getEnv("ENABLE_HIGH_SEVERITY_ALERT", "true") == "true",
		Mode:                  getEnv("SCHEDULER_MODE", "auto"),
	}

	if cfg.DatabaseURL == "" {
		panic("DATABASE_URL environment variable not set")
	}
	return cfg
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}