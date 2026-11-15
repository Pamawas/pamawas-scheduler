package config

import (
	"os"
	"strings"
	"testing"
	"time"
)

func inTempDir(t *testing.T) {
	t.Helper()
	old, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(t.TempDir()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.Chdir(old); err != nil {
			t.Logf("failed to chdir back: %v", err)
		}
	})
}

func TestLoadDefaultsAndEnvironmentOverrides(t *testing.T) {
	inTempDir(t)
	t.Setenv("PAMAWAS_SCHEDULER_DATABASE_URL", "postgres://example/test")
	t.Setenv("PAMAWAS_SERVICE_TOKEN", "test-service-token")
	t.Setenv("PAMAWAS_SCHEDULER_PORT", "9090")
	t.Setenv("PAMAWAS_SCHEDULER_CHECK_INTERVAL", "2m")
	t.Setenv("PAMAWAS_SCHEDULER_ENABLE_DAILY_REPORT", "false")
	os.Unsetenv("PORT") // avoid Kubernetes PORT env leaking into viper
	cfg := Load()
	if cfg.DatabaseURL != "postgres://example/test" || cfg.Port != "9090" || cfg.CheckInterval != 2*time.Minute || cfg.EnableDailyReport {
		t.Fatalf("unexpected config: %+v", cfg)
	}
	if cfg.ReporterURL != "http://pamawas-reporter:8080" || cfg.DailyReportTime != "07:00" || !cfg.EnableHighSeverityAlert || cfg.Mode != "auto" {
		t.Fatalf("defaults not applied: %+v", cfg)
	}
}

func TestLoadPanicsWithoutDatabaseURL(t *testing.T) {
	inTempDir(t)
	t.Setenv("PAMAWAS_SCHEDULER_DATABASE_URL", "")
	defer func() {
		var got interface{}
		if got = recover(); got == nil {
			t.Fatalf("expected panic, got nil")
			return
		}
		msg, ok := got.(string)
		if !ok || !strings.Contains(msg, "database_url is required") {
			t.Fatalf("unexpected panic: %v", got)
		}
	}()
	Load()
}

func TestLoadPanicsForInvalidInterval(t *testing.T) {
	inTempDir(t)
	t.Setenv("PAMAWAS_SCHEDULER_DATABASE_URL", "db")
	t.Setenv("PAMAWAS_SCHEDULER_CHECK_INTERVAL", "later")
	defer func() {
		var got interface{}
		if got = recover(); got == nil {
			t.Fatalf("expected panic, got nil")
			return
		}
		msg, ok := got.(string)
		if !ok || !strings.Contains(msg, "invalid check_interval") {
			t.Fatalf("unexpected panic: %v", got)
		}
	}()
	Load()
}

func TestValidate(t *testing.T) {
	base := Config{
		DatabaseURL:           "db",
		Port:                  "8080",
		CheckInterval:         time.Second,
		ReporterURL:           "http://reporter",
		ServiceToken:          "token",
		DailyReportTime:       "07:00",
		HighSeverityThreshold: "high",
		DefaultTimezone:       "Asia/Jakarta",
		RateLimitRPS:          10,
		RateLimitBurst:        20,
	}
	if err := base.Validate(); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name string
		cfg  Config
		want string
	}{
		{"database", func() Config { c := base; c.DatabaseURL = ""; return c }(), "database_url"},
		{"service_token", func() Config { c := base; c.ServiceToken = ""; return c }(), "service_token"},
		{"port", func() Config { c := base; c.Port = ""; return c }(), "port"},
		{"interval", func() Config { c := base; c.CheckInterval = 0; return c }(), "check_interval"},
		{"reporter_url", func() Config { c := base; c.ReporterURL = ""; return c }(), "reporter_url"},
		{"daily_time", func() Config { c := base; c.DailyReportTime = "7am"; return c }(), "daily_report_time"},
		{"threshold", func() Config { c := base; c.HighSeverityThreshold = "urgent"; return c }(), "high_severity_threshold"},
		{"timezone", func() Config { c := base; c.DefaultTimezone = "Mars/Tharsis"; return c }(), "default_timezone"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.cfg.Validate(); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}
