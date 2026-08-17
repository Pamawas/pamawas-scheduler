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
	t.Cleanup(func() { _ = os.Chdir(old) })
}

func TestLoadDefaultsAndEnvironmentOverrides(t *testing.T) {
	inTempDir(t)
	t.Setenv("PAMAWAS_SCHEDULER_DATABASE_URL", "postgres://example/test")
	t.Setenv("PAMAWAS_SCHEDULER_PORT", "9090")
	t.Setenv("PAMAWAS_SCHEDULER_CHECK_INTERVAL", "2m")
	t.Setenv("PAMAWAS_SCHEDULER_ENABLE_DAILY_REPORT", "false")
	cfg := Load()
	if cfg.DatabaseURL != "postgres://example/test" || cfg.Port != "9090" || cfg.CheckInterval != 2*time.Minute || cfg.EnableDailyReport {
		t.Fatalf("unexpected config: %+v", cfg)
	}
	if cfg.ReporterURL != "http://localhost:8081" || cfg.DailyReportTime != "07:00" || !cfg.EnableHighSeverityAlert || cfg.Mode != "auto" {
		t.Fatalf("defaults not applied: %+v", cfg)
	}
}

func TestLoadPanicsWithoutDatabaseURL(t *testing.T) {
	inTempDir(t)
	t.Setenv("PAMAWAS_SCHEDULER_DATABASE_URL", "")
	defer func() {
		if got := recover(); got == nil || !strings.Contains(got.(string), "DATABASE_URL not set") {
			t.Fatalf("panic = %v", got)
		}
	}()
	Load()
}

func TestLoadPanicsForInvalidInterval(t *testing.T) {
	inTempDir(t)
	t.Setenv("PAMAWAS_SCHEDULER_DATABASE_URL", "db")
	t.Setenv("PAMAWAS_SCHEDULER_CHECK_INTERVAL", "later")
	defer func() {
		if got := recover(); got == nil || !strings.Contains(got.(string), "invalid check_interval") {
			t.Fatalf("panic = %v", got)
		}
	}()
	Load()
}

func TestValidate(t *testing.T) {
	if err := (Config{DatabaseURL: "db", Port: "8080", CheckInterval: time.Second}).Validate(); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name string
		cfg  Config
		want string
	}{
		{"database", Config{Port: "8080", CheckInterval: time.Second}, "database_url"},
		{"port", Config{DatabaseURL: "db", CheckInterval: time.Second}, "port"},
		{"interval", Config{DatabaseURL: "db", Port: "8080"}, "check_interval"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.cfg.Validate(); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}
