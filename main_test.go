package main

import (
	"database/sql"
	"testing"

	_ "github.com/lib/pq"

	"github.com/Pamawas/pamawas-scheduler/service"
)

// TestNewScheduler tests that the scheduler can be created and its methods exist.
func TestNewScheduler(t *testing.T) {
	db, err := sql.Open("postgres", "postgres://localhost/test?sslmode=disable")
	if err != nil {
		t.Skipf("Skipping test: cannot open database: %v", err)
	}
	defer func() {
		if closeErr := db.Close(); closeErr != nil {
			t.Logf("Failed to close database: %v", closeErr)
		}
	}()
	s := service.NewScheduler(db, service.SchedulerConfig{}, nil)
	if s == nil {
		t.Error("NewScheduler returned nil")
	}
	// Test that methods exist
	_ = s.LastDailyReport()
	_ = s.LastHighSeverityCheck()
	_ = s.Running()
	_ = s.StartTime()
}
