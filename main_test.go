package main

import (
	"database/sql"
	"net/http"
	"net/http/httptest"
	"testing"

	_ "github.com/lib/pq"

	"github.com/Pamawas/pamawas-scheduler/service"
)

func TestMetricsRoutes(t *testing.T) {
	mux := http.NewServeMux()
	registerMetricsRoutes(mux, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	for _, test := range []struct {
		path       string
		statusCode int
	}{
		{path: "/metrics", statusCode: http.StatusOK},
		{path: "/api/v1/metrics", statusCode: http.StatusNotFound},
	} {
		t.Run(test.path, func(t *testing.T) {
			response := httptest.NewRecorder()
			mux.ServeHTTP(response, httptest.NewRequest(http.MethodGet, test.path, nil))
			if response.Code != test.statusCode {
				t.Fatalf("GET %s returned status %d, want %d", test.path, response.Code, test.statusCode)
			}
		})
	}
}

// TestNewScheduler tests that the scheduler can be created and its methods exist.
func TestNewScheduler(t *testing.T) {
	db, err := sql.Open("postgres", "postgres://localhost/test?sslmode=disable")
	if err != nil {
		t.Fatalf("cannot open database: %v", err)
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
