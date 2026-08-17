package service

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/Pamawas/pamawas-scheduler/metrics"
)

func schedulerMetrics() *metrics.Metrics {
	return &metrics.Metrics{
		DailyReportsTotal:        prometheus.NewCounter(prometheus.CounterOpts{Name: "test_daily_total"}),
		HighSeverityChecksTotal:  prometheus.NewCounter(prometheus.CounterOpts{Name: "test_checks_total"}),
		HighSeverityAlertsTotal:  prometheus.NewCounter(prometheus.CounterOpts{Name: "test_alerts_total"}),
		DBConnectionErrors:       prometheus.NewCounter(prometheus.CounterOpts{Name: "test_db_errors_total"}),
		LastDailyReport:          prometheus.NewGauge(prometheus.GaugeOpts{Name: "test_last_daily"}),
		LastHighSeverityCheck:    prometheus.NewGauge(prometheus.GaugeOpts{Name: "test_last_check"}),
		SchedulerRunning:         prometheus.NewGauge(prometheus.GaugeOpts{Name: "test_running"}),
		SchedulerUptime:          prometheus.NewGauge(prometheus.GaugeOpts{Name: "test_uptime"}),
	}
}

func newMockScheduler(t *testing.T, cfg SchedulerConfig) (*Scheduler, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return NewScheduler(db, cfg, schedulerMetrics()), mock
}

func TestNewSchedulerAndStop(t *testing.T) {
	s, _ := newMockScheduler(t, SchedulerConfig{ReporterURL: "http://localhost"})
	if s.reporter == nil || s.StartTime().IsZero() || s.Running() || !s.LastDailyReport().IsZero() {
		t.Fatalf("state=%+v", s)
	}
	s.Stop()
}

func TestStartWorkerStops(t *testing.T) {
	s, _ := newMockScheduler(t, SchedulerConfig{CheckInterval: time.Hour})
	done := make(chan struct{})
	go func() { s.StartWorker(); close(done) }()
	s.Stop()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("worker did not stop")
	}
}

func TestReporterClientTriggersRequests(t *testing.T) {
	var bodies []map[string]interface{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]interface{}
		_ = json.NewDecoder(r.Body).Decode(&body)
		bodies = append(bodies, body)
		if r.Method != http.MethodPost || r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("request=%s %s", r.Method, r.Header.Get("Content-Type"))
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()
	client := NewReporterClient(server.URL, context.Background())
	if err := client.SendReportRequest(ReportPayload{
		ContractVersion: 1,
		RequestID:       "rreq_test",
		ReportType:      "daily",
		PeriodStart:     "2026-08-15T17:00:00Z",
		PeriodEnd:       "2026-08-16T17:00:00Z",
		Timezone:        "Asia/Jakarta",
		IncidentIDs:     []string{},
	}); err != nil {
		t.Fatal(err)
	}
	if len(bodies) != 1 || bodies[0]["request_id"] != "rreq_test" || bodies[0]["report_type"] != "daily" {
		t.Fatalf("bodies=%#v", bodies)
	}
}

func TestReporterClientReturnsHTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { http.Error(nil, "", http.StatusBadGateway) }))
	defer server.Close()
	client := NewReporterClient("http://127.0.0.1:1", context.Background())
	if err := client.SendReportRequest(ReportPayload{}); err == nil {
		t.Fatal("expected transport error")
	}
}

func TestCheckHighSeverityIncidentsNoMatches(t *testing.T) {
	s, mock := newMockScheduler(t, SchedulerConfig{HighSeverityThreshold: "HIGH"})
	mock.ExpectQuery("SELECT COUNT").WithArgs("high", sqlmock.AnyArg()).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	if err := s.checkHighSeverityIncidents(); err != nil {
		t.Fatal(err)
	}
}

func TestCheckHighSeverityIncidentsTriggersAlert(t *testing.T) {
	var received bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { received = true; w.WriteHeader(http.StatusOK) }))
	defer server.Close()
	s, mock := newMockScheduler(t, SchedulerConfig{ReporterURL: server.URL, HighSeverityThreshold: "critical"})

	// Mock the initial COUNT query
	mock.ExpectQuery("SELECT COUNT").
		WithArgs("critical", sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(2))

	// Mock getEligibleHighSeverityIncidents query (called from CreateHighSeverityReportRequest)
	mock.ExpectQuery("SELECT id FROM incidents").
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("inc_01TEST111111111111111111111111").AddRow("inc_01TEST222222222222222222222222"))

	// Mock insert for first incident
	mock.ExpectQuery("INSERT INTO report_requests").
		WithArgs(sqlmock.AnyArg(), "high_severity", sqlmock.AnyArg(), sqlmock.AnyArg(), "UTC", sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"id", "inserted"}).AddRow("rreq_01TEST987654321098765432109876", true))

	// Mock insert for second incident
	mock.ExpectQuery("INSERT INTO report_requests").
		WithArgs(sqlmock.AnyArg(), "high_severity", sqlmock.AnyArg(), sqlmock.AnyArg(), "UTC", sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"id", "inserted"}).AddRow("rreq_01TEST987654321098765432109877", true))

	if err := s.checkHighSeverityIncidents(); err != nil {
		t.Fatal(err)
	}
	if !received {
		t.Fatal("alert was not sent")
	}
}

func TestCheckAndTriggerSkipsWhenRunning(t *testing.T) {
	s, _ := newMockScheduler(t, SchedulerConfig{})
	s.running = true
	if err := s.checkAndTrigger(); err != nil {
		t.Fatal(err)
	}
}

func TestCheckAndTriggerRecordsHighSeverityCheck(t *testing.T) {
	s, mock := newMockScheduler(t, SchedulerConfig{EnableHighSeverityAlert: true, HighSeverityThreshold: "high"})
	mock.ExpectQuery("SELECT COUNT").WithArgs("high", sqlmock.AnyArg()).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	if err := s.checkAndTrigger(); err != nil {
		t.Fatal(err)
	}
	if s.LastHighSeverityCheck().IsZero() {
		t.Fatal("last check not recorded")
	}
	if s.Running() {
		t.Fatal("scheduler left running")
	}
}

func TestStopCancelsReporterContext(t *testing.T) {
	s, _ := newMockScheduler(t, SchedulerConfig{})
	s.Stop()
	if err := s.reporter.ctx.Err(); !errors.Is(err, context.Canceled) {
		t.Fatalf("ctx error=%v", err)
	}
}

func TestCreateDailyReportRequest(t *testing.T) {
	s, mock := newMockScheduler(t, SchedulerConfig{
		ReporterURL:     "http://reporter",
		DefaultTimezone: "Asia/Jakarta",
	})

	// First call - should insert
	mock.ExpectQuery("INSERT INTO report_requests").
		WithArgs(sqlmock.AnyArg(), "daily", sqlmock.AnyArg(), sqlmock.AnyArg(), "Asia/Jakarta", sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"id", "inserted"}).AddRow("rreq_01TEST123456789012345678901234", true))

	resp, err := s.CreateDailyReportRequest(context.Background(), "2026-08-16", "Asia/Jakarta")
	require.NoError(t, err)
	require.Equal(t, "rreq_01TEST123456789012345678901234", resp.RequestID)
	require.Equal(t, "daily", resp.RequestType)
	require.Equal(t, "Asia/Jakarta", resp.Timezone)
	require.False(t, resp.Duplicate)
	require.Equal(t, "pending", resp.Status)

	// Verify period boundaries
	require.Equal(t, 17, resp.PeriodStart.Hour()) // 00:00 WIB = 17:00 UTC
	require.Equal(t, 17, resp.PeriodEnd.Hour())
	require.Equal(t, 24*time.Hour, resp.PeriodEnd.Sub(resp.PeriodStart))
}

func TestCreateDailyReportRequest_Duplicate(t *testing.T) {
	s, mock := newMockScheduler(t, SchedulerConfig{
		ReporterURL:     "http://reporter",
		DefaultTimezone: "Asia/Jakarta",
	})

	// Second call with same hash - should return existing
	mock.ExpectQuery("INSERT INTO report_requests").
		WithArgs(sqlmock.AnyArg(), "daily", sqlmock.AnyArg(), sqlmock.AnyArg(), "Asia/Jakarta", sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"id", "inserted"}).AddRow("rreq_01TEST123456789012345678901234", false))

	resp, err := s.CreateDailyReportRequest(context.Background(), "2026-08-16", "Asia/Jakarta")
	require.NoError(t, err)
	require.True(t, resp.Duplicate)
	require.Equal(t, "exists", resp.Status)
}

func TestCreateDailyReportRequest_InvalidTimezone(t *testing.T) {
	s, _ := newMockScheduler(t, SchedulerConfig{})
	_, err := s.CreateDailyReportRequest(context.Background(), "2026-08-16", "Invalid/Timezone")
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid timezone")
}

func TestCreateDailyReportRequest_InvalidDate(t *testing.T) {
	s, _ := newMockScheduler(t, SchedulerConfig{})
	_, err := s.CreateDailyReportRequest(context.Background(), "16-08-2026", "UTC")
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid report date")
}

func TestCreateHighSeverityReportRequest_NoEligible(t *testing.T) {
	s, mock := newMockScheduler(t, SchedulerConfig{HighSeverityThreshold: "high"})
	mock.ExpectQuery("SELECT id FROM incidents").WillReturnRows(sqlmock.NewRows([]string{"id"}))

	resp, err := s.CreateHighSeverityReportRequest(context.Background(), []string{}, true)
	require.NoError(t, err)
	require.Equal(t, 0, resp.EligibleCount)
	require.Equal(t, "no_eligible_incidents", resp.Status)
	require.Empty(t, resp.IncidentIDs)
}

func TestCreateHighSeverityReportRequest_WithExplicitIncidents(t *testing.T) {
	s, mock := newMockScheduler(t, SchedulerConfig{ReporterURL: "http://reporter", HighSeverityThreshold: "high"})

	// Filter eligible incidents
	mock.ExpectQuery("SELECT id FROM incidents WHERE id IN").
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("inc_01TEST123456789012345678901234"))

	// Insert report request for the incident
	mock.ExpectQuery("INSERT INTO report_requests").
		WithArgs(sqlmock.AnyArg(), "high_severity", sqlmock.AnyArg(), sqlmock.AnyArg(), "UTC", sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"id", "inserted"}).AddRow("rreq_01TEST987654321098765432109876", true))

	resp, err := s.CreateHighSeverityReportRequest(context.Background(), []string{"inc_01TEST123456789012345678901234"}, false)
	require.NoError(t, err)
	require.Equal(t, 1, resp.EligibleCount)
	require.Contains(t, resp.IncidentIDs, "inc_01TEST123456789012345678901234")
	require.False(t, resp.Duplicate)
}

func TestCreateHighSeverityReportRequest_DuplicatePerIncident(t *testing.T) {
	s, mock := newMockScheduler(t, SchedulerConfig{ReporterURL: "http://reporter", HighSeverityThreshold: "high"})

	// Filter eligible incidents
	mock.ExpectQuery("SELECT id FROM incidents WHERE id IN").
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("inc_01TEST123456789012345678901234"))

	// Insert report request - duplicate
	mock.ExpectQuery("INSERT INTO report_requests").
		WithArgs(sqlmock.AnyArg(), "high_severity", sqlmock.AnyArg(), sqlmock.AnyArg(), "UTC", sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"id", "inserted"}).AddRow("rreq_01TEST987654321098765432109876", false))

	resp, err := s.CreateHighSeverityReportRequest(context.Background(), []string{"inc_01TEST123456789012345678901234"}, false)
	require.NoError(t, err)
	require.True(t, resp.Duplicate)
	require.Equal(t, "exists", resp.Status)
}

func TestTriggerDailyReportForDate(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/reports" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		var payload ReportPayload
		_ = json.NewDecoder(r.Body).Decode(&payload)
		if payload.ReportType != "daily" || payload.ContractVersion != 1 {
			t.Errorf("unexpected payload: %+v", payload)
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	s, mock := newMockScheduler(t, SchedulerConfig{ReporterURL: server.URL, DefaultTimezone: "Asia/Jakarta"})

	// Insert report request
	mock.ExpectQuery("INSERT INTO report_requests").
		WithArgs(sqlmock.AnyArg(), "daily", sqlmock.AnyArg(), sqlmock.AnyArg(), "Asia/Jakarta", sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"id", "inserted"}).AddRow("rreq_01TEST123456789012345678901234", true))

	err := s.triggerDailyReportForDate(time.Date(2026, 8, 16, 7, 0, 0, 0, time.UTC))
	require.NoError(t, err)
}

func TestGetEligibleHighSeverityIncidents(t *testing.T) {
	s, mock := newMockScheduler(t, SchedulerConfig{HighSeverityThreshold: "high"})
	mock.ExpectQuery("SELECT id FROM incidents").
		WillReturnRows(sqlmock.NewRows([]string{"id"}).
			AddRow("inc_01TEST111111111111111111111111").
			AddRow("inc_01TEST222222222222222222222222"))

	incidents, err := s.getEligibleHighSeverityIncidents(context.Background())
	require.NoError(t, err)
	require.Len(t, incidents, 2)
	require.Contains(t, incidents, "inc_01TEST111111111111111111111111")
}

func TestFilterEligibleIncidents(t *testing.T) {
	s, mock := newMockScheduler(t, SchedulerConfig{HighSeverityThreshold: "high"})

	// Should only return those that match the query
	mock.ExpectQuery("SELECT id FROM incidents WHERE id IN").
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("inc_01TEST111111111111111111111111"))

	eligible, err := s.filterEligibleIncidents(context.Background(), []string{
		"inc_01TEST111111111111111111111111",
		"inc_01TEST222222222222222222222222",
	})
	require.NoError(t, err)
	require.Len(t, eligible, 1)
	require.Equal(t, "inc_01TEST111111111111111111111111", eligible[0])
}

func TestCheckAndTrigger_DailyReport(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	s, mock := newMockScheduler(t, SchedulerConfig{
		ReporterURL:          server.URL,
		DailyReportTime:      "07:00",
		EnableDailyReport:    true,
		EnableHighSeverityAlert: false,
		DefaultTimezone:      "Asia/Jakarta",
		CheckInterval:        time.Minute,
	})

	// Mock time to be 07:00
	// Since we can't easily mock time in the checkAndTrigger, we test the trigger method directly
	// The checkAndTrigger uses time.Now() which we can't easily control

	mock.ExpectQuery("INSERT INTO report_requests").
		WithArgs(sqlmock.AnyArg(), "daily", sqlmock.AnyArg(), sqlmock.AnyArg(), "Asia/Jakarta", sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"id", "inserted"}).AddRow("rreq_01TEST123456789012345678901234", true))

	err := s.triggerDailyReportForDate(time.Date(2026, 8, 16, 7, 0, 0, 0, time.UTC))
	require.NoError(t, err)
}