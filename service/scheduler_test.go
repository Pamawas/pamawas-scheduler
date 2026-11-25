package service

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/Pamawas/pamawas-scheduler/metrics"
)

func schedulerMetrics() *metrics.Metrics {
	return &metrics.Metrics{
		DailyReportsTotal:       prometheus.NewCounter(prometheus.CounterOpts{Name: "test_daily_total"}),
		HighSeverityChecksTotal: prometheus.NewCounter(prometheus.CounterOpts{Name: "test_checks_total"}),
		HighSeverityAlertsTotal: prometheus.NewCounter(prometheus.CounterOpts{Name: "test_alerts_total"}),
		DBConnectionErrors:      prometheus.NewCounter(prometheus.CounterOpts{Name: "test_db_errors_total"}),
		LastDailyReport:         prometheus.NewGauge(prometheus.GaugeOpts{Name: "test_last_daily"}),
		LastHighSeverityCheck:   prometheus.NewGauge(prometheus.GaugeOpts{Name: "test_last_check"}),
		SchedulerRunning:        prometheus.NewGauge(prometheus.GaugeOpts{Name: "test_running"}),
		SchedulerUptime:         prometheus.NewGauge(prometheus.GaugeOpts{Name: "test_uptime"}),
		SchedulerLeader:         prometheus.NewGauge(prometheus.GaugeOpts{Name: "test_leader"}),
	}
}

func newMockScheduler(t *testing.T, cfg SchedulerConfig) (*Scheduler, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Logf("failed to close db: %v", err)
		}
	})
	// Tests construct SchedulerConfig{} literals; NewScheduler requires a
	// valid timezone, so supply one unless the test set its own.
	if cfg.DefaultTimezone == "" {
		cfg.DefaultTimezone = "UTC"
	}
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
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode error: %v", err)
			return
		}
		bodies = append(bodies, body)
		if r.Method != http.MethodPost || r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("request=%s %s", r.Method, r.Header.Get("Content-Type"))
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()
	client := NewReporterClient(server.URL, "test-token", context.Background())
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
	client := NewReporterClient("http://127.0.0.1:1", "test-token", context.Background())
	if err := client.SendReportRequest(ReportPayload{}); err == nil {
		t.Fatal("expected transport error")
	}
}

// TestReporterClientSendAsyncDoesNotBlock verifies that the async dispatcher
// returns before a slow reporter responds, so it can never stall the scheduler
// tick loop.
func TestReporterClientSendAsyncDoesNotBlock(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release // hold the response open until the test releases it
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	client := NewReporterClient(server.URL, "test-token", context.Background())

	// The held-open request would keep SendReportRequest blocking for the full
	// client timeout; the async dispatch must return well before that.
	start := time.Now()
	client.SendReportRequestAsync(ReportPayload{RequestID: "rreq_async", ReportType: "high_severity"})
	if elapsed := time.Since(start); elapsed >= 100*time.Millisecond {
		t.Fatalf("SendReportRequestAsync blocked on a slow reporter: %s", elapsed)
	}

	close(release) // release the in-flight request
	client.Close() // drain the async send so the test can assert cleanly
}

func TestCheckHighSeverityIncidentsNoMatches(t *testing.T) {
	s, mock := newMockScheduler(t, SchedulerConfig{HighSeverityThreshold: "HIGH"})
	mock.ExpectQuery("SELECT id FROM incidents").
		WithArgs(4).
		WillReturnRows(sqlmock.NewRows([]string{"id"}))
	if err := s.checkHighSeverityIncidents(); err != nil {
		t.Fatal(err)
	}
}

func TestSeverityRank(t *testing.T) {
	cases := map[string]int{
		"":         0,
		"unknown":  0,
		"debug":    1,
		"info":     2,
		"warning":  3,
		"high":     4,
		"critical": 5,
		"WARNING":  3, // case-insensitive
		"Critical": 5,
		"DEBUG":    1,
	}
	for in, want := range cases {
		if got := severityRank(in); got != want {
			t.Errorf("severityRank(%q) = %d, want %d", in, got, want)
		}
	}
}

// TestCheckHighSeverityIncidentsTriggersAlert verifies that checkHighSeverityIncidents
// creates pending report requests (without dispatching to the reporter) for
// eligible high-severity incidents. The reporter owns the delivery channels
// (immediate alerts come event-driven from ingest).
func TestCheckHighSeverityIncidentsTriggersAlert(t *testing.T) {
	s, mock := newMockScheduler(t, SchedulerConfig{HighSeverityThreshold: "critical"})

	// checkHighSeverityIncidents calls getEligibleHighSeverityIncidents directly.
	mock.ExpectQuery("SELECT id FROM incidents").
		WithArgs(5).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).
			AddRow("inc_01TEST111111111111111111111111").
			AddRow("inc_01TEST222222222222222222222222"))

	// CreateHighSeverityReportRequest (evaluate=true) calls it again to filter eligible incidents.
	mock.ExpectQuery("SELECT id FROM incidents").
		WithArgs(5).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).
			AddRow("inc_01TEST111111111111111111111111").
			AddRow("inc_01TEST222222222222222222222222"))

	// CreateHighSeverityReportRequest inserts one report request per incident (dispatch=false).
	mock.ExpectQuery("INSERT INTO report_requests").
		WithArgs(sqlmock.AnyArg(), "high_severity", sqlmock.AnyArg(), sqlmock.AnyArg(), "UTC", sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"id", "inserted"}).AddRow("rreq_01TEST987654321098765432109876", true))

	mock.ExpectQuery("INSERT INTO report_requests").
		WithArgs(sqlmock.AnyArg(), "high_severity", sqlmock.AnyArg(), sqlmock.AnyArg(), "UTC", sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"id", "inserted"}).AddRow("rreq_01TEST987654321098765432109877", true))

	if err := s.checkHighSeverityIncidents(); err != nil {
		t.Fatal(err)
	}
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestCheckHighSeverityFollowUpDispatchesOnTerminal verifies that checkHighSeverityFollowUp
// dispatches a pending high-severity report request once an investigation run reaches a
// terminal state.
func TestCheckHighSeverityFollowUpDispatchesOnTerminal(t *testing.T) {
	requestReceived := make(chan ReportPayload, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/reports" {
			t.Errorf("unexpected reporter path: %s", r.URL.Path)
		}
		var payload ReportPayload
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Errorf("failed to decode reporter payload: %v", err)
			return
		}
		requestReceived <- payload
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	s, mock := newMockScheduler(t, SchedulerConfig{
		ReporterURL:             server.URL,
		EnableHighSeverityAlert: true,
		HighSeverityThreshold:   "high",
		DefaultTimezone:         "Asia/Jakarta",
	})
	defer s.reporter.Close()

	// Pending high-severity report request, incident recorded in its own column.
	mock.ExpectQuery("SELECT rr.id, rr.incident_id FROM report_requests rr").
		WillReturnRows(sqlmock.NewRows([]string{"id", "incident_id"}).
			AddRow("rreq_01TEST123456789012345678901234", "inc_01TEST111111111111111111111111"))

	// Investigation run in terminal state.
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM investigation_runs").
		WithArgs("inc_01TEST111111111111111111111111").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))

	if err := s.checkHighSeverityFollowUp(); err != nil {
		t.Fatal(err)
	}
	require.NoError(t, mock.ExpectationsWereMet())

	s.reporter.Close()
	select {
	case payload := <-requestReceived:
		require.Equal(t, "rreq_01TEST123456789012345678901234", payload.RequestID)
		require.Equal(t, []string{"inc_01TEST111111111111111111111111"}, payload.IncidentIDs)
		require.Equal(t, "high_severity", payload.ReportType)
		require.Equal(t, "Asia/Jakarta", payload.Timezone)
	case <-time.After(time.Second):
		t.Fatal("follow-up did not dispatch report request after terminal investigation")
	}
}

// TestCheckHighSeverityFollowUpSkipsNonTerminal verifies that checkHighSeverityFollowUp
// leaves a pending request alone when no investigation run has reached a terminal state.
func TestCheckHighSeverityFollowUpSkipsNonTerminal(t *testing.T) {
	s, mock := newMockScheduler(t, SchedulerConfig{
		EnableHighSeverityAlert: true,
		HighSeverityThreshold:   "high",
	})

	mock.ExpectQuery("SELECT rr.id, rr.incident_id FROM report_requests rr").
		WillReturnRows(sqlmock.NewRows([]string{"id", "incident_id"}).
			AddRow("rreq_01TEST123456789012345678901234", "inc_01TEST111111111111111111111111"))

	// No terminal investigation runs.
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM investigation_runs").
		WithArgs("inc_01TEST111111111111111111111111").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))

	if err := s.checkHighSeverityFollowUp(); err != nil {
		t.Fatal(err)
	}
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestCheckHighSeverityFollowUpSkipsWhenDisabled verifies that checkHighSeverityFollowUp
// returns immediately when high-severity alerts are disabled.
func TestCheckHighSeverityFollowUpSkipsWhenDisabled(t *testing.T) {
	s, mock := newMockScheduler(t, SchedulerConfig{
		EnableHighSeverityAlert: false,
		HighSeverityThreshold:   "high",
	})

	// No DB queries expected when disabled.
	if err := s.checkHighSeverityFollowUp(); err != nil {
		t.Fatal(err)
	}
	require.NoError(t, mock.ExpectationsWereMet())
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
	mock.ExpectQuery("SELECT id FROM incidents").
		WithArgs(4).
		WillReturnRows(sqlmock.NewRows([]string{"id"}))
	mock.ExpectQuery("SELECT rr.id, rr.incident_id FROM report_requests rr").
		WillReturnRows(sqlmock.NewRows([]string{"id", "idempotency_hash"}))
	if err := s.checkAndTrigger(); err != nil {
		t.Fatal(err)
	}
	if s.LastHighSeverityCheck().IsZero() {
		t.Fatal("last check not recorded")
	}
	if s.Running() {
		t.Fatal("scheduler left running")
	}
	require.NoError(t, mock.ExpectationsWereMet())
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
		WithArgs(sqlmock.AnyArg(), "daily", sqlmock.AnyArg(), sqlmock.AnyArg(), "Asia/Jakarta", sqlmock.AnyArg(), "").
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
		WithArgs(sqlmock.AnyArg(), "daily", sqlmock.AnyArg(), sqlmock.AnyArg(), "Asia/Jakarta", sqlmock.AnyArg(), "").
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
	mock.ExpectQuery("SELECT id FROM incidents").
		WithArgs(4).
		WillReturnRows(sqlmock.NewRows([]string{"id"}))

	resp, err := s.CreateHighSeverityReportRequest(context.Background(), []string{}, true, false)
	require.NoError(t, err)
	require.Equal(t, 0, resp.EligibleCount)
	require.Equal(t, "no_eligible_incidents", resp.Status)
	require.Empty(t, resp.IncidentIDs)
}

func TestCreateHighSeverityReportRequest_WithExplicitIncidents(t *testing.T) {
	s, mock := newMockScheduler(t, SchedulerConfig{
		ReporterURL: "http://reporter", HighSeverityThreshold: "high",
		DefaultTimezone: "Asia/Jakarta",
	})

	// Filter eligible incidents
	mock.ExpectQuery("SELECT id FROM incidents WHERE id = ANY").
		WithArgs(sqlmock.AnyArg(), 4).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("inc_01TEST123456789012345678901234"))

	// Insert report request for the incident
	mock.ExpectQuery("INSERT INTO report_requests").
		WithArgs(sqlmock.AnyArg(), "high_severity", sqlmock.AnyArg(), sqlmock.AnyArg(), "Asia/Jakarta", sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"id", "inserted"}).AddRow("rreq_01TEST987654321098765432109876", true))

	resp, err := s.CreateHighSeverityReportRequest(context.Background(), []string{"inc_01TEST123456789012345678901234"}, false, false)
	require.NoError(t, err)
	require.Equal(t, 1, resp.EligibleCount)
	require.Contains(t, resp.IncidentIDs, "inc_01TEST123456789012345678901234")
	require.False(t, resp.Duplicate)
}

func TestCreateHighSeverityReportRequest_DuplicatePerIncident(t *testing.T) {
	s, mock := newMockScheduler(t, SchedulerConfig{ReporterURL: "http://reporter", HighSeverityThreshold: "high"})

	// Filter eligible incidents
	mock.ExpectQuery("SELECT id FROM incidents WHERE id = ANY").
		WithArgs(sqlmock.AnyArg(), 4).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("inc_01TEST123456789012345678901234"))

	// Insert report request - duplicate
	mock.ExpectQuery("INSERT INTO report_requests").
		WithArgs(sqlmock.AnyArg(), "high_severity", sqlmock.AnyArg(), sqlmock.AnyArg(), "UTC", sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"id", "inserted"}).AddRow("rreq_01TEST987654321098765432109876", false))

	mock.ExpectQuery("UPDATE report_requests").
		WithArgs("rreq_01TEST987654321098765432109876").
		WillReturnRows(sqlmock.NewRows([]string{"id"}))

	resp, err := s.CreateHighSeverityReportRequest(context.Background(), []string{"inc_01TEST123456789012345678901234"}, false, false)
	require.NoError(t, err)
	require.True(t, resp.Duplicate)
	require.Equal(t, "exists", resp.Status)
}

func TestCreateHighSeverityReportRequest_RequeuesStaleRequest(t *testing.T) {
	requestReceived := make(chan ReportPayload, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/v1/reports" {
			t.Errorf("unexpected reporter request: %s %s", r.Method, r.URL.Path)
		}
		var payload ReportPayload
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Errorf("failed to decode reporter payload: %v", err)
			return
		}
		requestReceived <- payload
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	s, mock := newMockScheduler(t, SchedulerConfig{ReporterURL: server.URL, HighSeverityThreshold: "high"})
	defer s.reporter.Close()

	mock.ExpectQuery("SELECT id FROM incidents WHERE id = ANY").
		WithArgs(sqlmock.AnyArg(), 4).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("inc_01TEST123456789012345678901234"))
	mock.ExpectQuery("INSERT INTO report_requests").
		WithArgs(sqlmock.AnyArg(), "high_severity", sqlmock.AnyArg(), sqlmock.AnyArg(), "UTC", sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"id", "inserted"}).AddRow("rreq_01TEST987654321098765432109876", false))
	mock.ExpectQuery("UPDATE report_requests").
		WithArgs("rreq_01TEST987654321098765432109876").
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("rreq_01TEST987654321098765432109876"))

	resp, err := s.CreateHighSeverityReportRequest(context.Background(), []string{"inc_01TEST123456789012345678901234"}, false, true)
	require.NoError(t, err)
	require.False(t, resp.Duplicate)
	require.Equal(t, "pending", resp.Status)

	s.reporter.Close()
	select {
	case payload := <-requestReceived:
		require.Equal(t, "rreq_01TEST987654321098765432109876", payload.RequestID)
		require.Equal(t, []string{"inc_01TEST123456789012345678901234"}, payload.IncidentIDs)
	case <-time.After(time.Second):
		t.Fatal("stale report request was not dispatched")
	}
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestTriggerDailyReportForDate(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/reports" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		var payload ReportPayload
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Errorf("decode error: %v", err)
		}
		if payload.ReportType != "daily" || payload.ContractVersion != 1 {
			t.Errorf("unexpected payload: %+v", payload)
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	s, mock := newMockScheduler(t, SchedulerConfig{ReporterURL: server.URL, DefaultTimezone: "Asia/Jakarta"})

	// Insert report request
	mock.ExpectQuery("INSERT INTO report_requests").
		WithArgs(sqlmock.AnyArg(), "daily", sqlmock.AnyArg(), sqlmock.AnyArg(), "Asia/Jakarta", sqlmock.AnyArg(), "").
		WillReturnRows(sqlmock.NewRows([]string{"id", "inserted"}).AddRow("rreq_01TEST123456789012345678901234", true))

	err := s.triggerDailyReportForDate(time.Date(2026, 8, 16, 7, 0, 0, 0, time.UTC))
	require.NoError(t, err)
}

func TestGetEligibleHighSeverityIncidents(t *testing.T) {
	s, mock := newMockScheduler(t, SchedulerConfig{HighSeverityThreshold: "high"})
	mock.ExpectQuery("SELECT id FROM incidents").
		WithArgs(4).
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
	mock.ExpectQuery("SELECT id FROM incidents WHERE id = ANY").
		WithArgs(sqlmock.AnyArg(), 4).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("inc_01TEST111111111111111111111111"))

	eligible, err := s.filterEligibleIncidents(context.Background(), []string{
		"inc_01TEST111111111111111111111111",
		"inc_01TEST222222222222222222222222",
	})
	require.NoError(t, err)
	require.Len(t, eligible, 1)
	require.Equal(t, "inc_01TEST111111111111111111111111", eligible[0])
}

func TestParseDailyReportTime(t *testing.T) {
	tests := []struct {
		name      string
		timeStr   string
		wantHour  int
		wantMin   int
		wantError bool
	}{
		{"valid time 07:00", "07:00", 7, 0, false},
		{"valid time 00:00", "00:00", 0, 0, false},
		{"valid time 23:59", "23:59", 23, 59, false},
		{"invalid format missing minutes", "07", 0, 0, true},
		{"empty string", "", 0, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotHour, gotMin, err := parseDailyReportTime(tt.timeStr)
			if tt.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.wantHour, gotHour)
				require.Equal(t, tt.wantMin, gotMin)
			}
		})
	}
}

func TestCheckAndTrigger_DailyReport(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	s, mock := newMockScheduler(t, SchedulerConfig{
		ReporterURL:             server.URL,
		DailyReportTime:         "07:00",
		EnableDailyReport:       true,
		EnableHighSeverityAlert: false,
		DefaultTimezone:         "Asia/Jakarta",
		CheckInterval:           time.Minute,
	})

	// Mock time to be 07:00
	// Since we can't easily mock time in the checkAndTrigger, we test the trigger method directly
	// The checkAndTrigger uses time.Now() which we can't easily control

	mock.ExpectQuery("INSERT INTO report_requests").
		WithArgs(sqlmock.AnyArg(), "daily", sqlmock.AnyArg(), sqlmock.AnyArg(), "Asia/Jakarta", sqlmock.AnyArg(), "").
		WillReturnRows(sqlmock.NewRows([]string{"id", "inserted"}).AddRow("rreq_01TEST123456789012345678901234", true))

	err := s.triggerDailyReportForDate(time.Date(2026, 8, 16, 7, 0, 0, 0, time.UTC))
	require.NoError(t, err)
}

func TestShouldFireDaily(t *testing.T) {
	const (
		tHour = 7
		tMin  = 0
	)

	tests := []struct {
		name string
		h, m int
		want bool
	}{
		{"before target", 6, 59, false},
		{"exact target minute", 7, 0, true},
		{"after target minute same hour", 7, 30, true},
		{"same hour late minute", 7, 59, true},
		{"catch-up: after target hour", 17, 23, true}, // pod restarted after 07:00
		{"catch-up: next hour", 8, 0, true},
		{"midnight boundary before target", 0, 0, false},
		{"late hour late minute", 23, 59, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := shouldFireDaily(tt.h, tt.m, tHour, tMin); got != tt.want {
				t.Errorf("shouldFireDaily(%d:%d, target %d:%d) = %v, want %v", tt.h, tt.m, tHour, tMin, got, tt.want)
			}
		})
	}
}

func TestDailyReportBackoffAndRetry(t *testing.T) {

	// Reference start of day (first allowed attempt) at 07:00 UTC
	base := time.Date(2026, 8, 16, 7, 0, 0, 0, time.UTC)

	cases := []struct {
		name              string
		nowOffset         time.Duration // how far after base we are checking
		lastAttemptOffset time.Duration // offset of the last attempt relative to base (0 => same as base)
		wantAllowed       bool
	}{
		{"first attempt allowed", 0, 0, true},
		{"5min later, not allowed", 5 * time.Minute, 0, false},
		{"10min later, allowed", 10 * time.Minute, 0, true},
		{"30min later, allowed (still fast window)", 30 * time.Minute, 10 * time.Minute, true},
		{"59min later, allowed (still fast)", 59 * time.Minute, 49 * time.Minute, true},
		{"60min later, allowed (edge fast)", 60 * time.Minute, 50 * time.Minute, true},
		{"90min later, not allowed (slow backoff not elapsed)", 90 * time.Minute, 60 * time.Minute, false},
		{"120min later, allowed (slow backoff elapsed)", 120 * time.Minute, 60 * time.Minute, true},
	}

	for _, tt := range cases {
		tt := tt // capture range variable
		t.Run(tt.name, func(t *testing.T) {
			now := base.Add(tt.nowOffset)
			firstAttempt := base // first attempt is always at base for this day

			var lastAttempt time.Time
			if tt.lastAttemptOffset == 0 && tt.nowOffset == 0 {
				// Simulate no prior attempt (zero value)
				lastAttempt = time.Time{}
			} else {
				// last attempt occurred at base + offset
				lastAttempt = base.Add(tt.lastAttemptOffset)
			}

			var allowed bool
			if lastAttempt.IsZero() {
				allowed = true // first attempt
			} else {
				allowed = dailyReportRetryDue(now, firstAttempt, lastAttempt)
			}

			if allowed != tt.wantAllowed {
				t.Fatalf("allowed=%v, want %v (now=%v, lastAttempt=%v)", allowed, tt.wantAllowed, now, lastAttempt)
			}
		})
	}
}

// TestResolvedIncidentStaysReportEligible is the regression test for reports
// that were never generated for short incidents.
//
// Resolution frequently beats the report request to the scheduler: an incident
// that fires and clears between two ticks was never `open` when eligibility was
// evaluated, so it was dropped entirely — the investigator ran, its evidence was
// stored, and no report ever reached the user. A recovered incident still needs
// its investigation report.
func TestResolvedIncidentStaysReportEligible(t *testing.T) {
	s, mock := newMockScheduler(t, SchedulerConfig{HighSeverityThreshold: "critical"})

	mock.ExpectQuery("SELECT id FROM incidents").
		WithArgs(5).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).
			AddRow("inc_01RESOLVED111111111111111111"))

	eligible, err := s.getEligibleHighSeverityIncidents(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	require.Equal(t, []string{"inc_01RESOLVED111111111111111111"}, eligible)
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestReportEligibilityIncludesRecentlyResolved pins the predicate itself. It
// must admit resolved incidents only for a bounded window, so a deploy does not
// resurrect every historical incident.
func TestReportEligibilityIncludesRecentlyResolved(t *testing.T) {
	filter := reportEligibleStatusFilter

	for _, want := range []string{
		"status IN ('open', 'investigating')",
		"status = 'resolved'",
		"resolved_at >= now() - interval '1 hour'",
	} {
		if !strings.Contains(filter, want) {
			t.Errorf("eligibility filter missing %q, got:\n%s", want, filter)
		}
	}
}

// TestDailyReportCoversTheDayThatJustEnded pins the window. The job fires at
// 07:00, so reporting "today" produced a period of today 00:00 → tomorrow
// 00:00: 17 hours in the future at generation time, a card titled with
// tomorrow's date, and a 07:00–24:00 slice that no report ever covered.
func TestDailyReportCoversTheDayThatJustEnded(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	s, mock := newMockScheduler(t, SchedulerConfig{
		ReporterURL:       server.URL,
		DefaultTimezone:   "Asia/Jakarta",
		DailyReportTime:   "07:00",
		EnableDailyReport: true,
	})

	// 07:00 on 16 Aug 2026 in Jakarta.
	now := time.Date(2026, 8, 16, 0, 0, 0, 0, time.UTC)
	want, err := ComputeDailyPeriod("2026-08-15", "Asia/Jakarta")
	require.NoError(t, err)

	mock.ExpectQuery("INSERT INTO report_requests").
		WithArgs(
			sqlmock.AnyArg(),
			"daily",
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			"Asia/Jakarta",
			sqlmock.AnyArg(),
			"",
		).
		WillReturnRows(sqlmock.NewRows([]string{"id", "inserted"}).
			AddRow("rreq_01TEST123456789012345678901234", true))

	require.NoError(t, s.checkAndTriggerDaily(now))

	// The bound period must be the previous local calendar day, which in UTC
	// starts at 17:00 on the day before that.
	require.Equal(t, want.PeriodStart, time.Date(2026, 8, 14, 17, 0, 0, 0, time.UTC))
	require.Equal(t, want.PeriodEnd, time.Date(2026, 8, 15, 17, 0, 0, 0, time.UTC))
	require.Equal(t, 24*time.Hour, want.PeriodEnd.Sub(want.PeriodStart))

	require.NoError(t, mock.ExpectationsWereMet())
}

// TestDispatchPendingDailyRequestsSendsUndispatchedRequests pins the gap that
// made the manual daily endpoint a no-op: the request row was created and
// nothing ever sent it to the reporter, so the report was never generated.
func TestDispatchPendingDailyRequestsSendsUndispatchedRequests(t *testing.T) {
	received := make(chan ReportPayload, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload ReportPayload
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Errorf("decode error: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		received <- payload
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	s, mock := newMockScheduler(t, SchedulerConfig{ReporterURL: server.URL, DefaultTimezone: "Asia/Jakarta"})

	start := time.Date(2026, 11, 15, 17, 0, 0, 0, time.UTC)
	end := time.Date(2026, 11, 16, 17, 0, 0, 0, time.UTC)
	mock.ExpectQuery("SELECT rr.id, rr.period_start, rr.period_end, rr.timezone").
		WillReturnRows(sqlmock.NewRows([]string{"id", "period_start", "period_end", "timezone"}).
			AddRow("rreq_01TEST123456789012345678901234", start, end, "Asia/Jakarta"))

	require.NoError(t, s.dispatchPendingDailyRequests())

	select {
	case payload := <-received:
		require.Equal(t, "rreq_01TEST123456789012345678901234", payload.RequestID)
		require.Equal(t, "daily", payload.ReportType)
		require.Equal(t, 1, payload.ContractVersion)
		require.Equal(t, "2026-11-15T17:00:00Z", payload.PeriodStart)
		require.Equal(t, "2026-11-16T17:00:00Z", payload.PeriodEnd)
		require.Equal(t, "Asia/Jakarta", payload.Timezone)
		require.Empty(t, payload.IncidentIDs, "reporter selects daily incidents itself")
	case <-time.After(2 * time.Second):
		t.Fatal("pending daily request was not dispatched")
	}
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestDispatchPendingDailyRequestsIsIdempotent verifies the sweep only sees
// requests that are still pending. The reporter flips a request to `generating`
// on claim, so an in-flight request must not be dispatched again.
func TestDispatchPendingDailyRequestsIsIdempotent(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	s, mock := newMockScheduler(t, SchedulerConfig{ReporterURL: server.URL, DefaultTimezone: "UTC"})
	mock.ExpectQuery("SELECT rr.id, rr.period_start, rr.period_end, rr.timezone").
		WillReturnRows(sqlmock.NewRows([]string{"id", "period_start", "period_end", "timezone"}))

	require.NoError(t, s.dispatchPendingDailyRequests())
	time.Sleep(100 * time.Millisecond)
	require.Zero(t, calls, "nothing pending means nothing dispatched")
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestHighSeverityHashIsPerIncidentNotPerWindow pins the duplicate that reached
// Discord: one critical incident produced three identical cards because the
// idempotency key included the sliding ten-minute window. Every window the
// incident spanned was a new key, so every window was a new report.
func TestHighSeverityHashIsPerIncidentNotPerWindow(t *testing.T) {
	a := GenerateIncidentReportHash("inc_01TEST111111111111111111111111", ReportPolicyVersion)
	b := GenerateIncidentReportHash("inc_01TEST111111111111111111111111", ReportPolicyVersion)
	other := GenerateIncidentReportHash("inc_01TEST222222222222222222222222", ReportPolicyVersion)

	require.Equal(t, a, b, "the same incident must hash to the same key at any time")
	require.NotEqual(t, a, other, "different incidents must not share a key")

	// A new policy version changes the report's content, so it must produce a
	// new report rather than being swallowed by the old key.
	require.NotEqual(t, a, GenerateIncidentReportHash("inc_01TEST111111111111111111111111", ReportPolicyVersion+1))
}

func TestCalculateStaleReverifyBackoff(t *testing.T) {
	s := &Scheduler{
		config: SchedulerConfig{
			StaleReverifyBaseBackoff: time.Hour,
			StaleReverifyMaxBackoff:  24 * time.Hour,
		},
	}

	require.Equal(t, 1*time.Hour, s.calculateStaleReverifyBackoff(0))
	require.Equal(t, 2*time.Hour, s.calculateStaleReverifyBackoff(1))
	require.Equal(t, 4*time.Hour, s.calculateStaleReverifyBackoff(2))
	require.Equal(t, 8*time.Hour, s.calculateStaleReverifyBackoff(3))
	require.Equal(t, 16*time.Hour, s.calculateStaleReverifyBackoff(4))
	require.Equal(t, 24*time.Hour, s.calculateStaleReverifyBackoff(5))
	require.Equal(t, 24*time.Hour, s.calculateStaleReverifyBackoff(10))
}

func TestCheckAndEnqueueStaleIncidentVerifications(t *testing.T) {
	s, mock := newMockScheduler(t, SchedulerConfig{
		EnableStaleVerification: true,
		IncidentStaleAfter:      6 * time.Hour,
	})

	now := time.Now()
	cutoff := now.Add(-6 * time.Hour)

	mock.ExpectQuery("SELECT i.id.*FROM incidents i.*LEFT JOIN stale_verifications sv").
		WithArgs(cutoff, now).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("inc_1"))

	mock.ExpectExec("INSERT INTO stale_verifications").
		WithArgs("inc_1").
		WillReturnResult(sqlmock.NewResult(1, 1))

	err := s.checkAndEnqueueStaleIncidentVerifications(now)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestParseAffectedServices(t *testing.T) {
	// PostgreSQL array format
	got, err := parseAffectedServices([]byte(`{"api","worker"}`))
	require.NoError(t, err)
	require.Equal(t, []string{"api", "worker"}, got)

	// JSON array format
	got, err = parseAffectedServices([]byte(`["api", "worker"]`))
	require.NoError(t, err)
	require.Equal(t, []string{"api", "worker"}, got)

	// Empty
	got, err = parseAffectedServices([]byte(``))
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestProcessCompletedStaleIncidentVerifications_Recovered(t *testing.T) {
	s, mock := newMockScheduler(t, SchedulerConfig{
		EnableStaleVerification: true,
	})

	now := time.Now()
	verifiedAt := now.Add(-5 * time.Minute)
	lastEventAt := verifiedAt.Add(-10 * time.Minute)

	mock.ExpectQuery("SELECT sv.incident_id, COALESCE.*FROM stale_verifications sv").
		WillReturnRows(sqlmock.NewRows([]string{
			"incident_id", "verdict", "reason", "attempts", "verified_at",
			"title", "severity", "environment", "affected_services", "last_event_at",
		}).AddRow("inc_1", "recovered", "Target up=1", 0, verifiedAt, "Down", "critical", "prod", []byte(`["api"]`), lastEventAt))

	mock.ExpectExec("UPDATE incidents.*SET status = 'resolved'").
		WithArgs(verifiedAt, "inc_1").
		WillReturnResult(sqlmock.NewResult(0, 1))

	mock.ExpectExec("UPDATE stale_verifications SET reported_at = now").
		WithArgs("inc_1").
		WillReturnResult(sqlmock.NewResult(0, 1))

	err := s.processCompletedStaleIncidentVerifications(now)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestProcessCompletedStaleIncidentVerifications_InconclusiveStaysOpen(t *testing.T) {
	s, mock := newMockScheduler(t, SchedulerConfig{
		EnableStaleVerification:  true,
		StaleReverifyBaseBackoff: time.Hour,
		StaleReverifyMaxBackoff:  24 * time.Hour,
	})

	now := time.Now()
	verifiedAt := now.Add(-5 * time.Minute)
	lastEventAt := verifiedAt.Add(-10 * time.Minute)

	mock.ExpectQuery("SELECT sv.incident_id, COALESCE.*FROM stale_verifications sv").
		WillReturnRows(sqlmock.NewRows([]string{
			"incident_id", "verdict", "reason", "attempts", "verified_at",
			"title", "severity", "environment", "affected_services", "last_event_at",
		}).AddRow("inc_1", "inconclusive", "Authoritative source unavailable", 0, verifiedAt, "Down", "critical", "prod", []byte(`["api"]`), lastEventAt))

	mock.ExpectExec("UPDATE stale_verifications").
		WithArgs(now.Add(time.Hour), "inc_1").
		WillReturnResult(sqlmock.NewResult(0, 1))

	err := s.processCompletedStaleIncidentVerifications(now)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}
