package handlers

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/Pamawas/pamawas-scheduler/config"
	"github.com/Pamawas/pamawas-scheduler/metrics"
	"github.com/Pamawas/pamawas-scheduler/models"
)

func handlerMetrics() *metrics.Metrics {
	return &metrics.Metrics{
		DailyReportsTotal:       prometheus.NewCounter(prometheus.CounterOpts{Name: "handler_daily_total"}),
		HighSeverityChecksTotal: prometheus.NewCounter(prometheus.CounterOpts{Name: "handler_checks_total"}),
		HighSeverityAlertsTotal: prometheus.NewCounter(prometheus.CounterOpts{Name: "handler_alerts_total"}),
		DBConnectionErrors:      prometheus.NewCounter(prometheus.CounterOpts{Name: "handler_db_errors_total"}),
		LastDailyReport:         prometheus.NewGauge(prometheus.GaugeOpts{Name: "handler_last_daily"}),
		LastHighSeverityCheck:   prometheus.NewGauge(prometheus.GaugeOpts{Name: "handler_last_check"}),
		SchedulerRunning:        prometheus.NewGauge(prometheus.GaugeOpts{Name: "handler_running"}),
		SchedulerUptime:         prometheus.NewGauge(prometheus.GaugeOpts{Name: "handler_uptime"}),
	}
}

func newTestHandler(t *testing.T, cfg config.Config) (*Handler, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Logf("failed to close db: %v", err)
		}
	})
	return NewHandler(db, cfg, handlerMetrics()), mock
}

func TestNewHandlerExposesScheduler(t *testing.T) {
	h, _ := newTestHandler(t, config.Config{})
	if h.Scheduler() == nil {
		t.Fatal("nil scheduler")
	}
}

func TestHandlersRejectWrongMethods(t *testing.T) {
	h, _ := newTestHandler(t, config.Config{})
	cases := []struct {
		name   string
		fn     http.HandlerFunc
		method string
	}{
		{"health", h.HealthHandler, http.MethodPost},
		{"ready", h.ReadyHandler, http.MethodPost},
		{"status", h.StatusHandler, http.MethodPost},
		{"create_daily", h.CreateDailyReportRequest, http.MethodGet},
		{"create_high", h.CreateHighSeverityReportRequest, http.MethodGet},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rr := httptest.NewRecorder()
			tc.fn(rr, httptest.NewRequestWithContext(t.Context(), tc.method, "/", nil))
			if rr.Code != http.StatusMethodNotAllowed {
				t.Fatalf("status=%d", rr.Code)
			}
		})
	}
}

func TestHealthHandlerHealthy(t *testing.T) {
	h, mock := newTestHandler(t, config.Config{})
	mock.ExpectPing()
	rr := httptest.NewRecorder()
	h.HealthHandler(rr, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/healthz", nil))
	var response models.HealthResponse
	if err := json.NewDecoder(rr.Body).Decode(&response); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if rr.Code != http.StatusOK || response.Status != "healthy" || response.Version != "1.0.0" || response.Timestamp.IsZero() {
		t.Fatalf("response=%d %+v", rr.Code, response)
	}
}

func TestHealthHandlerUnhealthy(t *testing.T) {
	h, mock := newTestHandler(t, config.Config{})
	mock.ExpectPing().WillReturnError(errors.New("down"))
	rr := httptest.NewRecorder()
	h.HealthHandler(rr, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/healthz", nil))
	if rr.Code != http.StatusServiceUnavailable || testutil.ToFloat64(h.metrics.DBConnectionErrors) != 1 {
		t.Fatalf("status=%d metric=%v", rr.Code, testutil.ToFloat64(h.metrics.DBConnectionErrors))
	}
}

func TestReadyHandler(t *testing.T) {
	t.Run("ready", func(t *testing.T) {
		h, m := newTestHandler(t, config.Config{})
		m.ExpectPing()
		rr := httptest.NewRecorder()
		h.ReadyHandler(rr, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/ready", nil))
		if rr.Code != http.StatusOK || rr.Body.String() != "{\"status\":\"ready\"}\n" {
			t.Fatalf("response=%d %q", rr.Code, rr.Body.String())
		}
	})
	t.Run("not ready", func(t *testing.T) {
		h, m := newTestHandler(t, config.Config{})
		m.ExpectPing().WillReturnError(errors.New("down"))
		rr := httptest.NewRecorder()
		h.ReadyHandler(rr, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/ready", nil))
		if rr.Code != http.StatusServiceUnavailable {
			t.Fatalf("status=%d", rr.Code)
		}
	})
}

func TestCreateDailyReportRequest(t *testing.T) {
	h, mock := newTestHandler(t, config.Config{ReporterURL: "http://reporter", DefaultTimezone: "Asia/Jakarta"})

	// Mock the scheduler's CreateDailyReportRequest by using the actual scheduler
	// Since we're testing the handler, we need to mock the database for the scheduler
	mock.ExpectQuery("INSERT INTO report_requests").
		WithArgs(sqlmock.AnyArg(), "daily", sqlmock.AnyArg(), sqlmock.AnyArg(), "Asia/Jakarta", sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"id", "inserted"}).AddRow("rreq_01TEST123456789012345678901234", true))

	body := `{"report_date":"2026-08-16","timezone":"Asia/Jakarta"}`
	rr := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/v1/report-requests/daily", strings.NewReader(body))
	h.CreateDailyReportRequest(rr, req)

	require.Equal(t, http.StatusAccepted, rr.Code)
	var resp models.ReportRequestResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	require.Equal(t, "rreq_01TEST123456789012345678901234", resp.RequestID)
	require.Equal(t, "daily", resp.RequestType)
	require.Equal(t, "Asia/Jakarta", resp.Timezone)
	require.False(t, resp.Duplicate)
}

func TestCreateDailyReportRequest_MissingFields(t *testing.T) {
	h, _ := newTestHandler(t, config.Config{})

	// Missing report_date
	body := `{"timezone":"UTC"}`
	rr := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/v1/report-requests/daily", strings.NewReader(body))
	h.CreateDailyReportRequest(rr, req)
	require.Equal(t, http.StatusBadRequest, rr.Code)

	// Missing timezone
	body = `{"report_date":"2026-08-16"}`
	rr = httptest.NewRecorder()
	req = httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/v1/report-requests/daily", strings.NewReader(body))
	h.CreateDailyReportRequest(rr, req)
	require.Equal(t, http.StatusBadRequest, rr.Code)

	// Invalid JSON
	body = `{invalid`
	rr = httptest.NewRecorder()
	req = httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/v1/report-requests/daily", strings.NewReader(body))
	h.CreateDailyReportRequest(rr, req)
	require.Equal(t, http.StatusBadRequest, rr.Code)
}

func TestCreateDailyReportRequest_Duplicate(t *testing.T) {
	h, mock := newTestHandler(t, config.Config{ReporterURL: "http://reporter", DefaultTimezone: "Asia/Jakarta"})

	mock.ExpectQuery("INSERT INTO report_requests").
		WithArgs(sqlmock.AnyArg(), "daily", sqlmock.AnyArg(), sqlmock.AnyArg(), "Asia/Jakarta", sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"id", "inserted"}).AddRow("rreq_01TEST123456789012345678901234", false))

	body := `{"report_date":"2026-08-16","timezone":"Asia/Jakarta"}`
	rr := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/v1/report-requests/daily", strings.NewReader(body))
	h.CreateDailyReportRequest(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	var resp models.ReportRequestResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	require.True(t, resp.Duplicate)
	require.Equal(t, "exists", resp.Status)
}

func TestCreateHighSeverityReportRequest(t *testing.T) {
	h, mock := newTestHandler(t, config.Config{ReporterURL: "http://reporter", HighSeverityThreshold: "high"})

	// Filter eligible
	mock.ExpectQuery("SELECT id FROM incidents WHERE id IN").
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("inc_01TEST123456789012345678901234"))

	// Insert
	mock.ExpectQuery("INSERT INTO report_requests").
		WithArgs(sqlmock.AnyArg(), "high_severity", sqlmock.AnyArg(), sqlmock.AnyArg(), "UTC", sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"id", "inserted"}).AddRow("rreq_01TEST987654321098765432109876", true))

	body := `{"incident_ids":["inc_01TEST123456789012345678901234"],"evaluate":false}`
	rr := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/v1/report-requests/high-severity", strings.NewReader(body))
	h.CreateHighSeverityReportRequest(rr, req)

	require.Equal(t, http.StatusAccepted, rr.Code)
	var resp models.ReportRequestResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	require.Equal(t, 1, resp.EligibleCount)
	require.Contains(t, resp.IncidentIDs, "inc_01TEST123456789012345678901234")
	require.False(t, resp.Duplicate)
}

func TestCreateHighSeverityReportRequest_Evaluate(t *testing.T) {
	h, mock := newTestHandler(t, config.Config{ReporterURL: "http://reporter", HighSeverityThreshold: "high"})

	// Get eligible
	mock.ExpectQuery("SELECT id FROM incidents").
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("inc_01TEST111111111111111111111111"))

	// Insert for the eligible incident
	mock.ExpectQuery("INSERT INTO report_requests").
		WithArgs(sqlmock.AnyArg(), "high_severity", sqlmock.AnyArg(), sqlmock.AnyArg(), "UTC", sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"id", "inserted"}).AddRow("rreq_01TEST987654321098765432109876", true))

	body := `{"evaluate":true}`
	rr := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/v1/report-requests/high-severity", strings.NewReader(body))
	h.CreateHighSeverityReportRequest(rr, req)

	require.Equal(t, http.StatusAccepted, rr.Code)
	var resp models.ReportRequestResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	require.Equal(t, 1, resp.EligibleCount)
}

func TestCreateHighSeverityReportRequest_NoEligible(t *testing.T) {
	h, mock := newTestHandler(t, config.Config{HighSeverityThreshold: "high"})

	mock.ExpectQuery("SELECT id FROM incidents").
		WillReturnRows(sqlmock.NewRows([]string{"id"}))

	body := `{"evaluate":true}`
	rr := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/v1/report-requests/high-severity", strings.NewReader(body))
	h.CreateHighSeverityReportRequest(rr, req)

	require.Equal(t, http.StatusAccepted, rr.Code)
	var resp models.ReportRequestResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	require.Equal(t, 0, resp.EligibleCount)
	require.Equal(t, "no_eligible_incidents", resp.Status)
}

func TestCreateHighSeverityReportRequest_InvalidJSON(t *testing.T) {
	h, _ := newTestHandler(t, config.Config{})

	body := `{invalid`
	rr := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/v1/report-requests/high-severity", strings.NewReader(body))
	h.CreateHighSeverityReportRequest(rr, req)
	require.Equal(t, http.StatusBadRequest, rr.Code)
}

func TestStatusAndMetricsHandlers(t *testing.T) {
	h, _ := newTestHandler(t, config.Config{})
	rr := httptest.NewRecorder()
	h.StatusHandler(rr, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/status", nil))
	var response models.StatusResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&response))
	require.Equal(t, "1.0.0", response.Version)
	require.NotEmpty(t, response.Uptime)
	require.NotNil(t, h.MetricsHandler())
}
