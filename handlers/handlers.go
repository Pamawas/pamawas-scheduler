package handlers

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog/log"

	"github.com/Pamawas/pamawas-scheduler/config"
	"github.com/Pamawas/pamawas-scheduler/metrics"
	"github.com/Pamawas/pamawas-scheduler/models"
	"github.com/Pamawas/pamawas-scheduler/service"
)

// Handler holds dependencies for HTTP handlers
type Handler struct {
	scheduler *service.Scheduler
	cfg       config.Config
	metrics   *metrics.Metrics
	db        *sql.DB
}

// NewHandler creates a new handler with dependencies
func NewHandler(db *sql.DB, cfg config.Config, m *metrics.Metrics) *Handler {
	schedulerCfg := service.SchedulerConfig{
		ReporterURL:             cfg.ReporterURL,
		DailyReportTime:         cfg.DailyReportTime,
		HighSeverityThreshold:   cfg.HighSeverityThreshold,
		CheckInterval:           cfg.CheckInterval,
		EnableDailyReport:       cfg.EnableDailyReport,
		EnableHighSeverityAlert: cfg.EnableHighSeverityAlert,
	}

	scheduler := service.NewScheduler(db, schedulerCfg, m)

	return &Handler{
		scheduler: scheduler,
		cfg:       cfg,
		metrics:   m,
		db:        db,
	}
}

// HealthHandler handles health check requests
func (h *Handler) HealthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if err := h.db.PingContext(r.Context()); err != nil {
		h.metrics.DBConnectionErrors.Inc()
		log.Error().Err(err).Msg("Health check failed: database connection")
		w.WriteHeader(http.StatusServiceUnavailable)
		if encodeErr := json.NewEncoder(w).Encode(models.HealthResponse{
			Status: "unhealthy",
			Error:  fmt.Sprintf("Database connection failed: %v", err),
		}); encodeErr != nil {
			log.Error().Err(encodeErr).Msg("Failed to encode health response")
		}
		return
	}

	h.scheduler.MuLock()
	lastDailyReport := h.scheduler.LastDailyReport()
	lastHighSeverityCheck := h.scheduler.LastHighSeverityCheck()
	running := h.scheduler.Running()
	h.scheduler.MuUnlock()

	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(models.HealthResponse{
		Status:                "healthy",
		Timestamp:             time.Now().UTC(),
		LastDailyReport:       lastDailyReport,
		LastHighSeverityCheck: lastHighSeverityCheck,
		Running:               running,
		Version:               "1.0.0",
	}); err != nil {
		log.Error().Err(err).Msg("Failed to encode health response")
	}
}

// ReadyHandler handles readiness check requests
func (h *Handler) ReadyHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if err := h.db.PingContext(r.Context()); err != nil {
		log.Error().Err(err).Msg("Readiness check failed: database not ready")
		w.WriteHeader(http.StatusServiceUnavailable)
		if encodeErr := json.NewEncoder(w).Encode(models.ReadyResponse{
			Status: "not ready",
			Error:  fmt.Sprintf("Database not ready: %v", err),
		}); encodeErr != nil {
			log.Error().Err(encodeErr).Msg("Failed to encode ready response")
		}
		return
	}

	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(models.ReadyResponse{Status: "ready"}); err != nil {
		log.Error().Err(err).Msg("Failed to encode ready response")
	}
}

// CreateDailyReportRequest handles POST /v1/report-requests/daily
func (h *Handler) CreateDailyReportRequest(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req models.DailyReportRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON body", http.StatusBadRequest)
		return
	}

	if req.ReportDate == "" {
		http.Error(w, "report_date is required", http.StatusBadRequest)
		return
	}
	if req.Timezone == "" {
		http.Error(w, "timezone is required", http.StatusBadRequest)
		return
	}

	resp, err := h.scheduler.CreateDailyReportRequest(r.Context(), req.ReportDate, req.Timezone)
	if err != nil {
		log.Error().Err(err).Msg("Failed to create daily report request")
		http.Error(w, fmt.Sprintf("Failed to create daily report request: %v", err), http.StatusInternalServerError)
		return
	}

	status := http.StatusAccepted
	if resp.Duplicate {
		status = http.StatusOK
	}
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		log.Error().Err(err).Msg("Failed to encode daily report request response")
	}
}

// CreateHighSeverityReportRequest handles POST /v1/report-requests/high-severity
func (h *Handler) CreateHighSeverityReportRequest(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req models.HighSeverityReportRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON body", http.StatusBadRequest)
		return
	}

	resp, err := h.scheduler.CreateHighSeverityReportRequest(r.Context(), req.IncidentIDs, req.Evaluate)
	if err != nil {
		log.Error().Err(err).Msg("Failed to create high severity report request")
		http.Error(w, fmt.Sprintf("Failed to create high severity report request: %v", err), http.StatusInternalServerError)
		return
	}

	status := http.StatusAccepted
	if resp.Duplicate {
		status = http.StatusOK
	}
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		log.Error().Err(err).Msg("Failed to encode high severity report request response")
	}
}

// StatusHandler returns the current status of the scheduler
func (h *Handler) StatusHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	h.scheduler.MuLock()
	defer h.scheduler.MuUnlock()

	if err := json.NewEncoder(w).Encode(models.StatusResponse{
		LastDailyReport:       h.scheduler.LastDailyReport(),
		LastHighSeverityCheck: h.scheduler.LastHighSeverityCheck(),
		Running:               h.scheduler.Running(),
		Uptime:                time.Since(h.scheduler.StartTime()).String(),
		Version:               "1.0.0",
	}); err != nil {
		log.Error().Err(err).Msg("Failed to encode status response")
	}
}

// MetricsHandler returns the Prometheus metrics handler
func (h *Handler) MetricsHandler() http.Handler {
	return promhttp.Handler()
}

// Scheduler returns the scheduler for access in main.go
func (h *Handler) Scheduler() *service.Scheduler {
	return h.scheduler
}