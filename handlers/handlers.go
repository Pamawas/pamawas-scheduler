package handlers

import (
	"context"
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
		ReporterURL:            cfg.ReporterURL,
		DailyReportTime:        cfg.DailyReportTime,
		HighSeverityThreshold:  cfg.HighSeverityThreshold,
		CheckInterval:          cfg.CheckInterval,
		EnableDailyReport:      cfg.EnableDailyReport,
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
		json.NewEncoder(w).Encode(models.HealthResponse{
			Status: "unhealthy",
			Error:  fmt.Sprintf("Database connection failed: %v", err),
		})
		return
	}

	h.scheduler.MuLock()
	lastDailyReport := h.scheduler.LastDailyReport()
	lastHighSeverityCheck := h.scheduler.LastHighSeverityCheck()
	running := h.scheduler.Running()
	h.scheduler.MuUnlock()

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(models.HealthResponse{
		Status:                   "healthy",
		Timestamp:                time.Now().UTC(),
		LastDailyReport:          lastDailyReport,
		LastHighSeverityCheck:    lastHighSeverityCheck,
		Running:                  running,
		Version:                  "1.0.0",
	})
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
		json.NewEncoder(w).Encode(models.ReadyResponse{
			Status: "not ready",
			Error:  fmt.Sprintf("Database not ready: %v", err),
		})
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(models.ReadyResponse{Status: "ready"})
}

// TriggerDailyHandler handles manual daily report triggers
func (h *Handler) TriggerDailyHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if err := h.scheduler.TriggerDailyReport(); err != nil {
		http.Error(w, fmt.Sprintf("Failed to trigger daily report: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(models.TriggerResponse{
		Message: "Daily report triggered successfully",
	})
}

// TriggerHighSeverityHandler handles manual high severity alert triggers
func (h *Handler) TriggerHighSeverityHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if err := h.scheduler.TriggerHighSeverityAlert(); err != nil {
		http.Error(w, fmt.Sprintf("Failed to trigger high severity alert: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(models.TriggerResponse{
		Message: "High severity alert triggered successfully",
	})
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

	json.NewEncoder(w).Encode(models.StatusResponse{
		LastDailyReport:       h.scheduler.LastDailyReport(),
		LastHighSeverityCheck: h.scheduler.LastHighSeverityCheck(),
		Running:               h.scheduler.Running(),
		Uptime:                time.Since(h.scheduler.StartTime()).String(),
		Version:               "1.0.0",
	})
}

// MetricsHandler returns the Prometheus metrics handler
func (h *Handler) MetricsHandler() http.Handler {
	return promhttp.Handler()
}

// Reporter returns the scheduler for access in main.go
func (h *Handler) Scheduler() *service.Scheduler {
	return h.scheduler
}