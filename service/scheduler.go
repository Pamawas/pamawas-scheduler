package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/Pamawas/pamawas-scheduler/metrics"
	"github.com/Pamawas/pamawas-scheduler/models"
)

// SchedulerConfig holds the configuration for the scheduler
type SchedulerConfig struct {
	ReporterURL             string
	DailyReportTime         string
	HighSeverityThreshold   string
	CheckInterval           time.Duration
	EnableDailyReport       bool
	EnableHighSeverityAlert bool
	DefaultTimezone         string
}

// ReporterClient is a client for triggering reports via the reporter service
type ReporterClient struct {
	baseURL    string
	httpClient *http.Client
	ctx        context.Context
}

func NewReporterClient(baseURL string, ctx context.Context) *ReporterClient {
	return &ReporterClient{
		baseURL:    baseURL,
		httpClient: &http.Client{Timeout: 10 * time.Second},
		ctx:        ctx,
	}
}

// ReportPayload represents the typed request payload sent to reporter
type ReportPayload struct {
	ContractVersion int      `json:"contract_version"`
	RequestID       string   `json:"request_id"`
	ReportType      string   `json:"report_type"`
	PeriodStart     string   `json:"period_start"`
	PeriodEnd       string   `json:"period_end"`
	Timezone        string   `json:"timezone"`
	IncidentIDs     []string `json:"incident_ids"`
}

func (c *ReporterClient) SendReportRequest(payload ReportPayload) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}

	req, err := http.NewRequestWithContext(c.ctx, http.MethodPost, c.baseURL+"/v1/reports", strings.NewReader(string(body)))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			log.Error().Err(closeErr).Msg("Failed to close response body")
		}
	}()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("reporter returned status %d", resp.StatusCode)
	}
	return nil
}

// Scheduler holds the database connection and scheduling logic
type Scheduler struct {
	db                    *sql.DB
	config                SchedulerConfig
	mu                    sync.Mutex
	lastDailyReport       time.Time
	lastHighSeverityCheck time.Time
	running               bool
	wg                    sync.WaitGroup
	ctx                   context.Context
	cancelFunc            context.CancelFunc
	startTime             time.Time
	reporter              *ReporterClient
	metrics               *metrics.Metrics
}

// NewScheduler creates a new scheduler instance
func NewScheduler(db *sql.DB, cfg SchedulerConfig, m *metrics.Metrics) *Scheduler {
	if cfg.DefaultTimezone == "" {
		cfg.DefaultTimezone = "Asia/Jakarta"
	}
	ctx, cancel := context.WithCancel(context.Background())
	reporter := NewReporterClient(cfg.ReporterURL, ctx)
	return &Scheduler{
		db:         db,
		config:     cfg,
		ctx:        ctx,
		cancelFunc: cancel,
		startTime:  time.Now(),
		reporter:   reporter,
		metrics:    m,
	}
}

// StartWorker starts the background scheduler worker
func (s *Scheduler) StartWorker() {
	ticker := time.NewTicker(s.config.CheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			log.Info().Msg("Scheduler worker stopped")
			return
		case <-ticker.C:
			if err := s.checkAndTrigger(); err != nil {
				log.Error().Err(err).Msg("Scheduler check error")
			}
		}
	}
}

// checkAndTrigger checks if it's time to trigger reports and does so if needed
func (s *Scheduler) checkAndTrigger() error {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return nil // already running
	}
	s.running = true
	s.metrics.SchedulerRunning.Set(1)
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		s.running = false
		s.metrics.SchedulerRunning.Set(0)
		s.mu.Unlock()
	}()

	s.wg.Add(1)
	defer s.wg.Done()

	now := time.Now()

	// Check for daily report
	if s.config.EnableDailyReport {
		currentTime := now.Format("15:04") // HH:MM
		if currentTime == s.config.DailyReportTime {
			// Check if we already sent today's report
			today := now.Format("2006-01-02")
			if s.lastDailyReport.Format("2006-01-02") != today {
				log.Info().Str("time", s.config.DailyReportTime).Msg("Time for daily report")
				if err := s.triggerDailyReportForDate(now); err != nil {
					log.Error().Err(err).Msg("Failed to trigger daily report")
				} else {
					s.mu.Lock()
					s.lastDailyReport = now
					s.metrics.LastDailyReport.Set(float64(now.Unix()))
					s.metrics.DailyReportsTotal.Inc()
					s.mu.Unlock()
				}
			}
		}
	}

	// Check for high severity incidents
	if s.config.EnableHighSeverityAlert {
		if err := s.checkHighSeverityIncidents(); err != nil {
			log.Error().Err(err).Msg("Failed to check high severity incidents")
		} else {
			s.mu.Lock()
			s.lastHighSeverityCheck = now
			s.metrics.LastHighSeverityCheck.Set(float64(now.Unix()))
			s.metrics.HighSeverityChecksTotal.Inc()
			s.mu.Unlock()
		}
	}

	return nil
}

// triggerDailyReportForDate triggers a daily report for a specific date
func (s *Scheduler) triggerDailyReportForDate(reportDate time.Time) error {
	dateStr := reportDate.Format("2006-01-02")
	resp, err := s.CreateDailyReportRequest(s.ctx, dateStr, s.config.DefaultTimezone)
	if err != nil {
		return err
	}

	if !resp.Duplicate && resp.RequestID != "" {
		// Send to reporter with the created request
		payload := ReportPayload{
			ContractVersion: 1,
			RequestID:       resp.RequestID,
			ReportType:      "daily",
			PeriodStart:     resp.PeriodStart.Format(time.RFC3339),
			PeriodEnd:       resp.PeriodEnd.Format(time.RFC3339),
			Timezone:        resp.Timezone,
			IncidentIDs:     []string{}, // Reporter selects incidents for daily
		}
		if err := s.reporter.SendReportRequest(payload); err != nil {
			log.Error().Err(err).Str("request_id", resp.RequestID).Msg("Failed to send daily report request to reporter")
			return err
		}
		log.Info().Str("request_id", resp.RequestID).Msg("Daily report request sent to reporter")
	}

	return nil
}

// CreateDailyReportRequest creates an idempotent daily report request
func (s *Scheduler) CreateDailyReportRequest(ctx context.Context, reportDate, timezone string) (models.ReportRequestResponse, error) {
	var resp models.ReportRequestResponse

	// Compute period boundaries
	boundaries, err := ComputeDailyPeriod(reportDate, timezone)
	if err != nil {
		return resp, err
	}

	// Generate idempotency hash
	idempotencyHash := GenerateIdempotencyHash("daily", boundaries.PeriodStart, boundaries.PeriodEnd, timezone, ReportPolicyVersion)

	// Try to insert the report request (idempotent)
	requestID, duplicate, err := s.insertReportRequest(ctx, "daily", boundaries.PeriodStart, boundaries.PeriodEnd, timezone, idempotencyHash)
	if err != nil {
		return resp, fmt.Errorf("failed to insert report request: %w", err)
	}

	resp.RequestID = requestID
	resp.RequestType = "daily"
	resp.PeriodStart = boundaries.PeriodStart
	resp.PeriodEnd = boundaries.PeriodEnd
	resp.Timezone = boundaries.Timezone
	resp.Status = "pending"
	resp.Duplicate = duplicate

	if !duplicate {
		resp.Status = "pending"
	} else {
		resp.Status = "exists"
	}

	return resp, nil
}

// CreateHighSeverityReportRequest creates a high-severity report request
func (s *Scheduler) CreateHighSeverityReportRequest(ctx context.Context, incidentIDs []string, evaluate bool) (models.ReportRequestResponse, error) {
	var resp models.ReportRequestResponse

	var eligibleIncidentIDs []string

	if evaluate || len(incidentIDs) == 0 {
		// Evaluate currently eligible high-severity incidents
		eligible, err := s.getEligibleHighSeverityIncidents(ctx)
		if err != nil {
			return resp, fmt.Errorf("failed to get eligible incidents: %w", err)
		}
		eligibleIncidentIDs = eligible
	} else {
		// Use explicit incident IDs, but filter for eligibility
		eligible, err := s.filterEligibleIncidents(ctx, incidentIDs)
		if err != nil {
			return resp, fmt.Errorf("failed to filter incidents: %w", err)
		}
		eligibleIncidentIDs = eligible
	}

	if len(eligibleIncidentIDs) == 0 {
		resp.RequestID = ""
		resp.RequestType = "high_severity"
		resp.EligibleCount = 0
		resp.IncidentIDs = []string{}
		resp.Status = "no_eligible_incidents"
		resp.Duplicate = false
		return resp, nil
	}

	// Create one request per incident (per policy version)
	var firstRequestID string
	var duplicate bool

	for _, incidentID := range eligibleIncidentIDs {
		// Use a fixed period for high-severity (10 min window ending now)
		boundaries := ComputeHighSeverityPeriod()
		idempotencyHash := GenerateIdempotencyHash("high_severity", boundaries.PeriodStart, boundaries.PeriodEnd, "UTC", ReportPolicyVersion)

		// Include incident ID in the hash for per-incident idempotency
		incidentSpecificHash := idempotencyHash + "|" + incidentID

		requestID, isDup, err := s.insertReportRequest(ctx, "high_severity", boundaries.PeriodStart, boundaries.PeriodEnd, "UTC", incidentSpecificHash)
		if err != nil {
			log.Error().Err(err).Str("incident_id", incidentID).Msg("Failed to insert high severity report request")
			continue
		}

		if firstRequestID == "" {
			firstRequestID = requestID
			duplicate = isDup
		}

		if !isDup {
			// Send to reporter for each new request
			payload := ReportPayload{
				ContractVersion: 1,
				RequestID:       requestID,
				ReportType:      "high_severity",
				PeriodStart:     boundaries.PeriodStart.Format(time.RFC3339),
				PeriodEnd:       boundaries.PeriodEnd.Format(time.RFC3339),
				Timezone:        "UTC",
				IncidentIDs:     []string{incidentID},
			}
			if err := s.reporter.SendReportRequest(payload); err != nil {
				log.Error().Err(err).Str("request_id", requestID).Msg("Failed to send high severity report request to reporter")
			} else {
				log.Info().Str("request_id", requestID).Str("incident_id", incidentID).Msg("High severity report request sent to reporter")
			}
		}
	}

	resp.RequestID = firstRequestID
	resp.RequestType = "high_severity"
	resp.EligibleCount = len(eligibleIncidentIDs)
	resp.IncidentIDs = eligibleIncidentIDs
	resp.Duplicate = duplicate
	if duplicate {
		resp.Status = "exists"
	} else {
		resp.Status = "pending"
	}

	return resp, nil
}

// insertReportRequest inserts a report request with idempotency
func (s *Scheduler) insertReportRequest(ctx context.Context, requestType string, periodStart, periodEnd time.Time, timezone, idempotencyHash string) (string, bool, error) {
	// Generate a new ULID for the request ID
	requestID := generateULID("rreq")

	query := `
		INSERT INTO report_requests (id, request_type, period_start, period_end, timezone, idempotency_hash, status, attempts, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, 'pending', 0, now(), now())
		ON CONFLICT (idempotency_hash) DO UPDATE SET
			updated_at = now()
		RETURNING id, (xmax = 0) as inserted
	`

	var returnedID string
	var inserted bool
	err := s.db.QueryRowContext(ctx, query, requestID, requestType, periodStart, periodEnd, timezone, idempotencyHash).Scan(&returnedID, &inserted)
	if err != nil {
		return "", false, err
	}

	return returnedID, !inserted, nil
}

// checkHighSeverityIncidents checks for high severity incidents that need immediate attention
func (s *Scheduler) checkHighSeverityIncidents() error {
	threshold := strings.ToLower(s.config.HighSeverityThreshold)

	query := `
		SELECT COUNT(*)
		FROM incidents
		WHERE status IN ('open', 'investigating')
		AND (
			(LOWER(severity) = 'critical' AND $1 IN ('critical')) OR
			(LOWER(severity) = 'high' AND $1 IN ('high', 'critical')) OR
			(LOWER(severity) = 'warning' AND $1 IN ('warning', 'high', 'critical')) OR
			(LOWER(severity) = 'info' AND $1 IN ('info', 'warning', 'high', 'critical')) OR
			(LOWER(severity) = 'debug' AND $1 IN ('info', 'warning', 'high', 'critical', 'debug'))
		)
		AND NOT suppressed
		AND NOT EXISTS (
			SELECT 1 FROM report_requests rr
			WHERE rr.request_type = 'high_severity'
			AND rr.status IN ('pending', 'generating', 'generated')
			AND rr.idempotency_hash LIKE '%%' || incidents.id || '%%'
		)
	`

	// Only check incidents from the last hour to avoid repeated alerts
	since := time.Now().Add(-1 * time.Hour)

	var count int
	err := s.db.QueryRowContext(s.ctx, query, threshold, since).Scan(&count)
	if err != nil {
		return err
	}

	if count > 0 {
		log.Info().
			Int("count", count).
			Str("threshold", threshold).
			Msg("Found eligible high severity incidents - triggering alert")
		_, err := s.CreateHighSeverityReportRequest(s.ctx, []string{}, true)
		if err != nil {
			return err
		}
		s.metrics.HighSeverityAlertsTotal.Inc()
	}

	return nil
}

// getEligibleHighSeverityIncidents returns incident IDs that are eligible for high-severity reports
func (s *Scheduler) getEligibleHighSeverityIncidents(ctx context.Context) ([]string, error) {
	threshold := strings.ToLower(s.config.HighSeverityThreshold)

	query := `
		SELECT id
		FROM incidents
		WHERE status IN ('open', 'investigating')
		AND (
			(LOWER(severity) = 'critical' AND $1 IN ('critical')) OR
			(LOWER(severity) = 'high' AND $1 IN ('high', 'critical')) OR
			(LOWER(severity) = 'warning' AND $1 IN ('warning', 'high', 'critical')) OR
			(LOWER(severity) = 'info' AND $1 IN ('info', 'warning', 'high', 'critical')) OR
			(LOWER(severity) = 'debug' AND $1 IN ('info', 'warning', 'high', 'critical', 'debug'))
		)
		AND NOT suppressed
		AND NOT EXISTS (
			SELECT 1 FROM report_requests rr
			WHERE rr.request_type = 'high_severity'
			AND rr.status IN ('pending', 'generating', 'generated')
			AND rr.idempotency_hash LIKE '%%' || incidents.id || '%%'
		)
	`

	rows, err := s.db.QueryContext(ctx, query, threshold)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := rows.Close(); err != nil {
			log.Error().Err(err).Msg("Failed to close rows")
		}
	}()

	var incidentIDs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		incidentIDs = append(incidentIDs, id)
	}

	return incidentIDs, rows.Err()
}

// filterEligibleIncidents filters a list of incident IDs for eligibility
func (s *Scheduler) filterEligibleIncidents(ctx context.Context, incidentIDs []string) ([]string, error) {
	if len(incidentIDs) == 0 {
		return []string{}, nil
	}

	// Build placeholders for IN clause
	placeholders := make([]string, len(incidentIDs))
	args := make([]interface{}, len(incidentIDs)+1)
	threshold := strings.ToLower(s.config.HighSeverityThreshold)
	args[0] = threshold
	for i, id := range incidentIDs {
		placeholders[i] = fmt.Sprintf("$%d", i+2)
		args[i+1] = id
	}

	// Build query safely - placeholders are safe (just $N parameters), args are parameterized
	//nolint:gosec
	query := fmt.Sprintf(`
		SELECT id
		FROM incidents
		WHERE id IN (%s)
		AND status IN ('open', 'investigating')
		AND (
			(LOWER(severity) = 'critical' AND $1 IN ('critical')) OR
			(LOWER(severity) = 'high' AND $1 IN ('high', 'critical')) OR
			(LOWER(severity) = 'warning' AND $1 IN ('warning', 'high', 'critical')) OR
			(LOWER(severity) = 'info' AND $1 IN ('info', 'warning', 'high', 'critical')) OR
			(LOWER(severity) = 'debug' AND $1 IN ('info', 'warning', 'high', 'critical', 'debug'))
		)
		AND NOT suppressed
		AND NOT EXISTS (
			SELECT 1 FROM report_requests rr
			WHERE rr.request_type = 'high_severity'
			AND rr.status IN ('pending', 'generating', 'generated')
			AND rr.idempotency_hash LIKE '%%%%' || incidents.id || '%%%%'
		)
	`, strings.Join(placeholders, ","))

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := rows.Close(); err != nil {
			log.Error().Err(err).Msg("Failed to close rows")
		}
	}()

	var eligible []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		eligible = append(eligible, id)
	}

	return eligible, rows.Err()
}

// Stop stops the scheduler gracefully
func (s *Scheduler) Stop() {
	s.cancelFunc()
	s.wg.Wait()
}

// MuLock locks the scheduler mutex
func (s *Scheduler) MuLock() {
	s.mu.Lock()
}

// MuUnlock unlocks the scheduler mutex
func (s *Scheduler) MuUnlock() {
	s.mu.Unlock()
}

// LastDailyReport returns the last daily report time
func (s *Scheduler) LastDailyReport() time.Time {
	return s.lastDailyReport
}

// LastHighSeverityCheck returns the last high severity check time
func (s *Scheduler) LastHighSeverityCheck() time.Time {
	return s.lastHighSeverityCheck
}

// Running returns whether the scheduler is running
func (s *Scheduler) Running() bool {
	return s.running
}

// StartTime returns the start time
func (s *Scheduler) StartTime() time.Time {
	return s.startTime
}