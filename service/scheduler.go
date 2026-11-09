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

	"github.com/lib/pq"
	"github.com/rs/zerolog/log"

	"github.com/Pamawas/pamawas-scheduler/metrics"
	"github.com/Pamawas/pamawas-scheduler/models"
)

// Daily report retry backoff.
//
// The first attempt fires as soon as the trigger window opens (at 07:00, or
// immediately on boot if the pod starts after that). If it fails (reporter down,
// DB error, etc.) we retry instead of hammering the reporter on every check
// tick. Retries are spaced dailyReportFastBackoff apart for the first
// dailyReportFastWindow after the first attempt of the day, then every
// dailyReportSlowBackoff thereafter. Success is still gated to at most once per
// calendar day by lastDailyReport.
const (
	dailyReportFastBackoff = 10 * time.Minute
	dailyReportSlowBackoff = time.Hour
	dailyReportFastWindow  = time.Hour
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

	req, err := http.NewRequestWithContext(c.ctx, http.MethodPost, c.baseURL+"/api/v1/reports", strings.NewReader(string(body)))
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
	db                      *sql.DB
	config                  SchedulerConfig
	mu                      sync.Mutex
	lastDailyReport         time.Time
	lastDailyReportAttempt  time.Time
	dailyReportFirstAttempt time.Time
	lastHighSeverityCheck   time.Time
	running                 bool
	wg                      sync.WaitGroup
	ctx                     context.Context
	cancelFunc              context.CancelFunc
	startTime               time.Time
	reporter                *ReporterClient
	metrics                 *metrics.Metrics
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
	if err := s.checkAndTriggerDaily(now); err != nil {
		log.Error().Err(err).Msg("Failed to check daily report")
	}

	// Check for high severity incidents
	if err := s.checkAndTriggerHighSeverity(now); err != nil {
		log.Error().Err(err).Msg("Failed to check high severity incidents")
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

// severityRank returns the numeric rank for a severity string. Higher values
// are more severe. Unknown/empty severities map to rank 0 so they are never
// considered high severity.
func severityRank(severity string) int {
	switch strings.ToLower(severity) {
	case "debug":
		return 1
	case "info":
		return 2
	case "warning":
		return 3
	case "high":
		return 4
	case "critical":
		return 5
	default:
		return 0
	}
}

// checkHighSeverityIncidents checks for high severity incidents that need immediate attention
func (s *Scheduler) checkHighSeverityIncidents() error {
	thresholdRank := severityRank(s.config.HighSeverityThreshold)

	query := `
		SELECT COUNT(*)
		FROM incidents
		WHERE status IN ('open', 'investigating')
		AND (CASE LOWER(severity)
			WHEN 'debug' THEN 1
			WHEN 'info' THEN 2
			WHEN 'warning' THEN 3
			WHEN 'high' THEN 4
			WHEN 'critical' THEN 5
			ELSE 0
		END) >= $1
		AND status <> 'suppressed'
		AND NOT EXISTS (
			SELECT 1 FROM report_requests rr
			WHERE rr.request_type = 'high_severity'
			AND rr.status IN ('pending', 'generating', 'generated')
			AND rr.idempotency_hash LIKE '%' || incidents.id || '%'
		)
	`

	var count int
	err := s.db.QueryRowContext(s.ctx, query, thresholdRank).Scan(&count)
	if err != nil {
		return err
	}

	if count > 0 {
		log.Info().
			Int("count", count).
			Str("threshold", s.config.HighSeverityThreshold).
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
	thresholdRank := severityRank(s.config.HighSeverityThreshold)

	query := `
		SELECT id
		FROM incidents
		WHERE status IN ('open', 'investigating')
		AND (CASE LOWER(severity)
			WHEN 'debug' THEN 1
			WHEN 'info' THEN 2
			WHEN 'warning' THEN 3
			WHEN 'high' THEN 4
			WHEN 'critical' THEN 5
			ELSE 0
		END) >= $1
		AND status <> 'suppressed'
		AND NOT EXISTS (
			SELECT 1 FROM report_requests rr
			WHERE rr.request_type = 'high_severity'
			AND rr.status IN ('pending', 'generating', 'generated')
			AND rr.idempotency_hash LIKE '%' || incidents.id || '%'
		)
	`

	rows, err := s.db.QueryContext(ctx, query, thresholdRank)
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

	thresholdRank := severityRank(s.config.HighSeverityThreshold)

	query := `
		SELECT id
		FROM incidents
		WHERE id = ANY($1)
		AND status IN ('open', 'investigating')
		AND (CASE LOWER(severity)
			WHEN 'debug' THEN 1
			WHEN 'info' THEN 2
			WHEN 'warning' THEN 3
			WHEN 'high' THEN 4
			WHEN 'critical' THEN 5
			ELSE 0
		END) >= $2
		AND status <> 'suppressed'
		AND NOT EXISTS (
			SELECT 1 FROM report_requests rr
			WHERE rr.request_type = 'high_severity'
			AND rr.status IN ('pending', 'generating', 'generated')
			AND rr.idempotency_hash LIKE '%' || incidents.id || '%'
		)
	`

	rows, err := s.db.QueryContext(ctx, query, pq.Array(incidentIDs), thresholdRank)
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

func (s *Scheduler) checkAndTriggerHighSeverity(now time.Time) error {
	if !s.config.EnableHighSeverityAlert {
		return nil
	}

	if err := s.checkHighSeverityIncidents(); err != nil {
		return err
	}

	s.mu.Lock()
	s.lastHighSeverityCheck = now
	s.metrics.LastHighSeverityCheck.Set(float64(now.Unix()))
	s.metrics.HighSeverityChecksTotal.Inc()
	s.mu.Unlock()

	return nil
}

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

// shouldFireDaily reports whether the daily report should fire for the given
// local clock time relative to the configured target time.
//
// It returns true when the current time-of-day is at or past the target time,
// including catch-up for any hour strictly after the target hour. Combined with
// the day guard in checkAndTrigger, this fires the report at most once per day
// even when the process restarts after the scheduled time or first comes up
// late in the day.
func shouldFireDaily(currentHour, currentMin, targetHour, targetMin int) bool {
	return currentHour > targetHour || (currentHour == targetHour && currentMin >= targetMin)
}

// dailyReportBackoff returns the wait required before the next daily report
// retry, given when the first attempt of the day happened. Retries are spaced
// dailyReportFastBackoff apart for the first dailyReportFastWindow after the
// first attempt (so a transient failure near 07:00 is recovered within minutes),
// then dailyReportSlowBackoff thereafter to avoid hammering a still-down
// reporter for the rest of the day.
func dailyReportBackoff(now, firstAttempt time.Time) time.Duration {
	if now.Sub(firstAttempt) > dailyReportFastWindow {
		return dailyReportSlowBackoff
	}
	return dailyReportFastBackoff
}

// dailyReportRetryDue reports whether enough time has elapsed since the last
// attempt to retry the daily report, applying the stepped backoff of
// dailyReportBackoff. firstAttempt must be non-zero (it is guaranteed by the
// caller: the first attempt of a day bypasses this check entirely).
func dailyReportRetryDue(now, firstAttempt, lastAttempt time.Time) bool {
	return now.Sub(lastAttempt) >= dailyReportBackoff(now, firstAttempt)
}

// parseDailyReportTime parses a time string in "HH:MM" format and returns hour and minute.
func parseDailyReportTime(timeStr string) (hour, minute int, err error) {
	n, err := fmt.Sscanf(timeStr, "%d:%d", &hour, &minute)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to parse time %q: %w", timeStr, err)
	}
	if n != 2 {
		return 0, 0, fmt.Errorf("invalid time format %q, expected HH:MM", timeStr)
	}
	return hour, minute, nil
}

func (s *Scheduler) checkAndTriggerDaily(now time.Time) error {
	if !s.config.EnableDailyReport {
		return nil
	}

	// Parse the configured time and check if we're within a window
	targetHour, targetMin, err := parseDailyReportTime(s.config.DailyReportTime)
	if err != nil {
		log.Error().Err(err).Str("daily_report_time", s.config.DailyReportTime).Msg("Failed to parse daily_report_time")
		return err
	}

	// Compare against the configured timezone, not the machine's local
	// time. The pod may run in UTC while the report time is expressed in
	// the configured timezone (e.g. "07:00" Asia/Jakarta).
	loc, err := time.LoadLocation(s.config.DefaultTimezone)
	if err != nil {
		log.Error().Err(err).Str("timezone", s.config.DefaultTimezone).Msg("Failed to load timezone for daily report check")
		return err
	}

	localNow := now.In(loc)
	currentHour := localNow.Hour()
	currentMin := localNow.Minute()

	// Trigger once per day when the local time is at or past the
	// configured time. shouldFireDaily's "currentHour > targetHour"
	// arm is a catch-up so a pod that restarts after the scheduled
	// time (or first starts late in the day) still fires today's
	// report instead of missing it entirely until the next morning.
	shouldTrigger := shouldFireDaily(currentHour, currentMin, targetHour, targetMin)
	// Check if we already sent today's report
	today := localNow.Format("2006-01-02")

	s.mu.Lock()
	// Reset the per-day backoff bookkeeping when the calendar day
	// changes, so a fresh day's first attempt is not held back by a
	// stale backoff carried over from a prior day.
	if !s.dailyReportFirstAttempt.IsZero() &&
		s.dailyReportFirstAttempt.In(loc).Format("2006-01-02") != today {
		s.dailyReportFirstAttempt = time.Time{}
		s.lastDailyReportAttempt = time.Time{}
	}

	alreadySentToday := s.lastDailyReport.In(loc).Format("2006-01-02") == today

	// The report is due when the trigger window is open today and it has
	// not already succeeded, subject to the stepped retry backoff: the first
	// attempt of the day fires immediately, then retries wait
	// dailyReportFastBackoff for the first hour after that first attempt,
	// then dailyReportSlowBackoff thereafter.
	allowed := false
	if shouldTrigger && !alreadySentToday {
		if s.lastDailyReportAttempt.IsZero() {
			allowed = true // first attempt of the day fires immediately
		} else {
			allowed = dailyReportRetryDue(now, s.dailyReportFirstAttempt, s.lastDailyReportAttempt)
		}
	}
	s.mu.Unlock()

	if allowed {
		log.Info().Str("time", s.config.DailyReportTime).Msg("Time for daily report")
		if err := s.triggerDailyReportForDate(localNow); err != nil {
			log.Error().Err(err).Msg("Failed to trigger daily report")
			return err
		} else {
			s.mu.Lock()
			s.lastDailyReport = now
			s.metrics.LastDailyReport.Set(float64(now.Unix()))
			s.metrics.DailyReportsTotal.Inc()
			s.mu.Unlock()
		}

		// Record every attempt (success or failure) so the backoff is computed
		// from the actual last attempt, not the check tick.
		s.mu.Lock()
		if s.dailyReportFirstAttempt.IsZero() {
			s.dailyReportFirstAttempt = now
		}
		s.lastDailyReportAttempt = now
		s.mu.Unlock()
	}

	return nil
}

