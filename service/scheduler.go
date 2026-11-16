package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
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

// Daily report retry backoff. Retries are spaced dailyReportFastBackoff apart for the first
// dailyReportFastWindow after the first attempt of the day, then every dailyReportSlowBackoff thereafter.
// Success is gated to at most once per calendar day by lastDailyReport.
const (
	dailyReportFastBackoff = 10 * time.Minute
	dailyReportSlowBackoff = time.Hour
	dailyReportFastWindow  = time.Hour
	reporterRequestTimeout = 8 * time.Minute
)

// SchedulerConfig holds the configuration for the scheduler
type SchedulerConfig struct {
	ReporterURL             string
	ServiceToken            string
	DailyReportTime         string
	HighSeverityThreshold   string
	CheckInterval           time.Duration
	EnableDailyReport       bool
	EnableHighSeverityAlert bool
	DefaultTimezone         string
}

// ReporterClient is a client for triggering reports via the reporter service
type ReporterClient struct {
	baseURL string
	// serviceToken authenticates to the reporter, which rejects unauthenticated
	// callers on every /api/v1 route.
	serviceToken string
	httpClient   *http.Client
	ctx          context.Context
	wg           sync.WaitGroup
}

func NewReporterClient(baseURL, serviceToken string, ctx context.Context) *ReporterClient {
	return &ReporterClient{
		baseURL:      baseURL,
		serviceToken: serviceToken,
		httpClient:   &http.Client{Timeout: reporterRequestTimeout},
		ctx:          ctx,
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
	if c.serviceToken != "" {
		req.Header.Set("X-Service-Token", c.serviceToken)
	}

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

// SendReportRequestAsync dispatches the report request to the reporter without blocking the caller.
func (c *ReporterClient) SendReportRequestAsync(payload ReportPayload) {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		defer func() {
			if r := recover(); r != nil {
				log.Error().Interface("panic", r).Str("request_id", payload.RequestID).Msg("Recovered panic sending report request to reporter")
			}
		}()
		if err := c.SendReportRequest(payload); err != nil {
			log.Error().Err(err).Str("request_id", payload.RequestID).Str("report_type", payload.ReportType).Msg("Failed to send report request to reporter")
		}
	}()
}

// Close waits for any in-flight async report sends to complete. The caller
// should cancel the client context (via Scheduler.Stop) first so that requests
// still in-flight are aborted promptly instead of waiting out the full client
// timeout.
func (c *ReporterClient) Close() {
	c.wg.Wait()
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

	// Leader election via a PostgreSQL session-level advisory lock. A
	// dedicated connection holds the lock for the process lifetime; when it
	// is not the leader the tick loop stays idle so periodic work (daily
	// reports, high-severity sweeps, requeues) runs exactly once across
	// replicas. If the leader dies, its connection drops, Postgres releases
	// the lock, and a follower acquires it on its next retry.
	leaderConn *sql.Conn
	isLeader   bool
}

// NewScheduler creates a new scheduler instance. The config must carry a
// valid IANA default timezone — config.Load supplies the documented
// development default (Asia/Jakarta); there is no constructor fallback.
func NewScheduler(db *sql.DB, cfg SchedulerConfig, m *metrics.Metrics) *Scheduler {
	if cfg.DefaultTimezone == "" {
		panic("default_timezone is required (config must supply it)")
	}
	if _, err := time.LoadLocation(cfg.DefaultTimezone); err != nil {
		panic(fmt.Sprintf("invalid default_timezone %q: %v", cfg.DefaultTimezone, err))
	}
	ctx, cancel := context.WithCancel(context.Background())
	reporter := NewReporterClient(cfg.ReporterURL, cfg.ServiceToken, ctx)
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

// leaderLockID is the fixed advisory-lock key for scheduler leadership.
const leaderLockID = 7270001 // "PAMAWAS" themed constant, must be stable across replicas

// AcquireLeadership attempts to take the scheduler advisory lock on a
// dedicated pooled connection. Returns true when this instance is the leader.
// The connection must stay open for as long as leadership is held.
func (s *Scheduler) AcquireLeadership() bool {
	conn, err := s.db.Conn(s.ctx)
	if err != nil {
		log.Error().Err(err).Msg("Leader election: failed to obtain dedicated connection")
		return false
	}
	var locked bool
	if err := conn.QueryRowContext(s.ctx, "SELECT pg_try_advisory_lock($1)", leaderLockID).Scan(&locked); err != nil {
		log.Error().Err(err).Msg("Leader election: advisory lock query failed")
		if cerr := conn.Close(); cerr != nil {
			log.Error().Err(cerr).Msg("Leader election: failed to close dedicated connection after query error")
		}
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !locked {
		if cerr := conn.Close(); cerr != nil {
			log.Error().Err(cerr).Msg("Leader election: failed to close dedicated connection (lock not acquired)")
		}
		s.isLeader = false
		s.metrics.SchedulerLeader.Set(0)
		return false
	}
	s.leaderConn = conn
	s.isLeader = true
	s.metrics.SchedulerLeader.Set(1)
	log.Info().Msg("Leader election: acquired scheduler leadership")
	return true
}

// ReleaseLeadership relinquishes the advisory lock and closes the dedicated
// connection. Safe to call when not the leader.
func (s *Scheduler) ReleaseLeadership() {
	s.mu.Lock()
	conn := s.leaderConn
	s.leaderConn = nil
	s.isLeader = false
	s.mu.Unlock()
	s.metrics.SchedulerLeader.Set(0)
	if conn == nil {
		return
	}
	_, err := conn.ExecContext(context.Background(), "SELECT pg_advisory_unlock($1)", leaderLockID)
	if err != nil {
		log.Error().Err(err).Msg("Leader election: advisory unlock failed (connection close will release it)")
	}
	if cerr := conn.Close(); cerr != nil {
		log.Error().Err(cerr).Msg("Leader election: failed to close dedicated connection on release")
	}
}

// IsLeader reports whether this instance currently holds scheduler leadership.
func (s *Scheduler) IsLeader() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.isLeader
}

// StartWorker starts the background scheduler worker. Only the leader runs
// the tick loop; followers retry leadership every tick so a leader crash is
// recovered without manual intervention.
func (s *Scheduler) StartWorker() {
	ticker := time.NewTicker(s.config.CheckInterval)
	defer ticker.Stop()

	if !s.AcquireLeadership() {
		log.Info().Msg("Scheduler instance is a follower; tick loop idle until leadership frees up")
	}

	for {
		select {
		case <-s.ctx.Done():
			s.ReleaseLeadership()
			log.Info().Msg("Scheduler worker stopped")
			return
		case <-ticker.C:
			if !s.IsLeader() {
				// Leadership may have freed up (leader crashed/restarted).
				if !s.AcquireLeadership() {
					continue
				}
			}
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

	// Phase 2: Dispatch pending high-severity report requests whose investigations
	// have reached a terminal state. Runs every tick.
	if err := s.checkHighSeverityFollowUp(); err != nil {
		log.Error().Err(err).Msg("Failed to check high-severity follow-up")
	}

	// Phase 3: Requeue stale report requests (generating with expired lease,
	// failed_retryable past due, or pending older than 10 minutes) with no
	// associated report. This self-recovers stuck requests.
	if requeued, err := s.requeueStaleReportRequests(s.ctx); err != nil {
		log.Error().Err(err).Msg("Failed to requeue stale report requests")
	} else if requeued > 0 {
		log.Info().Int("count", requeued).Msg("Requeued stale report requests this tick")
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
func (s *Scheduler) CreateHighSeverityReportRequest(ctx context.Context, incidentIDs []string, evaluate bool, dispatch bool) (models.ReportRequestResponse, error) {
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
		log.Info().
			Str("threshold", s.config.HighSeverityThreshold).
			Msg("No eligible high severity incidents; report request not dispatched")
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

		if isDup {
			requeued, requeueErr := s.requeueStaleReportRequest(ctx, requestID)
			if requeueErr != nil {
				log.Error().Err(requeueErr).Str("request_id", requestID).Msg("Failed to requeue stale high severity report request")
				continue
			}
			if requeued {
				isDup = false
			}
		}

		if firstRequestID == "" {
			firstRequestID = requestID
			duplicate = isDup
		}

		if !isDup && dispatch {
			// Send to reporter for each new request. Dispatched asynchronously:
			// the report_requests row is already committed above, so a slow or
			// unreachable reporter must not block the scheduler tick loop (which
			// would also stall the daily report and later high-severity checks).
			payload := ReportPayload{
				ContractVersion: 1,
				RequestID:       requestID,
				ReportType:      "high_severity",
				PeriodStart:     boundaries.PeriodStart.Format(time.RFC3339),
				PeriodEnd:       boundaries.PeriodEnd.Format(time.RFC3339),
				Timezone:        "UTC",
				IncidentIDs:     []string{incidentID},
			}
			s.reporter.SendReportRequestAsync(payload)
			log.Info().Str("request_id", requestID).Str("incident_id", incidentID).Msg("High severity report request dispatched to reporter")
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
			updated_at = report_requests.updated_at
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

// requeueStaleReportRequests finds all stale report requests (generating with
// expired lease, failed_retryable past due, or pending older than 10 minutes)
// that have no associated report and resets them to pending so the reporter can
// pick them up on the next tick. This is a standalone sweep that runs every
// scheduler tick so stuck requests self-recover without waiting for an
// idempotency conflict that may never arrive.
func (s *Scheduler) requeueStaleReportRequests(ctx context.Context) (int, error) {
	const query = `
		UPDATE report_requests
		SET status = 'pending',
		    next_attempt_at = NULL,
		    lease_expires_at = NULL,
		    safe_error_code = NULL,
		    updated_at = now()
		WHERE (
		    (status = 'generating' AND (lease_expires_at IS NULL OR lease_expires_at < now()))
		    OR (status = 'failed_retryable' AND (next_attempt_at IS NULL OR next_attempt_at <= now()))
		    OR (status = 'pending' AND updated_at < now() - interval '10 minutes')
		)
		AND NOT EXISTS (
		    SELECT 1 FROM reports r WHERE r.request_id = report_requests.id
		)
		RETURNING id
	`

	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return 0, err
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			log.Error().Err(closeErr).Msg("Failed to close rows during stale report request requeue")
		}
	}()

	count := 0
	var requeuedID string
	for rows.Next() {
		if err := rows.Scan(&requeuedID); err != nil {
			return count, err
		}
		count++
		log.Info().Str("request_id", requeuedID).Msg("Requeued stale report request")
	}
	if err := rows.Err(); err != nil {
		return count, err
	}

	if count > 0 {
		log.Info().Int("count", count).Msg("Requeued stale report requests")
	}

	return count, nil
}

// requeueStaleReportRequest resets a single stale report request back to pending
// so the reporter can pick it up. It is called when an idempotency insert
// collides with an existing request that appears abandoned.
func (s *Scheduler) requeueStaleReportRequest(ctx context.Context, requestID string) (bool, error) {
	const query = `
		UPDATE report_requests
		SET status = 'pending',
		    next_attempt_at = NULL,
		    lease_expires_at = NULL,
		    safe_error_code = NULL,
		    updated_at = now()
		WHERE id = $1
		  AND (
		    (status = 'generating' AND (lease_expires_at IS NULL OR lease_expires_at < now()))
		    OR (status = 'failed_retryable' AND (next_attempt_at IS NULL OR next_attempt_at <= now()))
		    OR (status = 'pending' AND updated_at < now() - interval '10 minutes')
		  )
		  AND NOT EXISTS (
		    SELECT 1 FROM reports WHERE request_id = $1
		  )
		RETURNING id
	`

	var requeuedID string
	err := s.db.QueryRowContext(ctx, query, requestID).Scan(&requeuedID)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	log.Info().Str("request_id", requeuedID).Msg("Requeued stale report request")
	return true, nil
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
	eligible, err := s.getEligibleHighSeverityIncidents(s.ctx)
	if err != nil {
		return err
	}

	if len(eligible) > 0 {
		log.Info().
			Int("count", len(eligible)).
			Str("threshold", s.config.HighSeverityThreshold).
			Msg("Found eligible high severity incidents - dispatching report request to reporter")

		// Route high-severity alerts through the reporter (which has Discord/Telegram/email
		// configured) rather than sending directly from the scheduler. Create the report
		// request as pending WITHOUT dispatching it immediately: checkHighSeverityFollowUp
		// dispatches it to the reporter only after the investigation reaches a terminal
		// state, so the generated report includes the investigation's evidence instead of
		// "investigation unavailable".
		_, err := s.CreateHighSeverityReportRequest(s.ctx, eligible, true, false)
		if err != nil {
			return err
		}
		s.metrics.HighSeverityAlertsTotal.Inc()
	}

	return nil
}

// checkHighSeverityFollowUp checks pending high-severity report requests whose
// investigations have reached a terminal state and dispatches them to the
// reporter. This replaces the reporter's blocking wait/timeout with a
// scheduler-driven, non-blocking check that runs on each tick.
func (s *Scheduler) checkHighSeverityFollowUp() error {
	if !s.config.EnableHighSeverityAlert {
		return nil
	}

	query := `
		SELECT rr.id, rr.idempotency_hash
		FROM report_requests rr
		WHERE rr.request_type = 'high_severity'
		  AND rr.status = 'pending'
		  AND NOT EXISTS (SELECT 1 FROM reports r WHERE r.request_id = rr.id)
	`

	rows, err := s.db.QueryContext(s.ctx, query)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			log.Error().Err(closeErr).Msg("Failed to close rows")
		}
	}()

	type pendingRequest struct {
		requestID       string
		idempotencyHash string
	}
	var pending []pendingRequest
	for rows.Next() {
		var id, hash string
		if err := rows.Scan(&id, &hash); err != nil {
			return err
		}
		pending = append(pending, pendingRequest{requestID: id, idempotencyHash: hash})
	}
	if err := rows.Err(); err != nil {
		return err
	}

	for _, req := range pending {
		// Extract incident ID from the idempotency hash suffix ("baseHash|incidentID")
		incidentID := ""
		if idx := strings.LastIndex(req.idempotencyHash, "|"); idx >= 0 && idx+1 < len(req.idempotencyHash) {
			incidentID = req.idempotencyHash[idx+1:]
		}
		if incidentID == "" {
			log.Warn().Str("request_id", req.requestID).Msg("Cannot extract incident ID from idempotency hash; skipping follow-up")
			continue
		}

		// Check whether any investigation run for this incident has reached a
		// terminal state (completed, failed, or failed_terminal).
		var terminalCount int
		err := s.db.QueryRowContext(s.ctx, `
			SELECT COUNT(*) FROM investigation_runs
			WHERE incident_id = $1
			  AND status IN ('completed', 'failed', 'failed_terminal')
		`, incidentID).Scan(&terminalCount)
		if err != nil {
			log.Error().Err(err).Str("incident_id", incidentID).Msg("Failed to check investigation status during follow-up")
			continue
		}

		if terminalCount == 0 {
			// Investigation not yet complete; leave the request pending for the next tick.
			continue
		}

		// Investigation is terminal — dispatch the report request to the reporter.
		boundaries := ComputeHighSeverityPeriod()
		payload := ReportPayload{
			ContractVersion: 1,
			RequestID:       req.requestID,
			ReportType:      "high_severity",
			PeriodStart:     boundaries.PeriodStart.Format(time.RFC3339),
			PeriodEnd:       boundaries.PeriodEnd.Format(time.RFC3339),
			Timezone:        "UTC",
			IncidentIDs:     []string{incidentID},
		}
		s.reporter.SendReportRequestAsync(payload)
		log.Info().Str("request_id", req.requestID).Str("incident_id", incidentID).Msg("Dispatched high-severity report request after investigation completion")
	}

	return nil
}

// reportEligibleStatusFilter selects incidents that can still receive a
// high-severity follow-up report.
//
// A resolved incident is included, because resolution frequently beats the
// report request to the scheduler: an incident that fires and clears between
// two ticks was never `open` when eligibility was evaluated, and used to be
// dropped entirely — the investigator ran, its evidence was stored, and no
// report was ever generated. Bounded by resolved_at so this only backfills the
// recent gap and never resurrects old incidents.
const reportEligibleStatusFilter = `(
		status IN ('open', 'investigating')
		OR (status = 'resolved' AND resolved_at >= now() - interval '1 hour')
	)`

// getEligibleHighSeverityIncidents returns incident IDs that are eligible for high-severity reports
func (s *Scheduler) getEligibleHighSeverityIncidents(ctx context.Context) ([]string, error) {
	thresholdRank := severityRank(s.config.HighSeverityThreshold)

	query := `
		SELECT id
		FROM incidents
		WHERE ` + reportEligibleStatusFilter + `
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
			AND rr.idempotency_hash LIKE '%' || incidents.id || '%'
			AND (
				rr.status = 'generated'
				OR (rr.status = 'pending' AND rr.updated_at >= now() - interval '10 minutes')
				OR (rr.status = 'generating' AND rr.lease_expires_at >= now())
				OR (rr.status = 'failed_retryable' AND rr.next_attempt_at > now())
				OR EXISTS (SELECT 1 FROM reports r WHERE r.request_id = rr.id)
			)
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
		AND ` + reportEligibleStatusFilter + `
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
			AND rr.idempotency_hash LIKE '%' || incidents.id || '%'
			AND (
				rr.status = 'generated'
				OR (rr.status = 'pending' AND rr.updated_at >= now() - interval '10 minutes')
				OR (rr.status = 'generating' AND rr.lease_expires_at >= now())
				OR (rr.status = 'failed_retryable' AND rr.next_attempt_at > now())
				OR EXISTS (SELECT 1 FROM reports r WHERE r.request_id = rr.id)
			)
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
	// Release scheduler leadership deterministically instead of relying on the
	// connection close, so a follower can take over immediately.
	s.ReleaseLeadership()
	// Drain any in-flight async report sends. s.ctx (the reporter's context) was
	// cancelled above, so these abort promptly rather than waiting out the full
	// client timeout.
	s.reporter.Close()
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
		// The report covers the day that just ended, not the one that just
		// started. Firing at 07:00 with today's date produced a window of
		// today 00:00 → tomorrow 00:00, which is 17 hours in the future at
		// generation time: the card was titled with tomorrow's date, and each
		// day's 07:00–24:00 window was never covered by any report.
		reportDate := localNow.AddDate(0, 0, -1)
		if err := s.triggerDailyReportForDate(reportDate); err != nil {
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
