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
	"github.com/wneessen/go-mail"

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
	reporterRequestTimeout = 8 * time.Minute
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
	// Notification channel fields for Phase 1 real-time high-severity alerts.
	DiscordWebhookURL  string
	TelegramBotToken   string
	TelegramChatID     string
	TelegramAPIBaseURL string
	EmailSMTPHost      string
	EmailSMTPPort      int
	EmailUsername      string
	EmailPassword      string
	EmailFrom          string
	EmailTo            string
}

// ReporterClient is a client for triggering reports via the reporter service
type ReporterClient struct {
	baseURL    string
	httpClient *http.Client
	ctx        context.Context
	wg         sync.WaitGroup
}

func NewReporterClient(baseURL string, ctx context.Context) *ReporterClient {
	return &ReporterClient{
		baseURL:    baseURL,
		httpClient: &http.Client{Timeout: reporterRequestTimeout},
		ctx:        ctx,
	}
}

// NotificationChannel represents a configured notification channel
type NotificationChannel struct {
	Name           string
	DestinationKey string
	Enabled        bool
}

// NotificationClient sends real-time alerts via configured channels
type NotificationClient struct {
	config    SchedulerConfig
	httpClient *http.Client
	ctx       context.Context
	wg        sync.WaitGroup
}

func NewNotificationClient(cfg SchedulerConfig, ctx context.Context) *NotificationClient {
	return &NotificationClient{
		config:    cfg,
		httpClient: &http.Client{Timeout: 10 * time.Second},
		ctx:       ctx,
	}
}

// EnabledChannels returns the list of configured notification channels
func (n *NotificationClient) EnabledChannels() []NotificationChannel {
	channels := []NotificationChannel{
		{Name: "discord", DestinationKey: n.config.DiscordWebhookURL, Enabled: n.config.DiscordWebhookURL != ""},
		{Name: "telegram", DestinationKey: n.config.TelegramChatID, Enabled: n.config.TelegramBotToken != "" && n.config.TelegramChatID != ""},
		{Name: "email", DestinationKey: n.config.EmailTo, Enabled: n.config.EmailSMTPHost != "" && n.config.EmailUsername != "" && n.config.EmailPassword != "" && n.config.EmailTo != ""},
	}

	var enabled []NotificationChannel
	for _, ch := range channels {
		if ch.Enabled {
			enabled = append(enabled, ch)
		}
	}
	return enabled
}

// SendAlertAsync dispatches an immediate high-severity alert via all configured channels.
// It is fire-and-forget so the scheduler tick loop is never blocked.
func (n *NotificationClient) SendAlertAsync(incidentIDs []string, alertType string) {
	if len(n.EnabledChannels()) == 0 {
		log.Debug().Msg("No notification channels configured; skipping real-time alert")
		return
	}

	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		defer func() {
			if r := recover(); r != nil {
				log.Error().Interface("panic", r).Msg("Recovered panic sending real-time alert")
			}
		}()

		content := n.buildAlertContent(incidentIDs, alertType)
		if content == "" {
			return
		}

		for _, ch := range n.EnabledChannels() {
			switch ch.Name {
			case "discord":
				if err := n.sendDiscord(content); err != nil {
					log.Error().Err(err).Str("channel", "discord").Msg("Failed to send real-time alert to Discord")
				}
			case "telegram":
				if err := n.sendTelegram(content); err != nil {
					log.Error().Err(err).Str("channel", "telegram").Msg("Failed to send real-time alert to Telegram")
				}
			case "email":
				if err := n.sendEmail(content); err != nil {
					log.Error().Err(err).Str("channel", "email").Msg("Failed to send real-time alert via email")
				}
			}
		}
	}()
}

// Close waits for any in-flight async alert sends to complete
func (n *NotificationClient) Close() {
	n.wg.Wait()
}

// buildAlertContent builds the real-time alert message content
func (n *NotificationClient) buildAlertContent(incidentIDs []string, alertType string) string {
	if len(incidentIDs) == 0 {
		return ""
	}

	var sb strings.Builder
	fmt.Fprintf(&sb, "🚨 *Pamawas High-Severity Alert*\n")
	fmt.Fprintf(&sb, "Type: %s\n", alertType)
	fmt.Fprintf(&sb, "Incidents: %s\n", strings.Join(incidentIDs, ", "))
	fmt.Fprintf(&sb, "Time: %s\n", time.Now().UTC().Format(time.RFC3339))
	sb.WriteString("Investigation will follow up with findings once complete.")
	return sb.String()
}

// sendDiscord sends an alert message to Discord via webhook
func (n *NotificationClient) sendDiscord(content string) error {
	if n.config.DiscordWebhookURL == "" {
		return nil
	}
	payload := map[string]interface{}{
		"content":  content,
		"username": "Pamawas Scheduler",
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal Discord payload: %w", err)
	}

	req, err := http.NewRequestWithContext(n.ctx, http.MethodPost, n.config.DiscordWebhookURL, strings.NewReader(string(payloadBytes)))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := n.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			log.Error().Err(closeErr).Msg("Failed to close Discord response body")
		}
	}()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("discord webhook returned status %d", resp.StatusCode)
	}
	log.Info().Msg("Real-time alert sent to Discord")
	return nil
}

// sendTelegram sends an alert message to Telegram via Bot API
func (n *NotificationClient) sendTelegram(content string) error {
	if n.config.TelegramBotToken == "" || n.config.TelegramChatID == "" {
		return nil
	}
	telegramBase := n.config.TelegramAPIBaseURL
	if telegramBase == "" {
		telegramBase = "https://api.telegram.org"
	}
	url := fmt.Sprintf("%s/bot%s/sendMessage", telegramBase, n.config.TelegramBotToken)
	payload := map[string]interface{}{
		"chat_id":    n.config.TelegramChatID,
		"text":       content,
		"parse_mode": "Markdown",
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(n.ctx, http.MethodPost, url, strings.NewReader(string(payloadBytes)))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := n.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			log.Error().Err(closeErr).Msg("Failed to close Telegram response body")
		}
	}()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("telegram API returned status %d", resp.StatusCode)
	}
	log.Info().Msg("Real-time alert sent to Telegram")
	return nil
}

// sendEmail sends an alert message via SMTP
func (n *NotificationClient) sendEmail(content string) error {
	if n.config.EmailSMTPHost == "" || n.config.EmailUsername == "" || n.config.EmailPassword == "" || n.config.EmailFrom == "" || n.config.EmailTo == "" {
		return nil
	}

	client, err := mail.NewClient(n.config.EmailSMTPHost,
		mail.WithPort(n.config.EmailSMTPPort),
		mail.WithSMTPAuth(mail.SMTPAuthPlain),
		mail.WithUsername(n.config.EmailUsername),
		mail.WithPassword(n.config.EmailPassword),
		mail.WithTLSPortPolicy(mail.TLSMandatory),
	)
	if err != nil {
		return fmt.Errorf("failed to create SMTP client: %w", err)
	}

	msg := mail.NewMsg()
	if err := msg.From(n.config.EmailFrom); err != nil {
		return fmt.Errorf("failed to set from address: %w", err)
	}
	if err := msg.To(n.config.EmailTo); err != nil {
		return fmt.Errorf("failed to set to address: %w", err)
	}

	msg.Subject("Pamawas High-Severity Alert")
	msg.SetBodyString(mail.TypeTextPlain, content)

	emailCtx, cancel := context.WithTimeout(n.ctx, 10*time.Second)
	defer cancel()

	if err := client.DialAndSendWithContext(emailCtx, msg); err != nil {
		return fmt.Errorf("failed to send email: %w", err)
	}
	log.Info().Str("smtp_host", n.config.EmailSMTPHost).Str("to", n.config.EmailTo).Msg("Real-time alert sent via email")
	return nil
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

// SendReportRequestAsync dispatches the report request to the reporter without
// blocking the caller. It is safe to fire-and-forget here because the caller has
// already committed the report_requests row (idempotency hash) before invoking
// this method, so a slow or unreachable reporter cannot stall the scheduler tick
// loop nor cause a duplicate report. A panic in the send is recovered so the
// scheduler worker is never torn down by a misbehaving reporter. In-flight sends
// are drained by Close; the caller should cancel the client context first.
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
	notification            *NotificationClient
	metrics                 *metrics.Metrics
}

// NewScheduler creates a new scheduler instance
func NewScheduler(db *sql.DB, cfg SchedulerConfig, m *metrics.Metrics) *Scheduler {
	if cfg.DefaultTimezone == "" {
		cfg.DefaultTimezone = "Asia/Jakarta"
	}
	ctx, cancel := context.WithCancel(context.Background())
	reporter := NewReporterClient(cfg.ReporterURL, ctx)
	notification := NewNotificationClient(cfg, ctx)
	return &Scheduler{
		db:         db,
		config:     cfg,
		ctx:        ctx,
		cancelFunc: cancel,
		startTime:  time.Now(),
		reporter:   reporter,
		notification: notification,
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

	// Phase 2: Dispatch pending high-severity report requests whose investigations
	// have reached a terminal state. Runs every tick alongside checkAndTriggerHighSeverity.
	if err := s.checkHighSeverityFollowUp(); err != nil {
		log.Error().Err(err).Msg("Failed to check high-severity follow-up")
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
		// configured) rather than sending directly from the scheduler. Dispatch immediately
		// so the reporter picks up the report request on this tick; the scheduler never
		// blocks waiting for investigation completion.
		_, err := s.CreateHighSeverityReportRequest(s.ctx, eligible, true, true)
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
	// Drain any in-flight async report sends. s.ctx (the reporter's context) was
	// cancelled above, so these abort promptly rather than waiting out the full
	// client timeout.
	s.reporter.Close()
	// Drain any in-flight async alert sends.
	s.notification.Close()
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
