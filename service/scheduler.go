package service

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/Pamawas/pamawas-scheduler/metrics"
)

// SchedulerConfig holds the configuration for the scheduler
type SchedulerConfig struct {
	ReporterURL           string
	DailyReportTime       string
	HighSeverityThreshold string
	CheckInterval         time.Duration
	EnableDailyReport     bool
	EnableHighSeverityAlert bool
}

// ReporterClient is a simple client for triggering reports
type ReporterClient struct {
	baseURL    string
	httpClient *http.Client
}

func NewReporterClient(baseURL string) *ReporterClient {
	return &ReporterClient{
		baseURL: baseURL,
		httpClient: &http.Client{Timeout: 10 * time.Second},
	}
}

func (c *ReporterClient) TriggerDailyReport() error {
	resp, err := c.httpClient.Post(c.baseURL+"/report", "application/json", strings.NewReader(`{"source":"scheduler"}`))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("reporter returned status %d", resp.StatusCode)
	}
	return nil
}

func (c *ReporterClient) TriggerHighSeverityAlert() error {
	resp, err := c.httpClient.Post(c.baseURL+"/report", "application/json", strings.NewReader(`{"source":"scheduler","priority":"high"}`))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("reporter returned status %d", resp.StatusCode)
	}
	return nil
}

// Scheduler holds the database connection and scheduling logic
type Scheduler struct {
	db          *sql.DB
	config      SchedulerConfig
	mu          sync.Mutex
	lastDailyReport time.Time
	lastHighSeverityCheck time.Time
	running     bool
	wg          sync.WaitGroup
	ctx         context.Context
	cancelFunc  context.CancelFunc
	startTime   time.Time
	reporter    *ReporterClient
	metrics     *metrics.Metrics
}

// NewScheduler creates a new scheduler instance
func NewScheduler(db *sql.DB, cfg SchedulerConfig, m *metrics.Metrics) *Scheduler {
	ctx, cancel := context.WithCancel(context.Background())
	reporter := NewReporterClient(cfg.ReporterURL)

	return &Scheduler{
		db:       db,
		config:   cfg,
		ctx:      ctx,
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
			log.Println("Scheduler worker stopped")
			return
		case <-ticker.C:
			if err := s.checkAndTrigger(); err != nil {
				log.Printf("Scheduler check error: %v", err)
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
	currentTime := now.Format("15:04") // HH:MM

	// Check for daily report
	if s.config.EnableDailyReport {
		if currentTime == s.config.DailyReportTime {
			// Check if we already sent today's report
			today := now.Format("2006-01-02")
			if s.lastDailyReport.Format("2006-01-02") != today {
				log.Printf("Time for daily report (%s)", s.config.DailyReportTime)
				if err := s.triggerDailyReport(); err != nil {
					log.Printf("Failed to trigger daily report: %v", err)
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
			log.Printf("Failed to check high severity incidents: %v", err)
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

// triggerDailyReport triggers a daily report via the reporter service
func (s *Scheduler) triggerDailyReport() error {
	log.Printf("Triggering daily report")
	if err := s.reporter.TriggerDailyReport(); err != nil {
		return err
	}
	return nil
}

// triggerHighSeverityAlert triggers a high severity alert via the reporter service
func (s *Scheduler) triggerHighSeverityAlert() error {
	log.Printf("Triggering high severity alert")
	if err := s.reporter.TriggerHighSeverityAlert(); err != nil {
		return err
	}
	return nil
}

// checkHighSeverityIncidents checks for high severity incidents that need immediate attention
func (s *Scheduler) checkHighSeverityIncidents() error {
	threshold := strings.ToLower(s.config.HighSeverityThreshold)

	query := `
		SELECT COUNT(*)
		FROM incidents
		WHERE status = 'firing'
		AND (
			(LOWER(severity) = 'critical' AND $1 IN ('critical')) OR
			(LOWER(severity) = 'high' AND $1 IN ('high', 'critical')) OR
			(LOWER(severity) = 'warning' AND $1 IN ('warning', 'high', 'critical')) OR
			(LOWER(severity) = 'info' AND $1 IN ('info', 'warning', 'high', 'critical')) OR
			(LOWER(severity) = 'debug' AND $1 IN ('info', 'warning', 'high', 'critical', 'debug'))
		)
		AND started_at > $2
	`

	// Only check incidents from the last hour to avoid repeated alerts
	since := time.Now().Add(-1 * time.Hour)

	var count int
	err := s.db.QueryRow(query, threshold, since).Scan(&count)
	if err != nil {
		return err
	}

	if count > 0 {
		log.Printf("Found %d %s+ firing incidents in the last hour - triggering high severity alert", count, threshold)
		if err := s.triggerHighSeverityAlert(); err != nil {
			return err
		}
		s.metrics.HighSeverityAlertsTotal.Inc()
	}

	return nil
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