package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/lib/pq"
	"github.com/google/uuid"
)

// SchedulerConfig holds configuration for the scheduler service
type SchedulerConfig struct {
	// Server
	Port string

	// Database
	DatabaseURL string

	// Reporter service URL (for triggering reports)
	ReporterURL string

	// Scheduling
	DailyReportTime   string // HH:MM format (24-hour)
	HighSeverityThreshold string // e.g., "high", "critical"
	CheckInterval     time.Duration

	// Feature flags
	EnableDailyReport bool
	EnableHighSeverityAlert bool
}

// HealthStatus represents the health of the scheduler
type HealthStatus struct {
	Status      string    `json:"status"`
	Timestamp   time.Time `json:"timestamp"`
	LastDailyReport time.Time `json:"last_daily_report"`
	LastHighSeverityCheck time.Time `json:"last_high_severity_check"`
	Running     bool      `json:"running"`
	Uptime      string    `json:"uptime,omitempty"`
	Version     string    `json:"version,omitempty"`
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
}

// ReporterClient is a simple client for triggering reports
type ReporterClient struct {
	baseURL string
	httpClient *http.Client
}

func NewReporterClient(baseURL string) *ReporterClient {
	return &ReporterClient{
		baseURL: baseURL,
		httpClient: &http.Client{Timeout: 10 * time.Second},
	}
}

func (c *ReporterClient) TriggerDailyReport() error {
	resp, err := c.httpClient.Post(c.baseURL + "/report", "application/json", strings.NewReader(`{"source":"scheduler"}`))
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
	resp, err := c.httpClient.Post(c.baseURL + "/report", "application/json", strings.NewReader(`{"source":"scheduler","priority":"high"}`))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("reporter returned status %d", resp.StatusCode)
	}
	return nil
}

func main() {
	// Load configuration from environment
	config := SchedulerConfig{
		Port: getEnv("PORT", "8080"),

		DatabaseURL: getEnv("DATABASE_URL", ""),

		ReporterURL: getEnv("REPORTER_URL", "http://localhost:8081"),

		DailyReportTime: getEnv("DAILY_REPORT_TIME", "07:00"), // 7 AM default
		HighSeverityThreshold: getEnv("HIGH_SEVERITY_THRESHOLD", "high"),
		CheckInterval:     mustParseDuration(getEnv("CHECK_INTERVAL", "30s")),

		EnableDailyReport: getEnv("ENABLE_DAILY_REPORT", "true") == "true",
		EnableHighSeverityAlert: getEnv("ENABLE_HIGH_SEVERITY_ALERT", "true") == "true",
	}

	// Validate required configuration
	if config.DatabaseURL == "" {
		log.Fatal("DATABASE_URL environment variable not set")
	}

	// Connect to database
	db, err := sql.Open("postgres", config.DatabaseURL)
	if err != nil {
		log.Fatalf("Error opening database: %v", err)
	}
	defer db.Close()

	// Test connection
	if err = db.Ping(); err != nil {
		log.Fatalf("Error connecting to database: %v", err)
	}

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := &Scheduler{
		db:       db,
		config:   config,
		ctx:      ctx,
		startTime: time.Now(),
	}

	// HTTP server for health checks and manual triggers
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.healthHandler)
	mux.HandleFunc("/ready", s.readyHandler)
	mux.HandleFunc("/trigger/daily", s.triggerDailyHandler)
	mux.HandleFunc("/trigger/high-severity", s.triggerHighSeverityHandler)
	mux.HandleFunc("/status", s.statusHandler)
	mux.HandleFunc("/metrics", s.metricsHandler)

	port := config.Port
	log.Printf("Starting scheduler on :%s", port)

	// Start background worker if enabled
	if os.Getenv("SCHEDULER_MODE") != "manual" {
		go s.schedulerWorker()
	}

	// Start HTTP server
	srv := &http.Server{
		Addr:    ":" + port,
		Handler: mux,
	}

	// Listen for shutdown signals
	go func() {
		<-ctx.Done()
		log.Println("Shutting down server...")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			log.Printf("Server forced to shutdown: %v", err)
		}
	}()

	log.Fatal(srv.ListenAndServe())
}

// schedulerWorker runs the scheduling process periodically
func (s *Scheduler) schedulerWorker() {
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
				// Continue despite errors - don't want to stop the worker
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
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		s.running = false
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
			s.mu.Unlock()
		}
	}

	return nil
}

// triggerDailyReport triggers a daily report via the reporter service
func (s *Scheduler) triggerDailyReport() error {
	log.Printf("Triggering daily report")
	reporter := NewReporterClient(s.config.ReporterURL)
	return reporter.TriggerDailyReport()
}

// triggerHighSeverityAlert triggers a high severity alert via the reporter service
func (s *Scheduler) triggerHighSeverityAlert() error {
	log.Printf("Triggering high severity alert")
	reporter := NewReporterClient(s.config.ReporterURL)
	return reporter.TriggerHighSeverityAlert()
}

// checkHighSeverityIncidents checks for high severity incidents that need immediate attention
func (s *Scheduler) checkHighSeverityIncidents() error {
	// Query for incidents with severity >= threshold that are still firing and haven't been alerted recently
	// For simplicity, we'll just check if there are any firing incidents at or above the threshold
	
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
	}

	return nil
}

// triggerDailyHandler handles manual daily report triggers
func (s *Scheduler) triggerDailyHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if err := s.triggerDailyReport(); err != nil {
		http.Error(w, fmt.Sprintf("Failed to trigger daily report: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{
		"message": "Daily report triggered successfully",
	})
}

// triggerHighSeverityHandler handles manual high severity alert triggers
func (s *Scheduler) triggerHighSeverityHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if err := s.triggerHighSeverityAlert(); err != nil {
		http.Error(w, fmt.Sprintf("Failed to trigger high severity alert: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{
		"message": "High severity alert triggered successfully",
	})
}

// Health check handlers
func (s *Scheduler) healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Check database connectivity
	if err := s.db.PingContext(r.Context()); err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]string{
			"status": "unhealthy",
			"error":  fmt.Sprintf("Database connection failed: %v", err),
		})
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(HealthStatus{
		Status:      "healthy",
		Timestamp:   time.Now().UTC(),
		LastDailyReport: s.lastDailyReport,
		LastHighSeverityCheck: s.lastHighSeverityCheck,
		Running:     s.running,
		Version:     "1.0.0",
	})
}

func (s *Scheduler) readyHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Simple readiness check - if we can connect to db, we're ready
	if err := s.db.PingContext(r.Context()); err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]string{
			"status": "not ready",
			"error":  fmt.Sprintf("Database not ready: %v", err),
		})
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "ready"})
}

// statusHandler returns the current status of the scheduler
func (s *Scheduler) statusHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	json.NewEncoder(w).Encode(map[string]interface{}{
		"last_daily_report": s.lastDailyReport,
		"last_high_severity_check": s.lastHighSeverityCheck,
		"running": s.running,
		"uptime": time.Since(s.startTime).String(),
		"version": "1.0.0",
	})
}

// metricsHandler exposes Prometheus metrics
func (s *Scheduler) metricsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Simple metrics - in production you'd use prometheus client library
	fmt.Fprintf(w, `# HELP pamawas_scheduler_last_daily_report_timestamp_seconds Timestamp of last daily report
# TYPE pamawas_scheduler_last_daily_report_timestamp_seconds gauge
pamawas_scheduler_last_daily_report_timestamp_seconds %d
`,
		s.lastDailyReport.Unix())

	fmt.Fprintf(w, `# HELP pamawas_scheduler_last_high_severity_check_timestamp_seconds Timestamp of last high severity check
# TYPE pamawas_scheduler_last_high_severity_check_timestamp_seconds gauge
pamawas_scheduler_last_high_severity_check_timestamp_seconds %d
`,
		s.lastHighSeverityCheck.Unix())

	fmt.Fprintf(w, `# HELP pamawas_scheduler_running Whether the scheduler is currently running
# TYPE pamawas_scheduler_running gauge
pamawas_scheduler_running %d
`,
		boolToInt(s.running))

	fmt.Fprintf(w, `# HELP pamawas_scheduler_uptime_seconds Uptime of the scheduler in seconds
# TYPE pamawas_scheduler_uptime_seconds gauge
pamawas_scheduler_uptime_seconds %.2f
`,
		time.Since(s.startTime).Seconds())
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// Helper functions
func getEnv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func mustParseDuration(s string) time.Duration {
	d, err := time.ParseDuration(s)
	if err != nil {
		log.Fatalf("Invalid duration %s: %v", s, err)
	}
	return d
}
