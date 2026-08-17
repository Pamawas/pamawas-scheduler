package models

import (
	"time"
)

// HealthResponse represents the health check response
type HealthResponse struct {
	Status                string    `json:"status"`
	Timestamp             time.Time `json:"timestamp"`
	LastDailyReport       time.Time `json:"last_daily_report"`
	LastHighSeverityCheck time.Time `json:"last_high_severity_check"`
	Running               bool      `json:"running"`
	Uptime                string    `json:"uptime,omitempty"`
	Version               string    `json:"version,omitempty"`
	Error                 string    `json:"error,omitempty"`
}

// ReadyResponse represents the readiness check response
type ReadyResponse struct {
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

// StatusResponse represents the status response
type StatusResponse struct {
	LastDailyReport       time.Time `json:"last_daily_report"`
	LastHighSeverityCheck time.Time `json:"last_high_severity_check"`
	Running               bool      `json:"running"`
	Uptime                string    `json:"uptime"`
	Version               string    `json:"version"`
}

// DailyReportRequest represents the request to create a daily report request
type DailyReportRequest struct {
	ReportDate string `json:"report_date"`
	Timezone   string `json:"timezone"`
}

// HighSeverityReportRequest represents the request to create a high-severity report request
type HighSeverityReportRequest struct {
	IncidentIDs []string `json:"incident_ids,omitempty"`
	Evaluate    bool     `json:"evaluate,omitempty"`
}

// ReportRequestResponse represents the response for report request creation
type ReportRequestResponse struct {
	RequestID     string    `json:"request_id"`
	RequestType   string    `json:"request_type"`
	PeriodStart   time.Time `json:"period_start"`
	PeriodEnd     time.Time `json:"period_end"`
	Timezone      string    `json:"timezone"`
	Status        string    `json:"status"`
	Duplicate     bool      `json:"duplicate"`
	EligibleCount int       `json:"eligible_count,omitempty"`
	IncidentIDs   []string  `json:"incident_ids,omitempty"`
}