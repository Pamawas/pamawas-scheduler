package models

import (
	"time"
)

// HealthResponse represents the health check response
type HealthResponse struct {
	Status                   string    `json:"status"`
	Timestamp                time.Time `json:"timestamp"`
	LastDailyReport          time.Time `json:"last_daily_report"`
	LastHighSeverityCheck    time.Time `json:"last_high_severity_check"`
	Running                  bool      `json:"running"`
	Uptime                   string    `json:"uptime,omitempty"`
	Version                  string    `json:"version,omitempty"`
}

// ReadyResponse represents the readiness check response
type ReadyResponse struct {
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

// TriggerResponse represents the manual trigger response
type TriggerResponse struct {
	Message string `json:"message"`
	Error   string `json:"error,omitempty"`
}

// StatusResponse represents the status response
type StatusResponse struct {
	LastDailyReport       time.Time `json:"last_daily_report"`
	LastHighSeverityCheck time.Time `json:"last_high_severity_check"`
	Running               bool      `json:"running"`
	Uptime                string    `json:"uptime"`
	Version               string    `json:"version"`
}