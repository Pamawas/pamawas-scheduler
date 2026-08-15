package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics holds all Prometheus metrics for the scheduler service
type Metrics struct {
	DailyReportsTotal       prometheus.Counter
	HighSeverityChecksTotal prometheus.Counter
	HighSeverityAlertsTotal prometheus.Counter
	DBConnectionErrors      prometheus.Counter
	LastDailyReport         prometheus.Gauge
	LastHighSeverityCheck   prometheus.Gauge
	SchedulerRunning        prometheus.Gauge
	SchedulerUptime         prometheus.Gauge
}

// NewMetrics creates and registers all metrics
func NewMetrics() *Metrics {
	return &Metrics{
		DailyReportsTotal: promauto.NewCounter(
			prometheus.CounterOpts{
				Name: "scheduler_daily_reports_total",
				Help: "Total number of daily reports triggered",
			},
		),
		HighSeverityChecksTotal: promauto.NewCounter(
			prometheus.CounterOpts{
				Name: "scheduler_high_severity_checks_total",
				Help: "Total number of high severity checks performed",
			},
		),
		HighSeverityAlertsTotal: promauto.NewCounter(
			prometheus.CounterOpts{
				Name: "scheduler_high_severity_alerts_total",
				Help: "Total number of high severity alerts triggered",
			},
		),
		DBConnectionErrors: promauto.NewCounter(
			prometheus.CounterOpts{
				Name: "scheduler_db_connection_errors_total",
				Help: "Total number of database connection errors",
			},
		),
		LastDailyReport: promauto.NewGauge(
			prometheus.GaugeOpts{
				Name: "scheduler_last_daily_report_timestamp_seconds",
				Help: "Timestamp of last daily report",
			},
		),
		LastHighSeverityCheck: promauto.NewGauge(
			prometheus.GaugeOpts{
				Name: "scheduler_last_high_severity_check_timestamp_seconds",
				Help: "Timestamp of last high severity check",
			},
		),
		SchedulerRunning: promauto.NewGauge(
			prometheus.GaugeOpts{
				Name: "scheduler_running",
				Help: "Whether scheduler is currently running (1) or not (0)",
			},
		),
		SchedulerUptime: promauto.NewGauge(
			prometheus.GaugeOpts{
				Name: "scheduler_uptime_seconds",
				Help: "Uptime of the scheduler in seconds",
			},
		),
	}
}