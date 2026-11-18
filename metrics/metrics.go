package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics holds all Prometheus metrics for the scheduler service
type Metrics struct {
	DailyReportsTotal               prometheus.Counter
	HighSeverityChecksTotal         prometheus.Counter
	HighSeverityAlertsTotal         prometheus.Counter
	DBConnectionErrors              prometheus.Counter
	LastDailyReport                 prometheus.Gauge
	LastHighSeverityCheck           prometheus.Gauge
	SchedulerRunning                prometheus.Gauge
	SchedulerUptime                 prometheus.Gauge
	SchedulerLeader                 prometheus.Gauge
	StalledDeliveriesRecoveredTotal prometheus.Counter
}

// NewMetrics creates and registers all metrics.
//
// Names follow the pamawas_<subsystem>_<metric> convention (design review
// P3): namespace `pamawas`, subsystem `scheduler`.
func NewMetrics() *Metrics {
	return &Metrics{
		DailyReportsTotal: promauto.NewCounter(
			prometheus.CounterOpts{
				Namespace: "pamawas",
				Subsystem: "scheduler",
				Name:      "daily_reports_total",
				Help:      "Total number of daily reports triggered",
			},
		),
		HighSeverityChecksTotal: promauto.NewCounter(
			prometheus.CounterOpts{
				Namespace: "pamawas",
				Subsystem: "scheduler",
				Name:      "high_severity_checks_total",
				Help:      "Total number of high severity checks performed",
			},
		),
		HighSeverityAlertsTotal: promauto.NewCounter(
			prometheus.CounterOpts{
				Namespace: "pamawas",
				Subsystem: "scheduler",
				Name:      "high_severity_alerts_total",
				Help:      "Total number of high severity alerts triggered",
			},
		),
		DBConnectionErrors: promauto.NewCounter(
			prometheus.CounterOpts{
				Namespace: "pamawas",
				Subsystem: "scheduler",
				Name:      "db_connection_errors_total",
				Help:      "Total number of database connection errors",
			},
		),
		LastDailyReport: promauto.NewGauge(
			prometheus.GaugeOpts{
				Namespace: "pamawas",
				Subsystem: "scheduler",
				Name:      "last_daily_report_timestamp_seconds",
				Help:      "Timestamp of last daily report",
			},
		),
		LastHighSeverityCheck: promauto.NewGauge(
			prometheus.GaugeOpts{
				Namespace: "pamawas",
				Subsystem: "scheduler",
				Name:      "last_high_severity_check_timestamp_seconds",
				Help:      "Timestamp of last high severity check",
			},
		),
		SchedulerRunning: promauto.NewGauge(
			prometheus.GaugeOpts{
				Namespace: "pamawas",
				Subsystem: "scheduler",
				Name:      "running",
				Help:      "Whether scheduler is currently running (1) or not (0)",
			},
		),
		SchedulerUptime: promauto.NewGauge(
			prometheus.GaugeOpts{
				Namespace: "pamawas",
				Subsystem: "scheduler",
				Name:      "uptime_seconds",
				Help:      "Uptime of the scheduler in seconds",
			},
		),
		SchedulerLeader: promauto.NewGauge(
			prometheus.GaugeOpts{
				Namespace: "pamawas",
				Subsystem: "scheduler",
				Name:      "leader",
				Help:      "Whether this instance holds scheduler leadership (1) or not (0)",
			},
		),
		StalledDeliveriesRecoveredTotal: promauto.NewCounter(
			prometheus.CounterOpts{
				Namespace: "pamawas",
				Subsystem: "scheduler",
				Name:      "stalled_deliveries_recovered_total",
				Help:      "Total number of stalled delivery attempts recovered",
			},
		),
	}
}
