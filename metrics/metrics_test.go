package metrics

import (
	"github.com/prometheus/client_golang/prometheus/testutil"
	"testing"
)

func TestNewMetricsCollectors(t *testing.T) {
	m := NewMetrics()
	m.DailyReportsTotal.Inc()
	m.HighSeverityChecksTotal.Add(2)
	m.HighSeverityAlertsTotal.Add(3)
	m.DBConnectionErrors.Inc()
	m.LastDailyReport.Set(10)
	m.LastHighSeverityCheck.Set(20)
	m.SchedulerRunning.Set(1)
	m.SchedulerUptime.Set(30)
	checks := []struct {
		name      string
		got, want float64
	}{
		{"daily", testutil.ToFloat64(m.DailyReportsTotal), 1}, {"checks", testutil.ToFloat64(m.HighSeverityChecksTotal), 2},
		{"alerts", testutil.ToFloat64(m.HighSeverityAlertsTotal), 3}, {"errors", testutil.ToFloat64(m.DBConnectionErrors), 1},
		{"last daily", testutil.ToFloat64(m.LastDailyReport), 10}, {"last check", testutil.ToFloat64(m.LastHighSeverityCheck), 20},
		{"running", testutil.ToFloat64(m.SchedulerRunning), 1}, {"uptime", testutil.ToFloat64(m.SchedulerUptime), 30},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s=%v want %v", c.name, c.got, c.want)
		}
	}
}
