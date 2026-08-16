# pamawas-scheduler

**Cron Scheduler** — Daily morning digest + immediate high-severity alert paths

Language: Go 1.26

## Purpose

Triggers report generation on two paths: (1) daily morning cron for digest report, (2) immediate send path for high-severity incidents. Coordinates with the reporter service to deliver reports.

## MVP Reference

- **MVP §10 Build Order #6**: Scheduler — daily cron for the digest, plus an immediate-send path for high-severity incidents
- **MVP §9 Delivery**: Daily morning report + immediate send path for high-severity incidents
- **MVP §8 Architecture Overview**: Scheduler (Go, cron) component

## Responsibilities

- Daily cron trigger at configurable time (default 07:00) for morning digest
- High-severity incident check at configurable interval (default 30s)
- Trigger reporter service via HTTP for both paths
- Health/metrics endpoints
- Graceful shutdown

## Two Trigger Paths (MVP §10)

### 1. Daily Morning Digest (Cron)

- Runs at configured time daily (e.g., 07:00)
- Triggers reporter to generate daily report for last 24 hours
- Only sends if incidents occurred in period

### 2. Immediate High-Severity Alert

- Runs at configured interval (e.g., every 30s)
- Checks for firing incidents with severity >= threshold (high/critical)
- Triggers immediate reporter call for high-severity incidents
- Prevents duplicate alerts within 1 hour window

## Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/healthz` | GET | Health check with DB connectivity |
| `/ready` | GET | Readiness check |
| `/trigger/daily` | POST | Manual daily report trigger |
| `/trigger/high-severity` | POST | Manual high-severity alert trigger |
| `/status` | GET | Scheduler status (last runs, uptime) |
| `/metrics` | GET | Prometheus metrics |

## Configuration (Environment Variables)

| Variable | Description | Default |
|----------|-------------|---------|
| `DATABASE_URL` | PostgreSQL connection string | Required |
| `PORT` | HTTP server port | `8080` |
| `REPORTER_URL` | Reporter service base URL | `http://localhost:8081` |
| `DAILY_REPORT_TIME` | Daily report time (HH:MM 24h) | `07:00` |
| `HIGH_SEVERITY_THRESHOLD` | Severity threshold (debug/info/warning/high/critical) | `high` |
| `CHECK_INTERVAL` | High-severity check interval | `30s` |
| `ENABLE_DAILY_REPORT` | Enable daily cron | `true` |
| `ENABLE_HIGH_SEVERITY_ALERT` | Enable high-severity checks | `true` |
| `SCHEDULER_MODE` | `manual` to disable background worker | (auto) |
| `LOG_LEVEL` | Log level | `info` |
| `ENVIRONMENT` | Deployment environment | `development` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | OTLP gRPC endpoint for Tempo | `tempo:4317` |

## Observability

| Feature | Endpoint/Format |
|---------|-----------------|
| **Prometheus Metrics** | `/metrics` — `DailyReportsTotal`, `HighSeverityChecksTotal`, `HighSeverityAlertsTotal`, `SchedulerRunning`, `LastDailyReport`, `LastHighSeverityCheck` |
| **Structured JSON Logging** | stdout — trace_id, span_id, service, component, method, path, status_code, duration_ms |
| **OpenTelemetry Tracing** | OTLP gRPC → Tempo:4317 — W3C TraceContext propagation |

## Database Schema (from pamawas-schema)

```sql
-- Incidents table (read by scheduler for high-severity check)
CREATE TABLE IF NOT EXISTS incidents (
    id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    status TEXT NOT NULL,
    started_at TIMESTAMPTZ,
    resolved_at TIMESTAMPTZ,
    severity TEXT,
    affected_services TEXT[]
);
```

## Current Implementation Status

- ✅ Daily cron with configurable time (HH:MM)
- ✅ High-severity check with configurable threshold and interval
- ✅ Reporter HTTP client with daily/high-severity trigger methods
- ✅ Duplicate prevention (daily: once per day; high-severity: 1hr window)
- ✅ Manual trigger endpoints (`/trigger/daily`, `/trigger/high-severity`)
- ✅ Health (`/healthz`), readiness (`/ready`), status (`/status`), metrics (`/metrics`)
- ✅ Background worker with graceful shutdown
- ✅ Multi-stage Dockerfile (Go 1.26-alpine builder, alpine runtime)
- ✅ GitHub Actions workflow (main + dev branches, GHCR publishing)
- ✅ **Prometheus metrics with proper labels**
- ✅ **Structured JSON logging with zerolog**
- ✅ **Request/response logging middleware with Loki labels**
- ✅ **OpenTelemetry tracing (OTLP gRPC → Tempo)**
- ✅ Viper config management (YAML + ENV)

## Kanban Tasks

- `t_876ad908` — Design daily cron and immediate high-severity paths (architect)
- `t_c82ba500` — Implement cron scheduler with dual trigger paths (backend-dev)
- `t_2074ab51` — Write unit tests for cron logic (qa-dev)

## Dependencies

- **PostgreSQL** — incidents table (via pamawas-schema)
- **pamawas-schema** — Shared types and migrations (parent: `t_d1cdd7a9`)
- **pamawas-reporter** — Receives trigger calls to generate/send reports
- **pamawas-correlator** — Produces incidents with severity

## Build & Run

```bash
# Local development
go run main.go

# Docker
docker build -t pamawas-scheduler .
docker run -e DATABASE_URL="postgres://..." \
  -e REPORTER_URL="http://pamawas-reporter:8080" \
  -e DAILY_REPORT_TIME="07:00" \
  -e HIGH_SEVERITY_THRESHOLD="high" \
  -p 8080:8080 pamawas-scheduler

# Manual daily trigger
curl -X POST http://localhost:8080/trigger/daily

# Manual high-severity trigger
curl -X POST http://localhost:8080/trigger/high-severity
```

## Severity Threshold Logic

The `HIGH_SEVERITY_THRESHOLD` uses inclusive matching:

- `critical` → only critical
- `high` → high + critical
- `warning` → warning + high + critical
- `info` → info + warning + high + critical
- `debug` → all severities

## Example High-Severity Check Query

```sql
SELECT COUNT(*)
FROM incidents
WHERE status = 'firing'
  AND severity IN ('high', 'critical')  -- based on threshold
  AND started_at > NOW() - INTERVAL '1 hour'
```