# pamawas-scheduler

**Cron Scheduler** — Triggers daily morning digest reports and immediate high-severity alert paths.

[![Go Version](https://img.shields.io/badge/Go-1.26+-00ADD8?logo=go)](https://go.dev/)
[![Docker](https://img.shields.io/badge/Docker-Ready-2496ED?logo=docker)](https://docker.com/)

---

## Purpose

Coordinates report delivery on two paths:
1. **Daily cron** — Morning digest at configured time (default 07:00)
2. **High-severity immediate** — Checks every 30s for firing `high`/`critical` incidents

## Trigger Paths

### 1. Daily Morning Digest (Cron)
- Runs at configured time daily (e.g., 07:00 in configured timezone)
- Triggers reporter to generate daily report for last 24 hours
- Only sends if incidents occurred in period
- Prevents duplicate daily reports (once per day)

### 2. Immediate High-Severity Alert
- Runs at configurable interval (default 30s)
- Checks for firing incidents with severity ≥ threshold (default `high`)
- Triggers immediate reporter call for qualifying incidents
- Prevents duplicate alerts within 1-hour window per incident

## Quick Start

```bash
# Docker
docker run -e DATABASE_URL="postgres://user:pass@host:5432/db" \
  -e REPORTER_URL="http://pamawas-reporter:8080" \
  -e DAILY_REPORT_TIME="07:00" \
  -e HIGH_SEVERITY_THRESHOLD="high" \
  -p 8080:8080 ghcr.io/yoganovvaindra/pamawas-scheduler:latest

# Local development
go run main.go

# Manual triggers
curl -X POST http://localhost:8080/trigger/daily
curl -X POST http://localhost:8080/trigger/high-severity
```

## Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `DATABASE_URL` | PostgreSQL connection string | **Required** |
| `PORT` | HTTP server port | `8080` |
| `REPORTER_URL` | Reporter service base URL | `http://localhost:8081` |
| `DAILY_REPORT_TIME` | Daily report time (HH:MM 24h) | `07:00` |
| `HIGH_SEVERITY_THRESHOLD` | Severity threshold | `high` |
| `CHECK_INTERVAL` | High-severity check interval | `30s` |
| `ENABLE_DAILY_REPORT` | Enable daily cron | `true` |
| `ENABLE_HIGH_SEVERITY_ALERT` | Enable high-severity checks | `true` |
| `SCHEDULER_MODE` | Set to `manual` to disable background worker | auto |
| `LOG_LEVEL` | debug, info, warn, error | `info` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Tempo OTLP gRPC endpoint | `tempo:4317` |

## Severity Threshold Logic

| Threshold | Triggers On |
|-----------|-------------|
| `critical` | critical only |
| `high` | high + critical |
| `warning` | warning + high + critical |
| `info` | info + warning + high + critical |
| `debug` | all severities |

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/healthz` | GET | Health check with DB connectivity |
| `/ready` | GET | Readiness probe |
| `/trigger/daily` | POST | Manual daily report trigger |
| `/trigger/high-severity` | POST | Manual high-severity alert trigger |
| `/status` | GET | Scheduler status (last runs, uptime) |
| `/metrics` | GET | Prometheus metrics |

## High-Severity Check Query

```sql
SELECT COUNT(*)
FROM incidents
WHERE status = 'firing'
  AND severity IN ('high', 'critical')  -- based on threshold
  AND started_at > NOW() - INTERVAL '1 hour'
```

## Observability

| Feature | Endpoint |
|---------|----------|
| Prometheus Metrics | `/metrics` — `DailyReportsTotal`, `HighSeverityChecksTotal`, `HighSeverityAlertsTotal`, `LastDailyReport`, `LastHighSeverityCheck` |
| JSON Logging | stdout — trace_id, span_id, service, method, path, status_code, duration_ms |
| OpenTelemetry | OTLP gRPC → Tempo:4317 |

## Building

```bash
docker build -t pamawas-scheduler .
go build -o pamawas-scheduler main.go
```

## Related

- **Root README**: [../README.md](../README.md)
- **Reporter**: [../pamawas-reporter/README.md](../pamawas-reporter/README.md)
- **Correlator**: [../pamawas-correlator/README.md](../pamawas-correlator/README.md)
- **Database Schema**: [../pamawas-schema/README.md](../pamawas-schema/README.md)