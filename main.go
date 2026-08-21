package main

import (
	"context"
	"database/sql"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/lib/pq"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/Pamawas/pamawas-scheduler/config"
	"github.com/Pamawas/pamawas-scheduler/handlers"
	"github.com/Pamawas/pamawas-scheduler/metrics"
	"github.com/Pamawas/pamawas-scheduler/middleware"
	"github.com/Pamawas/pamawas-scheduler/otel"
)

func main() {
	cfg := config.Load()
	initLogger(cfg)

	// Initialize OpenTelemetry tracing
	otelShutdown, err := otel.InitTracer(otel.Config{
		ServiceName:  "pamawas-scheduler",
		OTLPEndpoint: os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"),
		Insecure:     true,
		SampleRatio:  1.0,
		Enabled:      os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "",
	})
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize OpenTelemetry")
	}
	defer func() {
		if shutdownErr := otelShutdown(context.Background()); shutdownErr != nil {
			log.Error().Err(shutdownErr).Msg("Error shutting down OpenTelemetry")
		}
	}()

	log.Info().
		Str("port", cfg.Port).
		Str("environment", cfg.Environment).
		Str("log_level", cfg.LogLevel).
		Str("reporter_url", cfg.ReporterURL).
		Str("daily_report_time", cfg.DailyReportTime).
		Str("high_severity_threshold", cfg.HighSeverityThreshold).
		Str("check_interval", cfg.CheckInterval.String()).
		Bool("enable_daily_report", cfg.EnableDailyReport).
		Bool("enable_high_severity_alert", cfg.EnableHighSeverityAlert).
		Str("mode", cfg.Mode).
		Msg("Starting pamawas-scheduler")

	// Connect to database with retries
	db, err := sql.Open("postgres", cfg.DatabaseURL)
	if err != nil {
		log.Fatal().Err(err).Msg("Error opening database")
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Error().Err(err).Msg("Failed to close database connection")
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for i := 0; i < 30; i++ {
		if err := db.PingContext(ctx); err == nil {
			break
		}
		log.Info().Int("attempt", i+1).Msg("Waiting for database...")
		time.Sleep(1 * time.Second)
	}

	if err := db.PingContext(ctx); err != nil {
		log.Fatal().Err(err).Msg("Failed to connect to database after retries")
	}

	log.Info().Msg("Connected to database")

	// Initialize metrics
	m := metrics.NewMetrics()

	// Initialize handlers
	h := handlers.NewHandler(db, cfg, m)

	// Start background worker if not in manual mode
	if cfg.Mode != "manual" {
		go h.Scheduler().StartWorker()
	}

	// Create router
	r := http.NewServeMux()
	r.HandleFunc("/healthz", h.HealthHandler)
	r.HandleFunc("/ready", h.ReadyHandler)
	r.HandleFunc("/status", h.StatusHandler)
	r.Handle("/metrics", h.MetricsHandler())

	// v1 API endpoints
	r.HandleFunc("/v1/report-requests/daily", h.CreateDailyReportRequest)
	r.HandleFunc("/v1/report-requests/high-severity", h.CreateHighSeverityReportRequest)

	// Wrap router with middleware
	var handler http.Handler = r
	handler = middleware.LoggingMiddleware("pamawas-scheduler", handler)
	handler = middleware.ErrorLoggingMiddleware("pamawas-scheduler", handler)

	// Create server
	srv := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      handler,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Graceful shutdown
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh

		log.Info().Msg("Shutdown signal received, stopping server...")
		h.Scheduler().Stop()

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			log.Error().Err(err).Msg("Server forced to shutdown")
		}
	}()

	log.Info().Str("port", cfg.Port).Msg("Starting server")
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal().Err(err).Msg("Server failed")
	}

	log.Info().Msg("Server stopped gracefully")
}

func initLogger(cfg config.Config) {
	level, err := zerolog.ParseLevel(cfg.LogLevel)
	if err != nil {
		level = zerolog.InfoLevel
	}
	zerolog.SetGlobalLevel(level)

	if cfg.Environment == "development" {
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})
	} else {
		log.Logger = zerolog.New(os.Stderr).With().Timestamp().Logger()
	}
}
