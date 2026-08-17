package main

import (
	"github.com/Pamawas/pamawas-scheduler/config"
	"github.com/rs/zerolog"
	"testing"
)

func TestInitLoggerSetsConfiguredLevel(t *testing.T) {
	old := zerolog.GlobalLevel()
	t.Cleanup(func() { zerolog.SetGlobalLevel(old) })
	initLogger(config.Config{LogLevel: "debug", Environment: "production"})
	if got := zerolog.GlobalLevel(); got != zerolog.DebugLevel {
		t.Fatalf("level=%v", got)
	}
}

func TestInitLoggerFallsBackToInfo(t *testing.T) {
	old := zerolog.GlobalLevel()
	t.Cleanup(func() { zerolog.SetGlobalLevel(old) })
	initLogger(config.Config{LogLevel: "invalid", Environment: "development"})
	if got := zerolog.GlobalLevel(); got != zerolog.InfoLevel {
		t.Fatalf("level=%v", got)
	}
}
