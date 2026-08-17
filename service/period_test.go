package service

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestComputeDailyPeriod(t *testing.T) {
	tests := []struct {
		name          string
		reportDate    string
		timezone      string
		wantStartHour int
		wantEndHour   int
		wantErr       bool
	}{
		{
			name:          "normal day Asia/Jakarta",
			reportDate:    "2026-08-16",
			timezone:      "Asia/Jakarta",
			wantStartHour: 17, // 00:00 WIB = 17:00 UTC previous day
			wantEndHour:   17, // 00:00 WIB next day = 17:00 UTC
			wantErr:       false,
		},
		{
			name:          "normal day UTC",
			reportDate:    "2026-08-16",
			timezone:      "UTC",
			wantStartHour: 0,
			wantEndHour:   0,
			wantErr:       false,
		},
		{
			name:          "normal day US/Eastern",
			reportDate:    "2026-08-16",
			timezone:      "America/New_York",
			wantStartHour: 4, // 00:00 EDT = 04:00 UTC
			wantEndHour:   4,
			wantErr:       false,
		},
		{
			name:       "invalid timezone",
			reportDate: "2026-08-16",
			timezone:   "Invalid/Timezone",
			wantErr:    true,
		},
		{
			name:       "invalid date format",
			reportDate: "16-08-2026",
			timezone:   "UTC",
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			boundaries, err := ComputeDailyPeriod(tt.reportDate, tt.timezone)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.timezone, boundaries.Timezone)
			require.Equal(t, tt.wantStartHour, boundaries.PeriodStart.Hour())
			require.Equal(t, tt.wantEndHour, boundaries.PeriodEnd.Hour())
			require.True(t, boundaries.PeriodEnd.After(boundaries.PeriodStart))
			require.Equal(t, 24*time.Hour, boundaries.PeriodEnd.Sub(boundaries.PeriodStart))
		})
	}
}

func TestComputeDailyPeriod_DST_Transitions(t *testing.T) {
	// Test DST spring forward (23-hour day) - US Eastern
	// 2026-03-08 is DST start in US
	boundaries, err := ComputeDailyPeriod("2026-03-08", "America/New_York")
	require.NoError(t, err)
	// 00:00 EST -> 00:00 EDT next day = 23 hours
	duration := boundaries.PeriodEnd.Sub(boundaries.PeriodStart)
	require.Equal(t, 23*time.Hour, duration, "DST spring forward should be 23 hours")

	// Test DST fall back (25-hour day) - US Eastern
	// 2026-11-01 is DST end in US
	boundaries, err = ComputeDailyPeriod("2026-11-01", "America/New_York")
	require.NoError(t, err)
	// 00:00 EDT -> 00:00 EST next day = 25 hours
	duration = boundaries.PeriodEnd.Sub(boundaries.PeriodStart)
	require.Equal(t, 25*time.Hour, duration, "DST fall back should be 25 hours")
}

func TestComputeHighSeverityPeriod(t *testing.T) {
	boundaries := ComputeHighSeverityPeriod()
	require.Equal(t, "UTC", boundaries.Timezone)
	require.True(t, boundaries.PeriodEnd.After(boundaries.PeriodStart))
	require.Equal(t, 10*time.Minute, boundaries.PeriodEnd.Sub(boundaries.PeriodStart))
	// Should be truncated to minute
	require.Equal(t, 0, boundaries.PeriodEnd.Second())
	require.Equal(t, 0, boundaries.PeriodStart.Second())
}

func TestGenerateIdempotencyHash(t *testing.T) {
	periodStart, err := time.Parse(time.RFC3339, "2026-08-15T17:00:00Z")
	require.NoError(t, err)
	periodEnd, err := time.Parse(time.RFC3339, "2026-08-16T17:00:00Z")
	require.NoError(t, err)

	hash1 := GenerateIdempotencyHash("daily", periodStart, periodEnd, "Asia/Jakarta", 1)
	hash2 := GenerateIdempotencyHash("daily", periodStart, periodEnd, "Asia/Jakarta", 1)
	hash3 := GenerateIdempotencyHash("daily", periodStart, periodEnd, "UTC", 1)

	require.Equal(t, hash1, hash2, "Same inputs should produce same hash")
	require.NotEqual(t, hash1, hash3, "Different timezone should produce different hash")
	require.Len(t, hash1, 64, "SHA256 hex should be 64 chars")
}

func TestGenerateULID(t *testing.T) {
	id1 := generateULID("rreq")
	id2 := generateULID("rreq")

	require.Len(t, id1, 31, "rreq_ + 26 chars = 31")
	require.Equal(t, "rreq_", id1[:5])
	require.NotEqual(t, id1, id2, "ULIDs should be unique")

	// Test prefix
	id3 := generateULID("evt")
	require.Equal(t, "evt_", id3[:4])
}