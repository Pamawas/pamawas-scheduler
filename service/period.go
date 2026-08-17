package service

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

// PeriodBoundaries represents the computed period boundaries for a report
type PeriodBoundaries struct {
	PeriodStart time.Time
	PeriodEnd   time.Time
	Timezone    string
}

// ComputeDailyPeriod computes the daily period boundaries from a report date and IANA timezone.
// The period is the local day [00:00, next 00:00) converted to exact UTC instants.
// Returns the period boundaries and an error if the timezone is invalid.
func ComputeDailyPeriod(reportDate string, timezone string) (PeriodBoundaries, error) {
	loc, err := time.LoadLocation(timezone)
	if err != nil {
		return PeriodBoundaries{}, fmt.Errorf("invalid timezone %q: %w", timezone, err)
	}

	// Parse the report date as a local date in the given timezone
	dateLayout := "2006-01-02"
	localDate, err := time.ParseInLocation(dateLayout, reportDate, loc)
	if err != nil {
		return PeriodBoundaries{}, fmt.Errorf("invalid report date %q: %w", reportDate, err)
	}

	// Period start is 00:00:00 local time on the report date
	periodStart := time.Date(localDate.Year(), localDate.Month(), localDate.Day(), 0, 0, 0, 0, loc)
	// Period end is 00:00:00 local time on the next day
	periodEnd := periodStart.AddDate(0, 0, 1)

	// Convert to UTC for persistence
	periodStartUTC := periodStart.UTC()
	periodEndUTC := periodEnd.UTC()

	log.Debug().
		Str("report_date", reportDate).
		Str("timezone", timezone).
		Str("period_start_local", periodStart.Format(time.RFC3339)).
		Str("period_end_local", periodEnd.Format(time.RFC3339)).
		Str("period_start_utc", periodStartUTC.Format(time.RFC3339)).
		Str("period_end_utc", periodEndUTC.Format(time.RFC3339)).
		Msg("Computed daily period boundaries")

	return PeriodBoundaries{
		PeriodStart: periodStartUTC,
		PeriodEnd:   periodEndUTC,
		Timezone:    timezone,
	}, nil
}

// ComputeHighSeverityPeriod computes the period boundaries for a high-severity immediate report.
// The period is a fixed 10-minute window ending at the current time in UTC.
// This is used for immediate high-severity alerts.
func ComputeHighSeverityPeriod() PeriodBoundaries {
	now := time.Now().UTC()
	periodEnd := now.Truncate(time.Minute) // Round to nearest minute
	periodStart := periodEnd.Add(-10 * time.Minute)

	return PeriodBoundaries{
		PeriodStart: periodStart,
		PeriodEnd:   periodEnd,
		Timezone:    "UTC",
	}
}

// GenerateIdempotencyHash creates a deterministic hash for idempotency key
// based on request type, date/timezone, and policy version.
func GenerateIdempotencyHash(requestType string, periodStart, periodEnd time.Time, timezone string, policyVersion int) string {
	data := fmt.Sprintf("%s|%s|%s|%s|%d",
		requestType,
		periodStart.Format(time.RFC3339Nano),
		periodEnd.Format(time.RFC3339Nano),
		timezone,
		policyVersion,
	)
	hash := sha256.Sum256([]byte(data))
	return hex.EncodeToString(hash[:])
}

// ReportPolicyVersion is the current report policy version
const ReportPolicyVersion = 1

// ULID generation for request IDs
var (
	ulidMu   sync.Mutex
	ulidTime uint64
	ulidSeq  uint64
)

// generateULID generates a ULID-like string with prefix
// Format: prefix_01XXXXXXXXXXXXXXXXXXXX (26 chars after prefix)
// This is a simplified ULID implementation for compatibility
func generateULID(prefix string) string {
	ulidMu.Lock()
	defer ulidMu.Unlock()

	now := uint64(time.Now().UnixMilli())
	if now == ulidTime {
		ulidSeq++
	} else {
		ulidTime = now
		ulidSeq = 0
	}

	// Encode timestamp (48 bits) + sequence/random (80 bits)
	// Using Crockford's base32 encoding
	const encoding = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
	result := make([]byte, 26)

	// Timestamp (first 10 chars = 48 bits / 5 = 9.6, so 10 chars)
	temp := now
	for i := 9; i >= 0; i-- {
		result[i] = encoding[temp&31]
		temp >>= 5
	}

	// Random/sequence for remaining 16 chars (80 bits)
	var randBytes [10]byte
	if _, err := rand.Read(randBytes[:]); err != nil {
		// Fallback to time-based if crypto/rand fails
		randVal := time.Now().UnixNano()
		randVal2 := time.Now().UnixNano() + 1
		combined := (uint64(randVal) << 16) | (uint64(randVal2) >> 48) | (ulidSeq << 48)
		for i := 25; i >= 10; i-- {
			result[i] = encoding[combined&31]
			combined >>= 5
		}
	} else {
		// Use first 8 bytes for random, last 2 for sequence
		combined := binary.BigEndian.Uint64(randBytes[:8])
		combined = (combined << 16) | (ulidSeq & 0xFFFF)
		for i := 25; i >= 10; i-- {
			result[i] = encoding[combined&31]
			combined >>= 5
		}
	}

	return prefix + "_" + string(result)
}