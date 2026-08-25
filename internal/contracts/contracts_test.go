package contracts

import (
	"testing"
	"time"
)

func TestAccessKeyUsesUTCAndRawURLIdentity(t *testing.T) {
	t.Parallel()

	hour := time.Date(2026, 8, 25, 17, 42, 0, 0, time.FixedZone("UTC+3", 3*60*60))
	got, err := AccessKey("site-a", "b", "a/b", hour)
	if err != nil {
		t.Fatalf("AccessKey() error = %v", err)
	}
	const want = "cwm-minio-tierer:v1:site-a:access:2026:08:25:14:Yg:YS9i"
	if got != want {
		t.Fatalf("AccessKey() = %q, want %q", got, want)
	}
}

func TestAccessKeyRejectsUnsafeInstanceIDAndEmptyIdentity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		instance string
		bucket   string
		object   string
	}{
		{name: "empty instance", bucket: "b", object: "o"},
		{name: "namespace delimiter", instance: "a:b", bucket: "b", object: "o"},
		{name: "empty bucket", instance: "a", object: "o"},
		{name: "empty object", instance: "a", bucket: "b"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if _, err := AccessKey(tt.instance, tt.bucket, tt.object, time.Now()); err == nil {
				t.Fatal("AccessKey() error = nil, want validation error")
			}
		})
	}
}

func TestHourWindowUsesCompletedUTCHours(t *testing.T) {
	t.Parallel()

	evaluation := time.Date(2026, 8, 25, 15, 37, 0, 0, time.UTC)
	got, err := HourWindow(evaluation, 3, false)
	if err != nil {
		t.Fatalf("HourWindow() error = %v", err)
	}
	want := []time.Time{
		time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC),
		time.Date(2026, 8, 25, 13, 0, 0, 0, time.UTC),
		time.Date(2026, 8, 25, 14, 0, 0, 0, time.UTC),
	}
	assertHours(t, got, want)
}

func TestHourWindowCanIncludeCurrentHour(t *testing.T) {
	t.Parallel()

	evaluation := time.Date(2026, 8, 25, 15, 37, 0, 0, time.UTC)
	got, err := HourWindow(evaluation, 3, true)
	if err != nil {
		t.Fatalf("HourWindow() error = %v", err)
	}
	want := []time.Time{
		time.Date(2026, 8, 25, 13, 0, 0, 0, time.UTC),
		time.Date(2026, 8, 25, 14, 0, 0, 0, time.UTC),
		time.Date(2026, 8, 25, 15, 0, 0, 0, time.UTC),
	}
	assertHours(t, got, want)
}

func TestCoverageTemplateMustVaryByUTCHour(t *testing.T) {
	t.Parallel()

	if err := ValidateCoverageTemplate("coverage:constant"); err == nil {
		t.Fatal("ValidateCoverageTemplate() error = nil, want invariant-template error")
	}
	if err := ValidateCoverageTemplate("coverage:03-PM"); err == nil {
		t.Fatal("ValidateCoverageTemplate() error = nil, want template that collides across hours to fail")
	}
	if err := ValidateCoverageTemplate("coverage:2006:01:02:15"); err != nil {
		t.Fatalf("ValidateCoverageTemplate() error = %v", err)
	}

	instant := time.Date(2026, 8, 25, 17, 0, 0, 0, time.FixedZone("UTC+3", 3*60*60))
	if got := CoverageKey("coverage:2006:01:02:15", instant); got != "coverage:2026:08:25:14" {
		t.Fatalf("CoverageKey() = %q", got)
	}
}

func TestRetentionMustExceedLongestWindowPlusOneHour(t *testing.T) {
	t.Parallel()

	if err := ValidateRetention(5*time.Hour, 2*time.Hour, 4*time.Hour); err == nil {
		t.Fatal("ValidateRetention() error = nil at boundary")
	}
	if err := ValidateRetention(5*time.Hour+time.Second, 2*time.Hour, 4*time.Hour); err != nil {
		t.Fatalf("ValidateRetention() error = %v above boundary", err)
	}
}

func TestCounterExpiryIsRetentionAfterEndOfUTCEventHour(t *testing.T) {
	t.Parallel()

	eventTime := time.Date(2026, 8, 25, 17, 59, 0, 0, time.FixedZone("UTC+3", 3*60*60))
	want := time.Date(2026, 8, 26, 15, 0, 0, 0, time.UTC)
	if got := CounterExpiry(eventTime, 24*time.Hour); !got.Equal(want) {
		t.Fatalf("CounterExpiry() = %s, want %s", got, want)
	}
}

func TestValidateCounterRetentionRejectsFractionalSecondExpiry(t *testing.T) {
	t.Parallel()

	if err := ValidateCounterRetention(24*time.Hour + time.Millisecond); err == nil {
		t.Fatal("ValidateCounterRetention() error = nil for fractional-second EXPIREAT retention")
	}
	if err := ValidateCounterRetention(24*time.Hour + time.Second); err != nil {
		t.Fatalf("ValidateCounterRetention() error = %v for whole-second retention", err)
	}
}

func assertHours(t *testing.T, got, want []time.Time) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("len(HourWindow()) = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if !got[i].Equal(want[i]) {
			t.Errorf("HourWindow()[%d] = %s, want %s", i, got[i], want[i])
		}
	}
}
