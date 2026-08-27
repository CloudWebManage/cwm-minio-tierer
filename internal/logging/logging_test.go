package logging

import (
	"log/slog"
	"testing"
)

func TestParseLevel(t *testing.T) {
	t.Parallel()
	tests := map[string]slog.Level{
		"":      slog.LevelInfo,
		"debug": slog.LevelDebug,
		"info":  slog.LevelInfo,
		"warn":  slog.LevelWarn,
		"error": slog.LevelError,
	}
	for raw, want := range tests {
		t.Run(raw, func(t *testing.T) {
			t.Parallel()
			got, err := ParseLevel(raw)
			if err != nil || got != want {
				t.Fatalf("ParseLevel(%q) = %v, %v, want %v", raw, got, err, want)
			}
		})
	}
	if _, err := ParseLevel("DEBUG"); err == nil {
		t.Fatal("ParseLevel() error = nil for invalid level")
	}
}
