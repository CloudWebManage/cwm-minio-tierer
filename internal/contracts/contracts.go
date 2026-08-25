package contracts

import (
	"encoding/base64"
	"errors"
	"fmt"
	"regexp"
	"time"
)

const Namespace = "cwm-minio-tierer:v1"

var instancePattern = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)

type Access struct {
	Bucket string
	Object string
}

func ValidateInstanceID(instance string) error {
	if !instancePattern.MatchString(instance) {
		return errors.New("instance ID must contain only ASCII letters, digits, dot, underscore, or hyphen")
	}
	return nil
}

func EncodeIdentity(value string) (string, error) {
	if value == "" {
		return "", errors.New("identity component must not be empty")
	}
	return base64.RawURLEncoding.EncodeToString([]byte(value)), nil
}

func AccessKey(instance, bucket, object string, hour time.Time) (string, error) {
	if err := ValidateInstanceID(instance); err != nil {
		return "", err
	}
	encodedBucket, err := EncodeIdentity(bucket)
	if err != nil {
		return "", fmt.Errorf("bucket: %w", err)
	}
	encodedObject, err := EncodeIdentity(object)
	if err != nil {
		return "", fmt.Errorf("object: %w", err)
	}
	return fmt.Sprintf("%s:%s:access:%s:%s:%s:%s:%s:%s", Namespace, instance,
		hour.UTC().Format("2006"), hour.UTC().Format("01"), hour.UTC().Format("02"),
		hour.UTC().Format("15"), encodedBucket, encodedObject), nil
}

func HourWindow(evaluation time.Time, hours int, includeCurrent bool) ([]time.Time, error) {
	if hours <= 0 {
		return nil, errors.New("window hours must be positive")
	}
	end := evaluation.UTC().Truncate(time.Hour)
	if includeCurrent {
		end = end.Add(time.Hour)
	}
	start := end.Add(-time.Duration(hours) * time.Hour)
	result := make([]time.Time, hours)
	for i := range result {
		result[i] = start.Add(time.Duration(i) * time.Hour)
	}
	return result, nil
}

func ValidateCoverageTemplate(template string) error {
	if template == "" {
		return errors.New("coverage template must not be empty")
	}
	reference := time.Date(2001, 2, 3, 4, 0, 0, 0, time.UTC)
	rendered := make(map[string]struct{}, 24*400)
	for offset := 0; offset < 24*400; offset++ {
		key := reference.Add(time.Duration(offset) * time.Hour).Format(template)
		if _, duplicate := rendered[key]; duplicate {
			return errors.New("coverage template must render a distinct key for every UTC hour")
		}
		rendered[key] = struct{}{}
	}
	return nil
}

func CoverageKey(template string, hour time.Time) string {
	return hour.UTC().Format(template)
}

func CounterExpiry(eventHour time.Time, retention time.Duration) time.Time {
	return eventHour.UTC().Truncate(time.Hour).Add(time.Hour).Add(retention)
}

func ValidateCounterRetention(retention time.Duration) error {
	if retention <= 0 {
		return errors.New("counter retention must be positive")
	}
	if retention%time.Second != 0 {
		return errors.New("counter retention must use whole seconds for EXPIREAT")
	}
	return nil
}

func ValidateRetention(retention, lowWindow, highWindow time.Duration) error {
	if retention <= 0 || lowWindow <= 0 || highWindow <= 0 {
		return errors.New("retention and windows must be positive")
	}
	longest := max(lowWindow, highWindow)
	if retention <= longest+time.Hour {
		return errors.New("retention must exceed the longest window plus one hour")
	}
	return nil
}
