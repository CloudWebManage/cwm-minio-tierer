package updater

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"unicode/utf8"

	"github.com/orihoch/cwm-minio-tierer/internal/contracts"
)

var (
	ErrUnsupportedMediaType = errors.New("unsupported media type")
	ErrBodyTooLarge         = errors.New("request body too large")
	ErrTooManyRecords       = errors.New("too many records")
	ErrInvalidBody          = errors.New("invalid request body")
)

type DecodeLimits struct {
	MaxBodyBytes int64
	MaxRecords   int
}

func DecodeRequest(body io.Reader, contentType string, limits DecodeLimits) ([]contracts.Access, error) {
	if limits.MaxBodyBytes <= 0 || limits.MaxRecords <= 0 {
		return nil, fmt.Errorf("%w: decoding limits must be positive", ErrInvalidBody)
	}
	mediaType, err := validateContentType(contentType)
	if err != nil {
		return nil, err
	}

	data, err := io.ReadAll(io.LimitReader(body, limits.MaxBodyBytes+1))
	if err != nil {
		return nil, fmt.Errorf("%w: read body: %v", ErrInvalidBody, err)
	}
	if int64(len(data)) > limits.MaxBodyBytes {
		return nil, ErrBodyTooLarge
	}
	if len(data) == 0 || !utf8.Valid(data) {
		return nil, ErrInvalidBody
	}

	if mediaType == "application/json" {
		record, err := decodeObject(data)
		if err != nil {
			return nil, err
		}
		return []contracts.Access{record}, nil
	}
	return decodeNDJSON(data, limits.MaxRecords)
}

func validateContentType(contentType string) (string, error) {
	mediaType, params, err := mime.ParseMediaType(contentType)
	if err != nil || len(params) != 0 || (mediaType != "application/json" && mediaType != "application/x-ndjson") {
		return "", ErrUnsupportedMediaType
	}
	return mediaType, nil
}

func decodeNDJSON(data []byte, maxRecords int) ([]contracts.Access, error) {
	lines := bytes.Split(data, []byte{'\n'})
	if len(lines) > 0 && len(lines[len(lines)-1]) == 0 {
		lines = lines[:len(lines)-1]
	}
	if len(lines) == 0 {
		return nil, ErrInvalidBody
	}
	if len(lines) > maxRecords {
		return nil, ErrTooManyRecords
	}
	records := make([]contracts.Access, 0, len(lines))
	for _, line := range lines {
		line = bytes.TrimSuffix(line, []byte{'\r'})
		if len(bytes.TrimSpace(line)) == 0 {
			return nil, ErrInvalidBody
		}
		record, err := decodeObject(line)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, nil
}

func decodeObject(data []byte) (contracts.Access, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	start, err := decoder.Token()
	if err != nil || start != json.Delim('{') {
		return contracts.Access{}, ErrInvalidBody
	}

	var record contracts.Access
	seen := make(map[string]struct{}, 2)
	for decoder.More() {
		fieldToken, err := decoder.Token()
		if err != nil {
			return contracts.Access{}, ErrInvalidBody
		}
		field, ok := fieldToken.(string)
		if !ok {
			return contracts.Access{}, ErrInvalidBody
		}
		if _, duplicate := seen[field]; duplicate {
			return contracts.Access{}, fmt.Errorf("%w: duplicate field %q", ErrInvalidBody, field)
		}
		seen[field] = struct{}{}
		if field != "bucket" && field != "object" {
			return contracts.Access{}, fmt.Errorf("%w: unknown field %q", ErrInvalidBody, field)
		}
		valueToken, err := decoder.Token()
		if err != nil {
			return contracts.Access{}, ErrInvalidBody
		}
		value, ok := valueToken.(string)
		if !ok || value == "" {
			return contracts.Access{}, fmt.Errorf("%w: %s must be a non-empty string", ErrInvalidBody, field)
		}
		if field == "bucket" {
			record.Bucket = value
		} else {
			record.Object = value
		}
	}
	if end, err := decoder.Token(); err != nil || end != json.Delim('}') {
		return contracts.Access{}, ErrInvalidBody
	}
	if record.Bucket == "" || record.Object == "" {
		return contracts.Access{}, ErrInvalidBody
	}
	if token, err := decoder.Token(); err != io.EOF || token != nil {
		return contracts.Access{}, ErrInvalidBody
	}
	return record, nil
}

func RejectionReason(err error) string {
	switch {
	case errors.Is(err, ErrUnsupportedMediaType):
		return "unsupported_media_type"
	case errors.Is(err, ErrBodyTooLarge):
		return "body_too_large"
	case errors.Is(err, ErrTooManyRecords):
		return "too_many_records"
	default:
		return "invalid_body"
	}
}
