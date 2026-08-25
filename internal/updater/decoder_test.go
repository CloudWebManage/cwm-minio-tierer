package updater

import (
	"errors"
	"strings"
	"testing"
)

func TestDecodeRequestAcceptsStrictJSON(t *testing.T) {
	t.Parallel()

	records, err := DecodeRequest(strings.NewReader(`{"bucket":"photos","object":"2026/a.jpg"}`), "application/json", DecodeLimits{MaxBodyBytes: 1024, MaxRecords: 10})
	if err != nil {
		t.Fatalf("DecodeRequest() error = %v", err)
	}
	if len(records) != 1 || records[0].Bucket != "photos" || records[0].Object != "2026/a.jpg" {
		t.Fatalf("DecodeRequest() records = %#v", records)
	}
}

func TestDecodeRequestAcceptsNDJSONWithTerminalNewline(t *testing.T) {
	t.Parallel()

	body := "{\"bucket\":\"b1\",\"object\":\"o1\"}\r\n{\"bucket\":\"b2\",\"object\":\"o2\"}\n"
	records, err := DecodeRequest(strings.NewReader(body), "application/x-ndjson", DecodeLimits{MaxBodyBytes: 1024, MaxRecords: 2})
	if err != nil {
		t.Fatalf("DecodeRequest() error = %v", err)
	}
	if len(records) != 2 || records[1].Bucket != "b2" || records[1].Object != "o2" {
		t.Fatalf("DecodeRequest() records = %#v", records)
	}
}

func TestDecodeRequestRejectsInvalidContracts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		contentType string
		body        string
		maxBytes    int64
		maxRecords  int
		wantError   error
	}{
		{name: "unsupported media", contentType: "text/plain", body: `{}`, maxBytes: 100, maxRecords: 1, wantError: ErrUnsupportedMediaType},
		{name: "media parameters", contentType: "application/json; charset=utf-8", body: `{}`, maxBytes: 100, maxRecords: 1, wantError: ErrUnsupportedMediaType},
		{name: "empty body", contentType: "application/json", body: "", maxBytes: 100, maxRecords: 1, wantError: ErrInvalidBody},
		{name: "empty bucket", contentType: "application/json", body: `{"bucket":"","object":"o"}`, maxBytes: 100, maxRecords: 1, wantError: ErrInvalidBody},
		{name: "non-string", contentType: "application/json", body: `{"bucket":"b","object":4}`, maxBytes: 100, maxRecords: 1, wantError: ErrInvalidBody},
		{name: "unknown field", contentType: "application/json", body: `{"bucket":"b","object":"o","extra":true}`, maxBytes: 100, maxRecords: 1, wantError: ErrInvalidBody},
		{name: "duplicate field", contentType: "application/json", body: `{"bucket":"b","bucket":"b2","object":"o"}`, maxBytes: 100, maxRecords: 1, wantError: ErrInvalidBody},
		{name: "trailing JSON", contentType: "application/json", body: `{"bucket":"b","object":"o"} {}`, maxBytes: 100, maxRecords: 1, wantError: ErrInvalidBody},
		{name: "blank NDJSON record", contentType: "application/x-ndjson", body: "{\"bucket\":\"b\",\"object\":\"o\"}\n \n{\"bucket\":\"b\",\"object\":\"p\"}", maxBytes: 200, maxRecords: 3, wantError: ErrInvalidBody},
		{name: "too many records", contentType: "application/x-ndjson", body: "{\"bucket\":\"b\",\"object\":\"o\"}\n{\"bucket\":\"b\",\"object\":\"p\"}", maxBytes: 200, maxRecords: 1, wantError: ErrTooManyRecords},
		{name: "body too large", contentType: "application/json", body: `{"bucket":"b","object":"o"}`, maxBytes: 10, maxRecords: 1, wantError: ErrBodyTooLarge},
		{name: "malformed utf8", contentType: "application/json", body: "{\"bucket\":\"b\",\"object\":\"\xff\"}", maxBytes: 100, maxRecords: 1, wantError: ErrInvalidBody},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := DecodeRequest(strings.NewReader(tt.body), tt.contentType, DecodeLimits{MaxBodyBytes: tt.maxBytes, MaxRecords: tt.maxRecords})
			if !errors.Is(err, tt.wantError) {
				t.Fatalf("DecodeRequest() error = %v, want %v", err, tt.wantError)
			}
		})
	}
}
