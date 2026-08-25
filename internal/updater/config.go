package updater

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/orihoch/cwm-minio-tierer/internal/contracts"
)

type Config struct {
	InstanceID      string
	AccessRetention time.Duration

	ListenAddress string
	RedisAddress  string
	RedisUsername string
	RedisPassword string
	RedisDB       int
	RedisTLS      bool

	SuccessStatus int
	FailureStatus int
	DataLossRisk  bool
	DuplicateRisk bool

	MaxBodyBytes int64
	MaxRecords   int
	QueueSize    int

	BatchMaxEvents int
	BatchMaxKeys   int
	BatchMaxWait   time.Duration

	RedisOperationTimeout time.Duration
	ShutdownTimeout       time.Duration
	ReadHeaderTimeout     time.Duration
	ReadTimeout           time.Duration
	WriteTimeout          time.Duration
	IdleTimeout           time.Duration
	MaxHeaderBytes        int
}

type envLookup func(string) (string, bool)

func LoadConfig(lookup envLookup) (Config, error) {
	var config Config
	var err error
	config.InstanceID = value(lookup, "INSTANCE_ID", "")
	if err := contracts.ValidateInstanceID(config.InstanceID); err != nil {
		return Config{}, fmt.Errorf("INSTANCE_ID: %w", err)
	}
	if config.AccessRetention, err = durationValue(lookup, "ACCESS_RETENTION", "", true); err != nil {
		return Config{}, err
	}
	if err := contracts.ValidateCounterRetention(config.AccessRetention); err != nil {
		return Config{}, fmt.Errorf("ACCESS_RETENTION: %w", err)
	}

	config.ListenAddress = value(lookup, "UPDATER_LISTEN_ADDR", ":8080")
	config.RedisAddress = value(lookup, "REDIS_ADDR", "127.0.0.1:6379")
	if err := validateAddress("UPDATER_LISTEN_ADDR", config.ListenAddress, true); err != nil {
		return Config{}, err
	}
	if err := validateAddress("REDIS_ADDR", config.RedisAddress, false); err != nil {
		return Config{}, err
	}
	config.RedisUsername = value(lookup, "REDIS_USERNAME", "")
	config.RedisPassword = value(lookup, "REDIS_PASSWORD", "")
	if config.RedisDB, err = intValue(lookup, "REDIS_DB", 0, 0, 1_000_000); err != nil {
		return Config{}, err
	}
	if config.RedisTLS, err = boolValue(lookup, "REDIS_TLS", false); err != nil {
		return Config{}, err
	}

	if config.SuccessStatus, err = intValue(lookup, "UPDATER_SUCCESS_STATUS", 200, 200, 599); err != nil {
		return Config{}, err
	}
	if config.FailureStatus, err = intValue(lookup, "UPDATER_FAILURE_STATUS", 500, 200, 599); err != nil {
		return Config{}, err
	}
	if config.DataLossRisk, err = boolValue(lookup, "UPDATER_ACCEPT_DATA_LOSS", false); err != nil {
		return Config{}, err
	}
	if config.DuplicateRisk, err = boolValue(lookup, "UPDATER_ACCEPT_DUPLICATE_RISK", false); err != nil {
		return Config{}, err
	}
	if config.FailureStatus < 500 && !config.DataLossRisk {
		return Config{}, errors.New("UPDATER_ACCEPT_DATA_LOSS=true is required for a non-5xx failure status")
	}
	if (config.SuccessStatus < 200 || config.SuccessStatus >= 300) && !config.DuplicateRisk {
		return Config{}, errors.New("UPDATER_ACCEPT_DUPLICATE_RISK=true is required for a non-2xx success status")
	}
	config.DataLossRisk = config.FailureStatus < 500 && config.DataLossRisk
	config.DuplicateRisk = (config.SuccessStatus < 200 || config.SuccessStatus >= 300) && config.DuplicateRisk

	if config.MaxBodyBytes, err = int64Value(lookup, "UPDATER_MAX_BODY_BYTES", 1<<20, 1, 1<<30); err != nil {
		return Config{}, err
	}
	if config.MaxRecords, err = intValue(lookup, "UPDATER_MAX_RECORDS", 1000, 1, 1_000_000); err != nil {
		return Config{}, err
	}
	if config.QueueSize, err = intValue(lookup, "UPDATER_QUEUE_SIZE", 128, 1, 1_000_000); err != nil {
		return Config{}, err
	}
	if config.BatchMaxEvents, err = intValue(lookup, "UPDATER_BATCH_MAX_EVENTS", 5000, 1, 1_000_000); err != nil {
		return Config{}, err
	}
	if config.BatchMaxKeys, err = intValue(lookup, "UPDATER_BATCH_MAX_KEYS", 5000, 1, 1_000_000); err != nil {
		return Config{}, err
	}
	if config.BatchMaxEvents < config.MaxRecords || config.BatchMaxKeys < config.MaxRecords {
		return Config{}, errors.New("one request must fit within both batch event and unique-key limits")
	}
	if config.BatchMaxWait, err = durationValue(lookup, "UPDATER_BATCH_MAX_WAIT", "50ms", false); err != nil {
		return Config{}, err
	}

	if config.RedisOperationTimeout, err = durationValue(lookup, "REDIS_OPERATION_TIMEOUT", "5s", false); err != nil {
		return Config{}, err
	}
	if config.ShutdownTimeout, err = durationValue(lookup, "UPDATER_SHUTDOWN_TIMEOUT", "30s", false); err != nil {
		return Config{}, err
	}
	if config.ReadHeaderTimeout, err = durationValue(lookup, "UPDATER_READ_HEADER_TIMEOUT", "5s", false); err != nil {
		return Config{}, err
	}
	if config.ReadTimeout, err = durationValue(lookup, "UPDATER_READ_TIMEOUT", "15s", false); err != nil {
		return Config{}, err
	}
	if config.IdleTimeout, err = durationValue(lookup, "UPDATER_IDLE_TIMEOUT", "60s", false); err != nil {
		return Config{}, err
	}
	if config.MaxHeaderBytes, err = intValue(lookup, "UPDATER_MAX_HEADER_BYTES", 1<<20, 1024, 1<<24); err != nil {
		return Config{}, err
	}
	minimumWriteTimeout, err := worstCaseAcknowledgementTime(config)
	if err != nil {
		return Config{}, err
	}
	if _, configured := lookup("UPDATER_WRITE_TIMEOUT"); configured {
		if config.WriteTimeout, err = durationValue(lookup, "UPDATER_WRITE_TIMEOUT", "", true); err != nil {
			return Config{}, err
		}
	} else {
		if minimumWriteTimeout > time.Duration(1<<63-1)-time.Second {
			return Config{}, errors.New("safe default UPDATER_WRITE_TIMEOUT overflows time.Duration")
		}
		config.WriteTimeout = minimumWriteTimeout + time.Second
	}
	if config.WriteTimeout <= minimumWriteTimeout {
		return Config{}, fmt.Errorf("UPDATER_WRITE_TIMEOUT must exceed request read budget plus queue backlog: %s + (%d+1) * (%s + %s) = %s",
			config.ReadTimeout, config.QueueSize, config.BatchMaxWait, config.RedisOperationTimeout, minimumWriteTimeout)
	}
	if config.ShutdownTimeout < config.RedisOperationTimeout {
		return Config{}, errors.New("UPDATER_SHUTDOWN_TIMEOUT must not be shorter than Redis operation timeout")
	}
	return config, nil
}

// worstCaseAcknowledgementTime covers request-body reading plus one active
// batch and every queue position. Each request may require its own batch.
func worstCaseAcknowledgementTime(config Config) (time.Duration, error) {
	perBatch := config.BatchMaxWait + config.RedisOperationTimeout
	if perBatch < config.BatchMaxWait {
		return 0, errors.New("batch wait plus Redis operation timeout overflows time.Duration")
	}
	batchCount := int64(config.QueueSize) + 1
	if int64(perBatch) > int64(time.Duration(1<<63-1))/batchCount {
		return 0, errors.New("queue backlog acknowledgement budget overflows time.Duration")
	}
	backlog := time.Duration(batchCount * int64(perBatch))
	if backlog > time.Duration(1<<63-1)-config.ReadTimeout {
		return 0, errors.New("request read plus queue backlog budget overflows time.Duration")
	}
	return config.ReadTimeout + backlog, nil
}

func value(lookup envLookup, key, fallback string) string {
	if raw, ok := lookup(key); ok {
		return raw
	}
	return fallback
}

func boolValue(lookup envLookup, key string, fallback bool) (bool, error) {
	raw, ok := lookup(key)
	if !ok {
		return fallback, nil
	}
	switch raw {
	case "true":
		return true, nil
	case "false":
		return false, nil
	default:
		return false, fmt.Errorf("%s must be exactly true or false", key)
	}
}

func intValue(lookup envLookup, key string, fallback, minimum, maximum int) (int, error) {
	raw, ok := lookup(key)
	if !ok {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(raw)
	if err != nil || parsed < minimum || parsed > maximum {
		return 0, fmt.Errorf("%s must be an integer between %d and %d", key, minimum, maximum)
	}
	return parsed, nil
}

func int64Value(lookup envLookup, key string, fallback, minimum, maximum int64) (int64, error) {
	raw, ok := lookup(key)
	if !ok {
		return fallback, nil
	}
	parsed, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || parsed < minimum || parsed > maximum {
		return 0, fmt.Errorf("%s must be an integer between %d and %d", key, minimum, maximum)
	}
	return parsed, nil
}

func durationValue(lookup envLookup, key, fallback string, required bool) (time.Duration, error) {
	raw, ok := lookup(key)
	if !ok {
		raw = fallback
	}
	if required && raw == "" {
		return 0, fmt.Errorf("%s is required", key)
	}
	parsed, err := time.ParseDuration(raw)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("%s must be a positive Go duration", key)
	}
	return parsed, nil
}

func validateAddress(key, address string, allowEmptyHost bool) error {
	if address == "" || strings.TrimSpace(address) != address {
		return fmt.Errorf("%s must be a host:port address", key)
	}
	host, port, err := net.SplitHostPort(address)
	if err != nil || (!allowEmptyHost && host == "") {
		return fmt.Errorf("%s must be a host:port address", key)
	}
	parsedPort, err := strconv.Atoi(port)
	if err != nil || parsedPort < 1 || parsedPort > 65535 {
		return fmt.Errorf("%s port must be between 1 and 65535", key)
	}
	return nil
}
