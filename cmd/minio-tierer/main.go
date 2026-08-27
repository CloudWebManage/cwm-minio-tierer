package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/orihoch/cwm-minio-tierer/internal/logging"
	"github.com/orihoch/cwm-minio-tierer/internal/tierer"
)

func main() {
	level, err := logging.ParseLevel(os.Getenv("LOG_LEVEL"))
	logger := logging.NewJSONLogger(os.Stdout, level)
	if err != nil {
		logger.Error("invalid logging configuration", "error", err)
		os.Exit(2)
	}
	config, err := tierer.LoadConfig(os.LookupEnv)
	if err != nil {
		logger.Error("invalid MinIO tierer configuration", "error", err)
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := tierer.Run(ctx, config, logger); err != nil {
		logger.Error("MinIO tierer stopped with an error", "error", err)
		os.Exit(1)
	}
}
