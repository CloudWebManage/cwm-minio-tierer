# syntax=docker/dockerfile:1

FROM golang:1.24.5-alpine3.22@sha256:daae04ebad0c21149979cd8e9db38f565ecefd8547cf4a591240dc1972cf1399 AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download && go mod verify
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 GOOS=linux go build -buildvcs=false -mod=readonly -trimpath -ldflags="-s -w -buildid=" -o /out/redis-updater ./cmd/redis-updater && \
    CGO_ENABLED=0 GOOS=linux go build -buildvcs=false -mod=readonly -trimpath -ldflags="-s -w -buildid=" -o /out/minio-tierer ./cmd/minio-tierer

FROM build AS integration-tests
CMD ["go", "test", "-v", "-count=1", "-tags=integration", "./internal/integration"]

FROM alpine:3.22.1@sha256:4bcff63911fcb4448bd4fdacec207030997caf25e9bea4045fa6c8c44de311d1 AS runtime

LABEL org.opencontainers.image.title="cwm-minio-tierer" \
      org.opencontainers.image.description="Redis access updater and policy-driven MinIO tierer" \
      org.opencontainers.image.source="https://github.com/orihoch/cwm-minio-tierer"

COPY --from=build --chmod=0555 /out/redis-updater /usr/local/bin/redis-updater
COPY --from=build --chmod=0555 /out/minio-tierer /usr/local/bin/minio-tierer

USER 65532:65532
EXPOSE 8080 8081
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
  CMD wget -q -T 1 -O /dev/null http://127.0.0.1:8081/livez || \
      wget -q -T 1 -O /dev/null http://127.0.0.1:8080/livez || exit 1
CMD ["/usr/local/bin/minio-tierer"]
