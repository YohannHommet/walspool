# Build Stage
FROM --platform=$BUILDPLATFORM golang:1.23-alpine AS builder
ARG TARGETOS
ARG TARGETARCH
ARG VERSION=1.0.0
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
COPY cmd/ ./cmd/
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH:-amd64} go build -ldflags="-s -w -extldflags '-static' -X main.Version=${VERSION}" -o /walspool-sidecar ./cmd/sidecar

# Production Stage
FROM alpine:3.19
RUN apk --no-cache add ca-certificates tzdata && \
    addgroup -g 10001 -S walspool && \
    adduser -u 10001 -S walspool -G walspool && \
    mkdir -p /data/spool && \
    chown -R walspool:walspool /data/spool

WORKDIR /
COPY --from=builder /walspool-sidecar /usr/local/bin/walspool-sidecar

# Spool directory volume
VOLUME /data/spool

ENV WALSPOOL_ADDR=":9099" \
    WALSPOOL_DATA_DIR="/data/spool" \
    WALSPOOL_BATCH_SIZE="50" \
    WALSPOOL_FLUSH_MS="50" \
    WALSPOOL_LOG_FORMAT="json" \
    WALSPOOL_LOG_LEVEL="info"

USER 10001:10001
EXPOSE 9099
HEALTHCHECK --interval=10s --timeout=2s --retries=3 CMD wget -q -O- http://127.0.0.1:9099/readyz || exit 1
ENTRYPOINT ["/usr/local/bin/walspool-sidecar"]
