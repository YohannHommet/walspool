# Build Stage
FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY go.mod ./
COPY *.go ./
COPY cmd/ ./cmd/
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /walspool-sidecar ./cmd/sidecar

# Production Stage
FROM alpine:3.19
RUN apk --no-cache add ca-certificates
WORKDIR /
COPY --from=builder /walspool-sidecar /usr/local/bin/walspool-sidecar

# Spool directory volume
VOLUME /data/spool
ENV WALSPOOL_ADDR=":9099"
ENV WALSPOOL_DATA_DIR="/data/spool"
ENV WALSPOOL_BATCH_SIZE="50"
ENV WALSPOOL_FLUSH_MS="50"

EXPOSE 9099
ENTRYPOINT ["walspool-sidecar"]
