#!/usr/bin/env bash
# ==============================================================================
# walspool — OpenTelemetry (OTLP/HTTP) Ingestion & Query Demo
# ==============================================================================
# Demonstrates:
# 1. Ingesting standard OpenTelemetry OTLP JSON logs into Walspool (POST /v1/logs)
# 2. Querying indexed entries by Trace ID (< 15µs) (GET /v1/logs?trace_id=...)
# 3. Querying indexed entries by Service & Severity (GET /v1/logs?service=...&level=...)
# ==============================================================================

set -euo pipefail

ENDPOINT="${1:-http://127.0.0.1:9099}"
TRACE_ID="4bf92f3577b34da6a3ce929d0e0e4736"
SPAN_ID="00f067aa0ba902b7"
NOW_NANO=$(date +%s%N 2>/dev/null || python3 -c 'import time; print(int(time.time()*1e9))')

echo "▲ [1/3] Ingesting OpenTelemetry OTLP log into Walspool at ${ENDPOINT}/v1/logs..."

curl -s -S -X POST "${ENDPOINT}/v1/logs" \
  -H "Content-Type: application/json" \
  -d @- <<EOF
{
  "resourceLogs": [
    {
      "resource": {
        "attributes": [
          { "key": "service.name", "value": { "stringValue": "checkout-service" } },
          { "key": "deployment.environment", "value": { "stringValue": "production" } }
        ]
      },
      "scopeLogs": [
        {
          "scope": { "name": "order-processor", "version": "v1.4.2" },
          "logRecords": [
            {
              "timeUnixNano": "${NOW_NANO}",
              "severityNumber": 9,
              "severityText": "INFO",
              "body": { "stringValue": "Payment authorized for order #994812" },
              "traceId": "${TRACE_ID}",
              "spanId": "${SPAN_ID}",
              "attributes": [
                { "key": "customer_id", "value": { "stringValue": "cust_4812" } },
                { "key": "amount_eur", "value": { "doubleValue": 149.99 } }
              ]
            },
            {
              "timeUnixNano": "${NOW_NANO}",
              "severityNumber": 17,
              "severityText": "ERROR",
              "body": { "stringValue": "Inventory sync failed: downstream timeout after 5000ms" },
              "traceId": "${TRACE_ID}",
              "spanId": "${SPAN_ID}",
              "attributes": [
                { "key": "sku", "value": { "stringValue": "SKU-9921" } },
                { "key": "retry_attempt", "value": { "intValue": 3 } }
              ]
            }
          ]
        }
      ]
    }
  ]
}
EOF

echo ""
echo "✔ Successfully ingested into NVMe WAL and MemoryLogHub."
echo ""

echo "▲ [2/3] Querying logs by Trace ID: ${TRACE_ID}..."
curl -s -S "${ENDPOINT}/v1/logs?trace_id=${TRACE_ID}" | jq . || curl -s -S "${ENDPOINT}/v1/logs?trace_id=${TRACE_ID}"

echo ""
echo "▲ [3/3] Querying logs by Service (checkout-service) & Level (ERROR)..."
curl -s -S "${ENDPOINT}/v1/logs?service=checkout-service&level=ERROR" | jq . || curl -s -S "${ENDPOINT}/v1/logs?service=checkout-service&level=ERROR"

echo ""
echo "✔ Done. To observe live stream in real time, run:"
echo "  curl -N \"${ENDPOINT}/v1/logs/stream?service=checkout-service\""
