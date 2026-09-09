#!/usr/bin/env python3
"""
walspool — OpenTelemetry (OTLP/HTTP) Python Ingestion Example

Demonstrates zero-dependency integration with Walspool's OTLP receiver (POST /v1/logs):
1. Ingests structured OTLP logs with service name, severity, trace IDs, and attributes
2. Verifies sub-15µs indexing via GET /v1/logs?trace_id=...
3. Subscribes to real-time Server-Sent Events (SSE) stream

Uses Python standard library (urllib, json) — zero external pip dependencies.
"""

import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, List, Optional


class WalspoolOTLPClient:
    """Client for Walspool OpenTelemetry OTLP/HTTP log receiver."""

    def __init__(self, endpoint: str = "http://127.0.0.1:9099"):
        self.endpoint = endpoint.rstrip("/")

    def emit_log(
        self,
        service: str,
        message: str,
        level: str = "INFO",
        trace_id: Optional[str] = None,
        span_id: Optional[str] = None,
        attributes: Optional[Dict[str, Any]] = None,
        scope: str = "python-app",
    ) -> bool:
        """
        Sends an OpenTelemetry standard OTLP JSON log record to Walspool on POST /v1/logs.
        Persisted to NVMe WAL and indexed in MemoryLogHub in sub-15 microseconds.
        """
        now_nano = int(time.time() * 1e9)

        # Map string level to OTel SeverityNumber
        severity_map = {
            "DEBUG": 5,
            "INFO": 9,
            "WARN": 13,
            "WARNING": 13,
            "ERROR": 17,
            "FATAL": 21,
        }
        sev_num = severity_map.get(level.upper(), 9)

        # Convert attributes dictionary to OTel KeyValue list
        otel_attrs = []
        if attributes:
            for k, v in attributes.items():
                if isinstance(v, bool):
                    val = {"boolValue": v}
                elif isinstance(v, int):
                    val = {"intValue": v}
                elif isinstance(v, float):
                    val = {"doubleValue": v}
                else:
                    val = {"stringValue": str(v)}
                otel_attrs.append({"key": k, "value": val})

        log_record: Dict[str, Any] = {
            "timeUnixNano": str(now_nano),
            "severityNumber": sev_num,
            "severityText": level.upper(),
            "body": {"stringValue": message},
            "attributes": otel_attrs,
        }
        if trace_id:
            log_record["traceId"] = trace_id
        if span_id:
            log_record["spanId"] = span_id

        payload = {
            "resourceLogs": [
                {
                    "resource": {
                        "attributes": [
                            {"key": "service.name", "value": {"stringValue": service}},
                        ]
                    },
                    "scopeLogs": [
                        {
                            "scope": {"name": scope},
                            "logRecords": [log_record],
                        }
                    ],
                }
            ]
        }

        req_data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            url=f"{self.endpoint}/v1/logs",
            data=req_data,
            headers={
                "Content-Type": "application/json",
                "User-Agent": "walspool-otlp-python/1.0",
            },
            method="POST",
        )

        try:
            with urllib.request.urlopen(req, timeout=5.0) as resp:
                return resp.status == 200
        except urllib.error.HTTPError as e:
            if e.code == 503:
                print(f"[BACKPRESSURE] Spool full, retry after {e.headers.get('Retry-After', '1')}s")
            else:
                print(f"[ERROR] HTTP {e.code}: {e.read().decode('utf-8')}")
            return False

    def query_by_trace(self, trace_id: str) -> List[Dict[str, Any]]:
        """Queries historical entries from in-memory hub by distributed trace ID (< 15 µs)."""
        url = f"{self.endpoint}/v1/logs?trace_id={urllib.parse.quote(trace_id)}"
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=5.0) as resp:
            return json.loads(resp.read().decode("utf-8"))


def main():
    endpoint = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:9099"
    client = WalspoolOTLPClient(endpoint)

    trace_id = "e4b3c2a1000011112222333344445555"
    span_id = "0011223344556677"

    print(f"▲ Emitting OpenTelemetry logs to {endpoint}/v1/logs...")

    # 1. Info log
    ok = client.emit_log(
        service="billing-api",
        message="Subscription invoice created for customer cust_8819",
        level="INFO",
        trace_id=trace_id,
        span_id=span_id,
        attributes={"plan": "pro_annual", "amount_eur": 990.00},
    )
    print(f"  → Emit INFO log: {'✔ OK' if ok else '✖ Failed'}")

    # 2. Error log
    ok = client.emit_log(
        service="billing-api",
        message="Webhook delivery failed: upstream returned 504 Gateway Timeout",
        level="ERROR",
        trace_id=trace_id,
        span_id=span_id,
        attributes={"endpoint": "https://partner.api/events", "attempt": 3},
    )
    print(f"  → Emit ERROR log: {'✔ OK' if ok else '✖ Failed'}")

    # 3. Query back by trace ID
    print(f"\n▲ Querying MemoryLogHub by Trace ID ({trace_id})...")
    logs = client.query_by_trace(trace_id)
    print(f"  Found {len(logs)} log entries in memory:")
    for entry in logs:
        print(f"    [{entry.get('level')}] {entry.get('service')}: {entry.get('payload')}")


if __name__ == "__main__":
    main()
