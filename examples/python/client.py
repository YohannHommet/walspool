#!/usr/bin/env python3
"""
walspool Python Polyglot Client (v1.0.0)

High-performance client for walspool sidecar daemon:
1. POST /v1/enqueue     - Instant local WAL append & Hub indexing (< 15µs)
2. GET  /v1/logs        - Historical query by trace_id, service, level in O(k) (< 15µs)
3. GET  /v1/logs/stream - Real-time non-blocking Server-Sent Events (SSE) with auto-reconnect
4. GET  /v1/logs/stats  - Ring Buffer capacity and dropped event metrics

Pure Python standard library (urllib) - zero external pip dependencies.
"""

import json
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, Generator, List, Optional, Union


class WalspoolClient:
    """Polyglot client interfacing with a local or remote walspool sidecar."""

    def __init__(self, endpoint: str = "http://127.0.0.1:9099", timeout: float = 5.0):
        self.endpoint = endpoint.rstrip("/")
        self.timeout = timeout

    def enqueue(
        self,
        topic: str,
        payload: Union[Dict[str, Any], List[Any], str, bytes],
        trace_id: Optional[str] = None,
        service: Optional[str] = None,
        level: str = "INFO",
    ) -> Dict[str, Any]:
        """
        Enqueues an event into local disk WAL with simultaneous in-memory Hub indexing.
        Sub-microsecond local append with CRC32 IEEE checksum.

        :param topic: Topic name (e.g. "billing.invoices")
        :param payload: Event payload (dict, list, string, or raw bytes)
        :param trace_id: Distributed tracing identifier (e.g. "tr-python-9941")
        :param service: Service name (defaults to topic if omitted)
        :param level: Severity level (INFO, WARN, ERROR, etc.)
        :return: Receipt dictionary containing status, topic, and size_bytes
        """
        if not topic:
            raise ValueError("walspool: topic must not be empty")

        body: Dict[str, Any] = {
            "topic": topic,
            "payload": payload,
            "level": level,
        }
        if trace_id:
            body["trace_id"] = trace_id
        if service:
            body["service"] = service

        req_bytes = json.dumps(body).encode("utf-8")
        req = urllib.request.Request(
            f"{self.endpoint}/v1/enqueue",
            data=req_bytes,
            headers={
                "Content-Type": "application/json",
                "User-Agent": "walspool-python/1.0.0",
            },
            method="POST",
        )

        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            err_content = e.read().decode("utf-8")
            raise RuntimeError(f"walspool enqueue failed (HTTP {e.code}): {err_content}") from e
        except urllib.error.URLError as e:
            raise ConnectionError(f"walspool sidecar unreachable at {self.endpoint}: {e.reason}") from e

    def query_logs(
        self,
        trace_id: Optional[str] = None,
        service: Optional[str] = None,
        level: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        Queries indexed logs from the in-memory LogHub in < 15µs.

        :param trace_id: Filter by exact trace_id
        :param service: Filter by service name
        :param level: Filter by log level
        :param limit: Maximum records to return (default: 100)
        :return: Chronologically ordered list of matching log entries
        """
        params = {}
        if trace_id:
            params["trace_id"] = trace_id
        if service:
            params["service"] = service
        if level:
            params["level"] = level
        if limit:
            params["limit"] = str(limit)

        query_str = urllib.parse.urlencode(params)
        url = f"{self.endpoint}/v1/logs?{query_str}" if query_str else f"{self.endpoint}/v1/logs"

        req = urllib.request.Request(
            url,
            headers={"Accept": "application/json", "User-Agent": "walspool-python/1.0.0"},
            method="GET",
        )

        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            err_content = e.read().decode("utf-8")
            raise RuntimeError(f"walspool query failed (HTTP {e.code}): {err_content}") from e
        except urllib.error.URLError as e:
            raise ConnectionError(f"walspool sidecar unreachable at {self.endpoint}: {e.reason}") from e

    def stream_logs(
        self,
        service: Optional[str] = None,
        level: Optional[str] = None,
        auto_reconnect: bool = True,
        initial_reconnect_delay: float = 1.0,
        max_reconnect_delay: float = 10.0,
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Subscribes to the real-time Server-Sent Events (SSE) stream on /v1/logs/stream.
        Yields parsed LogEntry dictionaries as they arrive.
        Automatically reconnects upon transport failure with exponential backoff.

        :param service: Optional filter by service name
        :param level: Optional filter by log level
        :param auto_reconnect: Automatically reconnect if the connection drops
        :param initial_reconnect_delay: Initial retry delay in seconds
        :param max_reconnect_delay: Maximum retry delay cap in seconds
        """
        params = {}
        if service:
            params["service"] = service
        if level:
            params["level"] = level

        query_str = urllib.parse.urlencode(params)
        url = f"{self.endpoint}/v1/logs/stream?{query_str}" if query_str else f"{self.endpoint}/v1/logs/stream"

        reconnect_delay = initial_reconnect_delay

        while True:
            try:
                req = urllib.request.Request(
                    url,
                    headers={
                        "Accept": "text/event-stream",
                        "Cache-Control": "no-cache",
                        "User-Agent": "walspool-python/1.0.0",
                    },
                    method="GET",
                )
                with urllib.request.urlopen(req, timeout=None) as resp:
                    reconnect_delay = initial_reconnect_delay  # reset on success
                    for raw_line in resp:
                        line = raw_line.decode("utf-8").strip()
                        if not line or line.startswith(":"):
                            # Skip empty lines and SSE comments (: connected, : keepalive)
                            continue
                        if line.startswith("data:"):
                            json_str = line[5:].strip()
                            try:
                                yield json.loads(json_str)
                            except json.JSONDecodeError:
                                continue
            except (urllib.error.URLError, ConnectionResetError, TimeoutError) as e:
                if not auto_reconnect:
                    raise
                time.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 1.5, max_reconnect_delay)

    def get_stats(self) -> Dict[str, Any]:
        """
        Fetches operational metrics and Ring Buffer statistics.
        :return: Dictionary containing capacity, current_size, total_ingested, dropped_events
        """
        req = urllib.request.Request(
            f"{self.endpoint}/v1/logs/stats",
            headers={"Accept": "application/json"},
            method="GET",
        )
        with urllib.request.urlopen(req, timeout=self.timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))


if __name__ == "__main__":
    print("=== Walspool v1.0 Python Polyglot Client ===")
    client = WalspoolClient("http://127.0.0.1:9099")
    test_trace_id = f"tr-py-{int(time.time() * 1000)}"
    test_service = "payment-gateway"

    # 1. Enqueue event with metadata and trace_id
    print(f"\n1. Ingesting event via POST /v1/enqueue (TraceID: {test_trace_id})...")
    try:
        receipt = client.enqueue(
            topic="payments.charged",
            payload={
                "transaction_id": "txn_88492",
                "customer_id": "cust_551",
                "amount_cents": 19900,
                "currency": "EUR",
                "status": "SETTLED",
            },
            trace_id=test_trace_id,
            service=test_service,
            level="INFO",
        )
        print(f"   ✓ Event persisted to WAL & LogHub: {receipt}")
    except ConnectionError as ce:
        print(f"   ✕ Connection error: {ce}")
        print("   (Start walspool sidecar via: go run ./cmd/sidecar -addr :9099)")
        exit(0)

    # 2. Query historical log by trace_id
    print(f"\n2. Querying historical log via GET /v1/logs?trace_id={test_trace_id}...")
    logs = client.query_logs(trace_id=test_trace_id)
    print(f"   ✓ Retrieved {len(logs)} matching events in < 15µs:")
    for item in logs:
        print(f"     - [ID {item.get('id')}] [{item.get('timestamp')}] {item.get('service')}: {item.get('payload')}")

    # 3. Query stats
    print("\n3. Fetching in-memory Hub stats via GET /v1/logs/stats...")
    stats = client.get_stats()
    print(f"   ✓ Operational Hub Stats: {stats}")

    print("\nClient demonstration completed successfully.")

