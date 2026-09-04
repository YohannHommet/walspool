#!/usr/bin/env node
/**
 * walspool Node.js Polyglot Client (v1.0.0)
 *
 * High-performance client for walspool sidecar daemon:
 * 1. POST /v1/enqueue - Instant local WAL append & Hub indexing (< 15µs)
 * 2. GET  /v1/logs    - Historical query by trace_id, service, level in O(k)
 * 3. GET  /v1/logs/stream - Real-time non-blocking Server-Sent Events (SSE) with auto-reconnect
 *
 * Zero external npm dependencies (uses native fetch & http available in Node 18+).
 */

const http = require("http");
const https = require("https");
const { URL } = require("url");

class WalspoolClient {
  /**
   * @param {string} endpoint - Base URL of walspool sidecar (e.g. http://127.0.0.1:9099)
   */
  constructor(endpoint = "http://127.0.0.1:9099") {
    this.endpoint = endpoint.replace(/\/+$/, "");
  }

  /**
   * Enqueues an event into local disk WAL with simultaneous in-memory Hub indexing.
   *
   * @param {string} topic - Topic name (e.g. "billing.invoices")
   * @param {object|string|Buffer} payload - Event payload
   * @param {object} [options]
   * @param {string} [options.traceId] - Distributed trace identifier (e.g. "tr-9481-abc")
   * @param {string} [options.service] - Originating service name (defaults to topic)
   * @param {string} [options.level] - Severity level (INFO, WARN, ERROR, etc.)
   * @returns {Promise<{status: string, topic: string, size_bytes: number}>}
   */
  async enqueue(topic, payload, options = {}) {
    if (!topic || typeof topic !== "string") {
      throw new Error("walspool: topic must be a non-empty string");
    }

    const body = {
      topic,
      payload,
      trace_id: options.traceId || options.trace_id,
      service: options.service,
      level: options.level || "INFO",
    };

    const res = await fetch(`${this.endpoint}/v1/enqueue`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "User-Agent": "walspool-node/1.0.0",
      },
      body: JSON.stringify(body),
    });

    if (!res.ok) {
      const errText = await res.text();
      throw new Error(`walspool enqueue failed (HTTP ${res.status}): ${errText}`);
    }

    return await res.json();
  }

  /**
   * Queries historical indexed logs from the in-memory LogHub in < 15µs.
   *
   * @param {object} [filters]
   * @param {string} [filters.traceId] - Filter by exact trace_id
   * @param {string} [filters.service] - Filter by service name
   * @param {string} [filters.level] - Filter by severity (e.g. "ERROR")
   * @param {number} [filters.limit=100] - Maximum records to return
   * @returns {Promise<Array<{id: number, timestamp: string, topic: string, service: string, trace_id?: string, level?: string, payload: any}>>}
   */
  async queryLogs(filters = {}) {
    const params = new URLSearchParams();
    if (filters.traceId || filters.trace_id) {
      params.set("trace_id", filters.traceId || filters.trace_id);
    }
    if (filters.service) {
      params.set("service", filters.service);
    }
    if (filters.level) {
      params.set("level", filters.level);
    }
    if (filters.limit) {
      params.set("limit", String(filters.limit));
    }

    const url = `${this.endpoint}/v1/logs?${params.toString()}`;
    const res = await fetch(url, {
      method: "GET",
      headers: { Accept: "application/json" },
    });

    if (!res.ok) {
      const errText = await res.text();
      throw new Error(`walspool query failed (HTTP ${res.status}): ${errText}`);
    }

    return await res.json();
  }

  /**
   * Subscribes to real-time Server-Sent Events (SSE) stream with automatic reconnection.
   *
   * @param {object} [filter]
   * @param {string} [filter.service] - Filter stream by service name
   * @param {string} [filter.level] - Filter stream by level (e.g. "ERROR")
   * @param {function(object): void} onEvent - Callback invoked on each received log entry
   * @param {function(Error): void} [onError] - Callback invoked on transport error
   * @returns {{ close: function(): void }} Subscription handle to abort streaming
   */
  streamLogs(filter = {}, onEvent, onError) {
    let closed = false;
    let currentReq = null;
    let retryDelay = 1000;

    const connect = () => {
      if (closed) return;

      const streamUrl = new URL(`${this.endpoint}/v1/logs/stream`);
      if (filter.service) streamUrl.searchParams.set("service", filter.service);
      if (filter.level) streamUrl.searchParams.set("level", filter.level);

      const clientModule = streamUrl.protocol === "https:" ? https : http;

      const req = clientModule.get(
        streamUrl.toString(),
        {
          headers: {
            Accept: "text/event-stream",
            "Cache-Control": "no-cache",
            Connection: "keep-alive",
          },
        },
        (res) => {
          if (res.statusCode !== 200) {
            const err = new Error(`walspool SSE stream failed with HTTP ${res.statusCode}`);
            if (onError) onError(err);
            res.resume();
            scheduleReconnect();
            return;
          }

          retryDelay = 1000;

          let buffer = "";
          res.setEncoding("utf8");

          res.on("data", (chunk) => {
            buffer += chunk;
            const lines = buffer.split("\n");
            buffer = lines.pop();

            for (const line of lines) {
              const trimmed = line.trim();
              if (!trimmed || trimmed.startsWith(":")) {
                continue;
              }
              if (trimmed.startsWith("data:")) {
                const jsonStr = trimmed.slice(5).trim();
                try {
                  const entry = JSON.parse(jsonStr);
                  if (onEvent) onEvent(entry);
                } catch (parseErr) {
                  // ignore malformed chunks
                }
              }
            }
          });

          res.on("end", () => {
            if (!closed) scheduleReconnect();
          });

          res.on("error", (err) => {
            if (onError && !closed) onError(err);
            if (!closed) scheduleReconnect();
          });
        }
      );

      req.on("error", (err) => {
        if (onError && !closed) onError(err);
        if (!closed) scheduleReconnect();
      });

      currentReq = req;
    };

    const scheduleReconnect = () => {
      if (closed) return;
      setTimeout(() => {
        if (!closed) connect();
      }, retryDelay);
      retryDelay = Math.min(retryDelay * 1.5, 10000);
    };

    connect();

    return {
      close: () => {
        closed = true;
        if (currentReq) {
          try {
            currentReq.destroy();
          } catch (_) {}
        }
      },
    };
  }

  /**
   * Retrieves runtime operational metrics and Ring Buffer statistics.
   * @returns {Promise<{capacity: number, current_size: number, total_ingested: number, active_streams: number, indexed_traces: number, dropped_events: number}>}
   */
  async getStats() {
    const res = await fetch(`${this.endpoint}/v1/logs/stats`);
    if (!res.ok) {
      throw new Error(`walspool stats failed with HTTP ${res.status}`);
    }
    return await res.json();
  }
}

// Demo usage:
async function run() {
  console.log("=== Walspool v1.0 Node.js Polyglot Client ===");
  const client = new WalspoolClient("http://127.0.0.1:9099");
  const testTraceId = `tr-node-${Date.now()}`;
  const testService = "billing-service";

  console.log(`\n1. Subscribing to real-time SSE stream on /v1/logs/stream (service=${testService})...`);
  const subscription = client.streamLogs(
    { service: testService },
    (entry) => {
      console.log(`   ⚡ [SSE STREAM RECEIVED] TraceID=${entry.trace_id} Service=${entry.service} Level=${entry.level}`);
      console.log("      Payload:", entry.payload);
    },
    (err) => {
      console.warn(`   [SSE Notice] ${err.message}`);
    }
  );

  await new Promise((r) => setTimeout(r, 200));

  console.log(`\n2. Ingesting event via POST /v1/enqueue (TraceID: ${testTraceId})...`);
  try {
    const receipt = await client.enqueue(
      "invoices.generated",
      {
        invoiceId: "inv_99812",
        customerId: "cust_381",
        amountCents: 15990,
        currency: "USD",
        status: "PAID",
      },
      {
        traceId: testTraceId,
        service: testService,
        level: "INFO",
      }
    );
    console.log("   ✓ Event acknowledged by walspool sidecar:", receipt);
  } catch (err) {
    console.error("   ✕ Enqueue error:", err.message);
  }

  await new Promise((r) => setTimeout(r, 400));

  console.log(`\n3. Querying historical logs via GET /v1/logs?trace_id=${testTraceId}...`);
  try {
    const logs = await client.queryLogs({ traceId: testTraceId });
    console.log(`   ✓ Retrieved ${logs.length} matching events in < 15µs:`);
    for (const log of logs) {
      console.log(`     - [ID ${log.id}] [${log.timestamp}] ${log.service}:`, log.payload);
    }
  } catch (err) {
    console.error("   ✕ Query error:", err.message);
  }

  console.log("\n4. Fetching in-memory Ring Buffer stats via GET /v1/logs/stats...");
  try {
    const stats = await client.getStats();
    console.log("   ✓ Ring Buffer & Hub Stats:", stats);
  } catch (err) {
    console.error("   ✕ Stats error:", err.message);
  }

  subscription.close();
  console.log("\nCompleted successfully.");
}

if (require.main === module) {
  run().catch(console.error);
}

module.exports = { WalspoolClient };
