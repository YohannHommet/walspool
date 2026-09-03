#!/usr/bin/env node
/**
 * walspool Node.js Client
 * Dispatches events to a local walspool sidecar daemon using native fetch.
 */

class WalspoolClient {
  constructor(endpoint = "http://localhost:9099") {
    this.endpoint = endpoint.replace(/\/+$/, "");
  }

  async enqueue(topic, payload) {
    const res = await fetch(`${this.endpoint}/v1/enqueue`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ topic, payload }),
    });

    if (!res.ok) {
      const err = await res.text();
      throw new Error(`walspool enqueue failed (${res.status}): ${err}`);
    }

    return await res.json();
  }
}

// Demo usage:
async function run() {
  const client = new WalspoolClient();
  console.log("Sending event from Node.js to walspool sidecar...");
  
  const receipt = await client.enqueue("webhooks.partner_sync", {
    userId: "usr_4491",
    action: "password_reset",
    ipAddress: "192.168.1.50"
  });

  console.log("✓ Event buffered safely on disk:", receipt);
}

if (require.main === module) {
  run().catch(console.error);
}

module.exports = { WalspoolClient };
