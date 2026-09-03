#!/usr/bin/env python3
"""
walspool Python Client
Drop-in outbox client that dispatches events to a local walspool sidecar daemon.
Sub-millisecond local dispatch, zero external broker dependencies.
"""

import json
import urllib.request
import urllib.error

class WalspoolClient:
    def __init__(self, endpoint: str = "http://localhost:9099"):
        self.endpoint = endpoint.rstrip("/")

    def enqueue(self, topic: str, payload: dict | str | bytes) -> dict:
        """
        Enqueues an event to the local walspool sidecar.
        Returns immediately after the sidecar appends the record to disk WAL.
        """
        url = f"{self.endpoint}/v1/enqueue"
        
        # Serialize payload if necessary
        if isinstance(payload, (dict, list)):
            data = {"topic": topic, "payload": payload}
        else:
            data = {"topic": topic, "payload": str(payload)}

        req_body = json.dumps(data).encode("utf-8")
        req = urllib.request.Request(
            url,
            data=req_body,
            headers={"Content-Type": "application/json", "User-Agent": "walspool-python/1.0"}
        )

        try:
            with urllib.request.urlopen(req, timeout=1.0) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            err_content = e.read().decode("utf-8")
            raise RuntimeError(f"walspool enqueue failed ({e.code}): {err_content}")

if __name__ == "__main__":
    client = WalspoolClient()
    print("Sending test event to walspool sidecar...")
    result = client.enqueue("billing.invoice_paid", {
        "invoice_id": "inv_8849",
        "customer": "acme_corp",
        "amount_cents": 12500,
        "currency": "usd"
    })
    print(f"✓ Event successfully buffered in WAL: {result}")
