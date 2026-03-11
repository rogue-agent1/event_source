#!/usr/bin/env python3
"""Event sourcing — append-only event store with projections and replay."""
import sys, json, time, uuid

class EventStore:
    def __init__(self):
        self.events = []; self.snapshots = {}
    def append(self, stream, event_type, data):
        event = {"id": str(uuid.uuid4())[:8], "stream": stream, "type": event_type,
                 "data": data, "ts": time.time(),
                 "version": sum(1 for e in self.events if e["stream"] == stream) + 1}
        self.events.append(event)
        return event
    def stream(self, name):
        return [e for e in self.events if e["stream"] == name]
    def project(self, name, projector, initial=None):
        state = dict(initial or {})
        for e in self.stream(name):
            state = projector(state, e)
        return state
    def streams(self):
        return sorted(set(e["stream"] for e in self.events))

def account_projector(state, event):
    s = dict(state)
    if "balance" not in s: s["balance"] = 0; s["txns"] = 0
    if event["type"] == "deposit": s["balance"] += event["data"]["amount"]
    elif event["type"] == "withdraw": s["balance"] -= event["data"]["amount"]
    s["txns"] += 1
    return s

def cart_projector(state, event):
    s = dict(state)
    if "items" not in s: s["items"] = {}; s["total"] = 0
    d = event["data"]
    if event["type"] == "item_added":
        s["items"][d["item"]] = s["items"].get(d["item"], 0) + d.get("qty", 1)
        s["total"] += d["price"] * d.get("qty", 1)
    elif event["type"] == "item_removed" and d["item"] in s["items"]:
        del s["items"][d["item"]]
    return s

if __name__ == "__main__":
    store = EventStore()
    store.append("acct-1", "deposit", {"amount": 1000})
    store.append("acct-1", "withdraw", {"amount": 200})
    store.append("acct-1", "deposit", {"amount": 500})
    store.append("acct-2", "deposit", {"amount": 5000})
    store.append("acct-2", "withdraw", {"amount": 1500})
    store.append("cart-1", "item_added", {"item": "laptop", "price": 999, "qty": 1})
    store.append("cart-1", "item_added", {"item": "mouse", "price": 29, "qty": 2})
    print("=== Event Sourcing Demo ===\n")
    for s in store.streams():
        if s.startswith("acct"):
            state = store.project(s, account_projector)
            print(f"{s}: balance=${state['balance']}, {state['txns']} transactions")
        elif s.startswith("cart"):
            state = store.project(s, cart_projector)
            print(f"{s}: {state['items']}, total=${state['total']}")
        for e in store.stream(s):
            print(f"  [{e['version']}] {e['type']}: {e['data']}")
        print()
    print(f"Total events: {len(store.events)}")
