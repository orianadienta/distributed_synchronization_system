"""
Locust load test scenarios for the distributed sync system.
Run with:  locust -f benchmarks/load_test_scenarios.py --host http://localhost:8001
"""
from locust import HttpUser, task, between
import random
import string
import json


def rand_key(n=8):
    return "".join(random.choices(string.ascii_lowercase, k=n))


# ── Lock Manager ──────────────────────────────────────────────────────────────

class LockManagerUser(HttpUser):
    wait_time = between(0.05, 0.2)
    host = "http://localhost:8001"

    @task(3)
    def acquire_exclusive(self):
        name = f"resource-{random.randint(1, 20)}"
        payload = {
            "lock_name": name,
            "mode":      "exclusive",
            "holder_id": f"user-{self.environment.runner.user_count}",
            "ttl":       5.0,
            "timeout":   2.0,
        }
        with self.client.post("/lock/acquire", json=payload, catch_response=True) as resp:
            if resp.status_code in (200, 408, 503):
                resp.success()

    @task(2)
    def acquire_shared(self):
        name = f"resource-{random.randint(1, 20)}"
        payload = {
            "lock_name": name,
            "mode":      "shared",
            "holder_id": f"reader-{random.randint(1, 100)}",
            "ttl":       3.0,
            "timeout":   1.0,
        }
        with self.client.post("/lock/acquire", json=payload, catch_response=True) as resp:
            if resp.status_code in (200, 408, 503):
                resp.success()

    @task(1)
    def check_status(self):
        self.client.get("/lock/status")

    @task(1)
    def check_deadlocks(self):
        self.client.get("/lock/deadlocks")


# ── Queue System ──────────────────────────────────────────────────────────────

class QueueProducerUser(HttpUser):
    wait_time = between(0.02, 0.1)
    host = "http://localhost:8002"

    @task
    def enqueue(self):
        payload = {
            "queue_name":  f"queue-{random.randint(1, 5)}",
            "payload":     {"data": rand_key(20), "ts": random.random()},
            "producer_id": f"producer-{random.randint(1, 10)}",
        }
        self.client.post("/queue/enqueue", json=payload)


class QueueConsumerUser(HttpUser):
    wait_time = between(0.05, 0.3)
    host = "http://localhost:8002"

    @task(3)
    def consume(self):
        payload = {"queue_name": f"queue-{random.randint(1, 5)}"}
        with self.client.post("/queue/consume", json=payload, catch_response=True) as resp:
            if resp.status_code == 200:
                data = resp.json()
                msg  = data.get("message")
                if msg:
                    # Acknowledge the message
                    self.client.post("/queue/ack", json={
                        "message_id": msg["message_id"],
                        "op": "ack",
                    })
                resp.success()

    @task(1)
    def queue_status(self):
        self.client.get("/queue/status")


# ── Cache System ──────────────────────────────────────────────────────────────

class CacheUser(HttpUser):
    wait_time = between(0.01, 0.05)
    host = "http://localhost:8003"

    @task(5)
    def cache_get(self):
        key = f"key-{random.randint(1, 100)}"
        with self.client.get(f"/cache/{key}", catch_response=True) as resp:
            if resp.status_code in (200, 404):
                resp.success()

    @task(2)
    def cache_put(self):
        key     = f"key-{random.randint(1, 100)}"
        payload = {"value": rand_key(50), "ttl": 60.0}
        self.client.put(f"/cache/{key}", json=payload)

    @task(1)
    def cache_delete(self):
        key = f"key-{random.randint(1, 100)}"
        self.client.delete(f"/cache/{key}")

    @task(1)
    def cache_stats(self):
        self.client.get("/cache/stats")


# ── Mixed workload ────────────────────────────────────────────────────────────

class MixedWorkloadUser(HttpUser):
    """Simulates a realistic application hitting all three subsystems."""
    wait_time = between(0.1, 0.5)
    host = "http://localhost:8001"

    @task(3)
    def workflow_lock_and_enqueue(self):
        # Acquire lock → success → enqueue work item → release
        lock_resp = self.client.post("/lock/acquire", json={
            "lock_name": f"work-item-{random.randint(1, 50)}",
            "mode":      "exclusive",
            "holder_id": "mixed-user",
            "ttl":       5.0,
            "timeout":   1.0,
        })
        if lock_resp.status_code == 200:
            data = lock_resp.json()
            if data.get("status") == "acquired":
                # Simulate work: enqueue result
                self.client.post(
                    "http://localhost:8002/queue/enqueue",
                    json={
                        "queue_name":  "results",
                        "payload":     {"job": "done"},
                        "producer_id": "mixed",
                    },
                )
                # Release
                self.client.post("/lock/release", json={
                    "lock_name":  f"work-item-{random.randint(1, 50)}",
                    "request_id": data.get("request_id", ""),
                    "holder_id":  "mixed-user",
                })

    @task(2)
    def health_check(self):
        self.client.get("/health")

    @task(1)
    def metrics(self):
        self.client.get("/metrics")