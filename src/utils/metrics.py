"""
Performance monitoring and metrics collection for distributed sync system.
"""
import asyncio
import time
import logging
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Callable, Any
from contextlib import asynccontextmanager

logger = logging.getLogger(__name__)


@dataclass
class MetricSample:
    timestamp: float
    value: float
    labels: Dict[str, str] = field(default_factory=dict)


@dataclass
class CounterMetric:
    name: str
    description: str
    value: float = 0.0
    labels: Dict[str, str] = field(default_factory=dict)

    def inc(self, amount: float = 1.0) -> None:
        self.value += amount

    def reset(self) -> None:
        self.value = 0.0


@dataclass
class GaugeMetric:
    name: str
    description: str
    value: float = 0.0
    labels: Dict[str, str] = field(default_factory=dict)

    def set(self, value: float) -> None:
        self.value = value

    def inc(self, amount: float = 1.0) -> None:
        self.value += amount

    def dec(self, amount: float = 1.0) -> None:
        self.value -= amount


class HistogramMetric:
    """Rolling-window histogram for latency tracking."""

    def __init__(self, name: str, description: str, window_size: int = 1000):
        self.name = name
        self.description = description
        self.window_size = window_size
        self._samples: deque = deque(maxlen=window_size)
        self._total: float = 0.0
        self._count: int = 0

    def observe(self, value: float) -> None:
        self._samples.append(value)
        self._total += value
        self._count += 1

    @property
    def count(self) -> int:
        return self._count

    @property
    def sum(self) -> float:
        return self._total

    @property
    def mean(self) -> float:
        if not self._samples:
            return 0.0
        return sum(self._samples) / len(self._samples)

    def percentile(self, p: float) -> float:
        """Return pth percentile (0-100) from the current window."""
        if not self._samples:
            return 0.0
        sorted_samples = sorted(self._samples)
        idx = int(len(sorted_samples) * p / 100)
        idx = min(idx, len(sorted_samples) - 1)
        return sorted_samples[idx]

    @property
    def p50(self) -> float:
        return self.percentile(50)

    @property
    def p95(self) -> float:
        return self.percentile(95)

    @property
    def p99(self) -> float:
        return self.percentile(99)

    def snapshot(self) -> Dict[str, float]:
        return {
            "count": self.count,
            "sum": self.sum,
            "mean": self.mean,
            "p50": self.p50,
            "p95": self.p95,
            "p99": self.p99,
        }


class MetricsRegistry:
    """Central registry for all metrics in a node."""

    def __init__(self, node_id: str):
        self.node_id = node_id
        self._counters: Dict[str, CounterMetric] = {}
        self._gauges: Dict[str, GaugeMetric] = {}
        self._histograms: Dict[str, HistogramMetric] = {}
        self._start_time = time.time()

    # ── counter ──────────────────────────────────────────────────────────────

    def counter(self, name: str, description: str = "") -> CounterMetric:
        if name not in self._counters:
            self._counters[name] = CounterMetric(name, description)
        return self._counters[name]

    # ── gauge ─────────────────────────────────────────────────────────────────

    def gauge(self, name: str, description: str = "") -> GaugeMetric:
        if name not in self._gauges:
            self._gauges[name] = GaugeMetric(name, description)
        return self._gauges[name]

    # ── histogram ─────────────────────────────────────────────────────────────

    def histogram(self, name: str, description: str = "") -> HistogramMetric:
        if name not in self._histograms:
            self._histograms[name] = HistogramMetric(name, description)
        return self._histograms[name]

    # ── timing helper ─────────────────────────────────────────────────────────

    @asynccontextmanager
    async def timer(self, histogram_name: str):
        """Async context manager that records elapsed seconds into a histogram."""
        start = time.perf_counter()
        try:
            yield
        finally:
            elapsed = time.perf_counter() - start
            self.histogram(histogram_name).observe(elapsed)

    # ── snapshot ──────────────────────────────────────────────────────────────

    def snapshot(self) -> Dict[str, Any]:
        return {
            "node_id": self.node_id,
            "uptime_seconds": time.time() - self._start_time,
            "timestamp": time.time(),
            "counters": {k: v.value for k, v in self._counters.items()},
            "gauges": {k: v.value for k, v in self._gauges.items()},
            "histograms": {k: v.snapshot() for k, v in self._histograms.items()},
        }

    def log_snapshot(self) -> None:
        snap = self.snapshot()
        logger.info(
            "[Metrics %s] uptime=%.1fs counters=%s gauges=%s",
            self.node_id,
            snap["uptime_seconds"],
            snap["counters"],
            snap["gauges"],
        )


# ── Raft-specific pre-built metrics ──────────────────────────────────────────

def build_raft_metrics(registry: MetricsRegistry) -> Dict[str, Any]:
    """Convenience factory: returns a dict of commonly used Raft metrics."""
    return {
        # elections
        "elections_started":    registry.counter("raft_elections_started", "Number of elections started"),
        "elections_won":        registry.counter("raft_elections_won",     "Number of elections won"),
        "votes_granted":        registry.counter("raft_votes_granted",     "Number of votes granted"),
        "votes_rejected":       registry.counter("raft_votes_rejected",    "Number of votes rejected"),

        # log / replication
        "log_entries_appended": registry.counter("raft_log_entries_appended", "Log entries written"),
        "log_entries_committed":registry.counter("raft_log_entries_committed","Log entries committed"),
        "log_entries_applied":  registry.counter("raft_log_entries_applied",  "Log entries applied to state machine"),
        "append_entries_sent":  registry.counter("raft_append_entries_sent",  "AppendEntries RPCs sent"),
        "append_entries_recv":  registry.counter("raft_append_entries_recv",  "AppendEntries RPCs received"),

        # failures / network
        "rpc_timeouts":         registry.counter("raft_rpc_timeouts",     "RPC calls that timed out"),
        "peer_failures":        registry.counter("raft_peer_failures",    "Peer contact failures"),
        "heartbeats_sent":      registry.counter("raft_heartbeats_sent",  "Heartbeat messages sent"),

        # state
        "current_term":         registry.gauge("raft_current_term",  "Current Raft term"),
        "commit_index":         registry.gauge("raft_commit_index",  "Highest log index known to be committed"),
        "last_applied":         registry.gauge("raft_last_applied",  "Highest log index applied to state machine"),
        "log_length":           registry.gauge("raft_log_length",    "Total number of log entries"),
        "peer_count":           registry.gauge("raft_peer_count",    "Number of known peers"),

        # latency histograms
        "rpc_latency":          registry.histogram("raft_rpc_latency_seconds",          "RPC round-trip latency"),
        "election_duration":    registry.histogram("raft_election_duration_seconds",    "Time from election start to leader elected"),
        "commit_latency":       registry.histogram("raft_commit_latency_seconds",       "Time from log append to commit"),
        "apply_latency":        registry.histogram("raft_apply_latency_seconds",        "Time from commit to applied"),
    }