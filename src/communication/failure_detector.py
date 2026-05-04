"""
Phi Accrual Failure Detector for distributed nodes.

Reference: "The Phi Accrual Failure Detector" – Hayashibara et al., 2004.
A phi value above `threshold` means the node is suspected to have failed.
"""
import asyncio
import logging
import math
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Callable, Dict, Optional, Set

logger = logging.getLogger(__name__)

# Default suspicion threshold (phi).  Values commonly used in practice: 8–12.
DEFAULT_PHI_THRESHOLD = 8.0
# Window of inter-arrival samples used to estimate distribution.
SAMPLE_WINDOW = 200


@dataclass
class HeartbeatWindow:
    """Sliding window of heartbeat inter-arrival times for one peer."""
    samples: deque = field(default_factory=lambda: deque(maxlen=SAMPLE_WINDOW))
    last_arrival: Optional[float] = None

    def record(self, now: float) -> None:
        if self.last_arrival is not None:
            interval = now - self.last_arrival
            if interval > 0:
                self.samples.append(interval)
        self.last_arrival = now

    # ── Statistics ────────────────────────────────────────────────────────

    def mean(self) -> float:
        if not self.samples:
            return 1.0  # assume 1 s if no data yet
        return sum(self.samples) / len(self.samples)

    def variance(self) -> float:
        if len(self.samples) < 2:
            return 0.0
        m = self.mean()
        return sum((x - m) ** 2 for x in self.samples) / (len(self.samples) - 1)

    def std_dev(self) -> float:
        v = self.variance()
        return math.sqrt(v) if v > 0 else 0.01

    # ── Phi ───────────────────────────────────────────────────────────────

    def phi(self, now: float) -> float:
        """Compute current phi value given the current wall-clock time."""
        if self.last_arrival is None:
            return 0.0
        elapsed = now - self.last_arrival
        if elapsed <= 0:
            return 0.0
        mu = self.mean()
        sigma = self.std_dev()
        # Exponential CDF approximation (works when distribution is approximately normal)
        # F(t) = 1 - exp(-lambda * t),  lambda = 1/mu
        # phi = -log10(1 - F(elapsed))
        # Using the normal CDF for better accuracy when we have enough samples:
        if len(self.samples) >= 10:
            z = (elapsed - mu) / sigma
            p = self._normal_cdf(z)
        else:
            # Fall back to exponential when sample count is low
            lam = 1.0 / mu
            p = 1.0 - math.exp(-lam * elapsed)
        p = min(p, 1.0 - 1e-15)
        return -math.log10(1.0 - p)

    @staticmethod
    def _normal_cdf(z: float) -> float:
        """Standard normal CDF via erf approximation."""
        return 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))


class FailureDetector:
    """
    Monitors peer liveness using the Phi Accrual algorithm.

    Usage
    -----
    fd = FailureDetector(phi_threshold=8.0)
    fd.register_peer("node-2")
    ...
    # Call this every time we receive a heartbeat from a peer:
    fd.heartbeat("node-2")
    ...
    # Query suspicion level:
    if fd.is_alive("node-2"):
        ...
    """

    def __init__(
        self,
        phi_threshold: float = DEFAULT_PHI_THRESHOLD,
        on_suspect: Optional[Callable[[str], None]] = None,
        on_recover: Optional[Callable[[str], None]] = None,
    ):
        self.phi_threshold = phi_threshold
        self._on_suspect = on_suspect
        self._on_recover = on_recover

        self._windows: Dict[str, HeartbeatWindow] = {}
        self._suspected: Set[str] = set()
        self._running = False
        self._task: Optional[asyncio.Task] = None

    # ── Registration ──────────────────────────────────────────────────────

    def register_peer(self, peer_id: str) -> None:
        if peer_id not in self._windows:
            self._windows[peer_id] = HeartbeatWindow()
            logger.debug("[FailureDetector] Registered peer %s", peer_id)

    def unregister_peer(self, peer_id: str) -> None:
        self._windows.pop(peer_id, None)
        self._suspected.discard(peer_id)

    # ── Heartbeat input ───────────────────────────────────────────────────

    def heartbeat(self, peer_id: str) -> None:
        """Call this whenever a message is received from peer_id."""
        now = time.time()
        if peer_id not in self._windows:
            self.register_peer(peer_id)
        self._windows[peer_id].record(now)

        # Clear suspicion if the node recovered
        if peer_id in self._suspected:
            self._suspected.discard(peer_id)
            logger.info("[FailureDetector] %s recovered (phi below threshold)", peer_id)
            if self._on_recover:
                try:
                    self._on_recover(peer_id)
                except Exception:
                    pass

    # ── Queries ───────────────────────────────────────────────────────────

    def phi(self, peer_id: str) -> float:
        """Return current phi for peer, or 0 if unknown."""
        window = self._windows.get(peer_id)
        if window is None:
            return 0.0
        return window.phi(time.time())

    def is_alive(self, peer_id: str) -> bool:
        return self.phi(peer_id) < self.phi_threshold

    def is_suspected(self, peer_id: str) -> bool:
        return peer_id in self._suspected

    def all_phi(self) -> Dict[str, float]:
        now = time.time()
        return {pid: w.phi(now) for pid, w in self._windows.items()}

    # ── Background scan ───────────────────────────────────────────────────

    async def start(self, scan_interval: float = 0.5) -> None:
        self._running = True
        self._task = asyncio.create_task(self._scan_loop(scan_interval))
        logger.info("[FailureDetector] Started (phi_threshold=%.1f)", self.phi_threshold)

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("[FailureDetector] Stopped")

    async def _scan_loop(self, interval: float) -> None:
        while self._running:
            now = time.time()
            for peer_id, window in list(self._windows.items()):
                phi_val = window.phi(now)
                if phi_val >= self.phi_threshold and peer_id not in self._suspected:
                    self._suspected.add(peer_id)
                    logger.warning(
                        "[FailureDetector] Suspecting %s (phi=%.2f >= %.1f)",
                        peer_id, phi_val, self.phi_threshold,
                    )
                    if self._on_suspect:
                        try:
                            self._on_suspect(peer_id)
                        except Exception:
                            pass
            await asyncio.sleep(interval)