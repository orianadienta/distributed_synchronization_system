"""Unit tests for Phi Accrual Failure Detector."""
import asyncio
import sys, os, time
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from src.communication.failure_detector import FailureDetector, HeartbeatWindow


def test_heartbeat_window_phi_zero_no_data():
    w = HeartbeatWindow()
    assert w.phi(time.time()) == 0.0


def test_heartbeat_window_builds_up():
    w = HeartbeatWindow()
    t = time.time()
    for i in range(10):
        w.record(t + i * 0.1)
    # After ~0.1s intervals, phi at t+2s should be elevated
    phi = w.phi(t + 2.0)
    assert phi > 0


def test_heartbeat_window_mean():
    w = HeartbeatWindow()
    t = time.time()
    for i in range(5):
        w.record(t + i * 0.2)
    # Mean interval ≈ 0.2s
    assert abs(w.mean() - 0.2) < 0.05


def test_failure_detector_alive_after_heartbeat():
    fd = FailureDetector(phi_threshold=8.0)
    fd.register_peer("n1")
    t = time.time()
    for i in range(20):
        fd._windows["n1"].record(t + i * 0.1)
    # Right after last heartbeat, phi should be low
    assert fd.is_alive("n1")


def test_failure_detector_suspect_after_silence():
    fd = FailureDetector(phi_threshold=1.0)  # low threshold for test
    fd.register_peer("n2")
    t = time.time() - 10  # 10 seconds ago
    for i in range(20):
        fd._windows["n2"].record(t + i * 0.1)
    # 10 seconds since last heartbeat with 1.0 threshold → suspected
    assert not fd.is_alive("n2")


def test_failure_detector_callback_on_suspect():
    suspects = []
    fd = FailureDetector(phi_threshold=1.0, on_suspect=lambda pid: suspects.append(pid))
    fd.register_peer("n3")
    t = time.time() - 10
    for i in range(20):
        fd._windows["n3"].record(t + i * 0.1)

    # Manually trigger scan logic (without async loop)
    now = time.time()
    for peer_id, window in list(fd._windows.items()):
        phi_val = window.phi(now)
        if phi_val >= fd.phi_threshold and peer_id not in fd._suspected:
            fd._suspected.add(peer_id)
            fd._on_suspect(peer_id)

    assert "n3" in suspects


def test_failure_detector_recover_clears_suspected():
    fd = FailureDetector(phi_threshold=1.0)
    fd.register_peer("n4")
    fd._suspected.add("n4")

    # Simulate heartbeat arriving
    fd.heartbeat("n4")
    assert "n4" not in fd._suspected


@pytest.mark.asyncio
async def test_failure_detector_start_stop():
    fd = FailureDetector()
    await fd.start(scan_interval=0.01)
    await asyncio.sleep(0.05)
    await fd.stop()