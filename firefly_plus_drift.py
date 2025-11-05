# firefly_adaptive_sync.py
# Adaptive, node-specific sync scheduling with fallback
# VIBE-CODED IN ONE FILE. RUNS. NO CRASHES. PURE SPECULATION.
# Inspired by Firefly (SIGCOMM '25) + feedback
# DISCLAIMER: Untested on real hardware. Firefly closed source. Laugh at will.

import time
import random
import threading
from dataclasses import dataclass
from typing import Dict
from collections import defaultdict

# === CONFIG ===
FULL_SYNC_INTERVAL_DEFAULT = 1.0   # fallback: 1 Hz
REPORT_INTERVAL = 10.0             # nodes report every 10s
DRIFT_THRESHOLD = 5e-9             # 5 ns
PROFILE_HISTORY = 5                # keep last N drift rates
SCHEDULER_HEARTBEAT = 2.0          # fail if no heartbeat in 2s
# =============

@dataclass
class NodeProfile:
    drift_rate: float = 0.0
    history: list = None
    last_sync: float = 0.0
    sync_interval: float = FULL_SYNC_INTERVAL_DEFAULT
    correction: float = 0.0

    def __post_init__(self):
        self.history = []

    def update_drift(self, measured_offset, dt):
        if dt > 0:
            rate = measured_offset / dt
            self.history.append(rate)
            if len(self.history) > PROFILE_HISTORY:
                self.history.pop(0)
            self.drift_rate = sum(self.history) / len(self.history) if self.history else 0.0

    def predict_offset(self, now):
        dt = now - self.last_sync
        return self.drift_rate * dt + self.correction


class Node:
    def __init__(self, node_id: str, scheduler=None):
        self.id = node_id
        self.local_time_offset = random.gauss(0, 1e-8)  # simulate unique drift
        self.scheduler = scheduler
        self.profile = NodeProfile()
        self.running = True
        self.thread = threading.Thread(target=self._run, daemon=True)

    def start(self):
        self.thread.start()

    def _run(self):
        last_report = 0.0
        while self.running:
            now = time.time()
            self.local_time = now + self.local_time_offset * (now - time.time())

            if now - last_report >= REPORT_INTERVAL:
                self._send_report(now)
                last_report = now

            time.sleep(0.1)

    def _send_report(self, now):
        if not self.scheduler or not self.scheduler.is_alive():
            self._fallback_sync(now)
            return

        measured_offset = self._measure_offset_to_peers()
        data = {
            "node_id": self.id,
            "local_time": self.local_time,
            "last_sync": self.profile.last_sync,
            "measured_offset": measured_offset
        }
        self.scheduler.receive_report(data)

    def _measure_offset_to_peers(self):
        return random.gauss(0, 2e-9)  # light probe

    def _fallback_sync(self, now):
        offset = random.gauss(0, 3e-9)
        self.profile.correction += offset
        self.profile.last_sync = now
        self.profile.sync_interval = FULL_SYNC_INTERVAL_DEFAULT
        print(f"[{self.id}] Fallback: full sync, corr {offset:+.2e}")

    def receive_schedule(self, interval, correction):
        self.profile.sync_interval = interval
        self.profile.correction = correction
        self.profile.last_sync = time.time()
        print(f"[{self.id}] ← Scheduler: sync {interval:.2f}s, corr {correction:+.2e}")

    def stop(self):
        self.running = False
        self.thread.join()


class Scheduler:
    def __init__(self):
        self.profiles: Dict[str, NodeProfile] = {}
        self.last_heartbeat = time.time()
        self.lock = threading.Lock()
        self.thread = threading.Thread(target=self._heartbeat, daemon=True)
        self.thread.start()

    def receive_report(self, data):
        node_id = data["node_id"]
        now = time.time()
        local_time = data["local_time"]
        last_sync = data["last_sync"]
        measured = data["measured_offset"]

        with self.lock:
            profile = self.profiles.setdefault(node_id, NodeProfile(last_sync=last_sync))
            dt = now - last_sync
            predicted = profile.predict_offset(now)
            error = abs(measured - predicted)

            profile.update_drift(measured, dt)

            if error > DRIFT_THRESHOLD:
                new_interval = max(1.0, min(10.0, 1.0 / (abs(profile.drift_rate) / DRIFT_THRESHOLD + 1e-12)))
                correction = -measured
                self._send_schedule(node_id, new_interval, correction)
            else:
                self._send_schedule(node_id, profile.sync_interval, profile.correction)

    def _send_schedule(self, node_id, interval, correction):
        print(f"[Scheduler] → {node_id}: sync {interval:.2f}s, corr {correction:+.2e}")

    def _heartbeat(self):
        while True:
            self.last_heartbeat = time.time()
            time.sleep(SCHEDULER_HEARTBEAT / 2)

    def is_alive(self):
        return time.time() - self.last_heartbeat < SCHEDULER_HEARTBEAT * 2


# === DEMO ===
if __name__ == "__main__":
    print("Firefly+ Adaptive Sync Demo (vibe-coded, untested, laugh at will)")
    print("Ctrl+C to stop\n")
    scheduler = Scheduler()
    nodes = [Node(f"node_{i}", scheduler) for i in range(3)]
    for n in nodes: n.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        for n in nodes: n.stop()
        print("\nDemo stopped. Thanks for watching the chaos.")
