# firefly_plus_drift.py
# Firefly + Predictive Drift Compensation (5s sync, <10ns error)
# VIBE-CODED IN ~10 MINUTES. NOT TESTED. MIGHT EXPLODE.

import numpy as np
from collections import deque

class FireflyPlus:
    def __init__(self, node_id, peers=16, full_sync_interval=5.0):
        self.id = node_id
        self.peers = peers
        self.full_sync_interval = full_sync_interval
        self.last_full_sync = 0.0
        self.local_time = 0.0
        self.rate_offset = 0.0
        self.accel = 0.0
        self.history = deque(maxlen=10)

    def predict_offset(self, now):
        dt = now - self.last_full_sync
        return self.rate_offset * dt + 0.5 * self.accel * dt**2

    def light_probe(self, peer_time, rtt):
        delay = rtt / 2
        measured_offset = peer_time - (self.local_time + delay)
        predicted = self.predict_offset(self.local_time)
        return abs(measured_offset - predicted) < 3e-9

    def full_sync(self, offsets):
        offset = np.median(offsets)
        self.history.append((self.local_time, offset))
        if len(self.history) >= 3:
            self._fit_drift_model()
        self.last_full_sync = self.local_time
        return offset

    def _fit_drift_model(self):
        t, o = zip(*self.history)
        t = np.array(t); o = np.array(o)
        A = np.vstack([t, np.ones(len(t))]).T
        self.rate_offset, _ = np.linalg.lstsq(A, o, rcond=None)[0]

    def tick(self, now, peer_samples=None):
        self.local_time = now
        if now - self.last_full_sync >= self.full_sync_interval:
            if peer_samples:
                return self.full_sync(peer_samples)
        elif peer_samples and len(peer_samples) == 1:
            rtt = 1e-6
            if not self.light_probe(peer_samples[0], rtt):
                print(f"[!] Drift error >3ns → trigger early sync!")
                return self.full_sync(peer_samples * 5)
        return self.predict_offset(now)
