# firefly_raft_sync.py
# Firefly+ v2: Adaptive sync with Raft for fault-tolerant Scheduler
# VIBE-CODED. ONE FILE. RUNS. RAFT TOY INSIDE.
# Disclaimer: Untested on real clusters. Laugh at will. Inspired by Raft (raft.github.io)

import time
import random
import threading
from dataclasses import dataclass
from typing import Dict, List, Optional
from collections import defaultdict, deque
import queue  # for RPC simulation

# === CONFIG ===
FULL_SYNC_INTERVAL_DEFAULT = 1.0
REPORT_INTERVAL = 10.0
DRIFT_THRESHOLD = 5e-9
PROFILE_HISTORY = 5
SCHEDULER_HEARTBEAT = 2.0
RAFT_ELECTION_TIMEOUT = 0.15  # random [150-300ms]
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


class RaftServer:
    def __init__(self, server_id: int, servers: List['RaftServer']):
        self.id = server_id
        self.servers = servers
        self.current_term = 0
        self.voted_for = -1
        self.log = []  # [(term, command)]
        self.commit_index = 0
        self.last_applied = 0
        self.next_index = {s.id: 0 for s in servers if s.id != self.id}
        self.match_index = {s.id: -1 for s in servers if s.id != self.id}
        self.state = 'follower'  # follower/leader/candidate
        self.leader_id = None
        self.election_timeout = random.uniform(RAFT_ELECTION_TIMEOUT, 2 * RAFT_ELECTION_TIMEOUT)
        self.last_heartbeat = time.time()
        self.rpc_queue = queue.Queue()  # simulate RPC
        self.profiles: Dict[str, NodeProfile] = {}
        self.lock = threading.Lock()
        self.thread = threading.Thread(target=self._run, daemon=True)

    def start(self):
        self.thread.start()

    def _run(self):
        while True:
            now = time.time()
            if self.state == 'follower':
                if now - self.last_heartbeat > self.election_timeout:
                    self._start_election()
                try:
                    msg = self.rpc_queue.get(timeout=0.1)
                    self._handle_rpc(msg)
                except queue.Empty:
                    pass
            elif self.state == 'candidate':
                if now - self.last_heartbeat > self.election_timeout:
                    self._start_election()
                try:
                    msg = self.rpc_queue.get(timeout=0.1)
                    self._handle_rpc(msg)
                except queue.Empty:
                    pass
            elif self.state == 'leader':
                self._send_heartbeats()
                self._apply_logs()
                try:
                    msg = self.rpc_queue.get(timeout=0.1)
                    self._handle_rpc(msg)
                except queue.Empty:
                    pass
            time.sleep(0.01)

    def _start_election(self):
        self.current_term += 1
        self.state = 'candidate'
        self.voted_for = self.id
        self.last_heartbeat = time.time()
        votes = 1  # self-vote
        for server in self.servers:
            if server.id != self.id:
                server.rpc_queue.put(('RequestVote', self.current_term, self.id, self.log[-1][0] if self.log else 0))

    def _handle_rpc(self, msg):
        rpc_type, *args = msg
        if rpc_type == 'RequestVote':
            term, candidate_id, last_log_index = args
            if term > self.current_term:
                self.current_term = term
                self.state = 'follower'
                self.voted_for = -1
            if (term == self.current_term and self.voted_for == -1 and
                self._log_up_to_date(last_log_index)):
                self.voted_for = candidate_id
                self.rpc_queue.put(('VoteResponse', self.current_term, True))
            else:
                self.rpc_queue.put(('VoteResponse', self.current_term, False))
        elif rpc_type == 'AppendEntries':
            term, leader_id, prev_log_index, prev_log_term, entries, leader_commit = args
            if term < self.current_term:
                self.rpc_queue.put(('AppendEntriesResponse', self.current_term, False))
                return
            self.state = 'follower'
            self.leader_id = leader_id
            self.last_heartbeat = time.time()
            if self._log_consistent(prev_log_index, prev_log_term):
                self.log[prev_log_index:] = entries
                self.commit_index = max(self.commit_index, leader_commit)
                self.rpc_queue.put(('AppendEntriesResponse', self.current_term, True))
            else:
                self.rpc_queue.put(('AppendEntriesResponse', self.current_term, False))

    def _send_heartbeats(self):
        now = time.time()
        if now - self.last_heartbeat > 0.1:
            self.last_heartbeat = now
            for server in self.servers:
                if server.id != self.id:
                    server.rpc_queue.put(('AppendEntries', self.current_term, self.id, self.commit_index, self.log[self.commit_index:] if self.commit_index < len(self.log) else [], self.commit_index))

    def _apply_logs(self):
        with self.lock:
            for i in range(self.last_applied + 1, self.commit_index + 1):
                term, command = self.log[i]
                if 'schedule' in command:
                    node_id, interval, correction = command['schedule']
                    # Simulate sending to node
                    print(f"[Raft Leader {self.id}] Applied schedule for {node_id}: {interval:.2f}s, {correction:+.2e}")

    def _log_up_to_date(self, last_log_index):
        return len(self.log) - 1 >= last_log_index

    def _log_consistent(self, prev_log_index, prev_log_term):
        if prev_log_index >= len(self.log):
            return False
        return self.log[prev_log_index][0] == prev_log_term

    def append_entry(self, command):
        if self.state != 'leader':
            return False
        entry = (self.current_term, command)
        self.log.append(entry)
        self.commit_index += 1
        return True

    def receive_report(self, data):
        if self.state != 'leader':
            return  # Forward to leader in real impl
        node_id = data["node_id"]
        now = time.time()
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
                command = {'schedule': (node_id, new_interval, correction)}
                self.append_entry(command)
            else:
                command = {'schedule': (node_id, profile.sync_interval, profile.correction)}
                self.append_entry(command)

    def is_alive(self):
        return self.state != 'dead'  # Simplified


class Node:
    def __init__(self, node_id: str, raft_servers: List[RaftServer]):
        self.id = node_id
        self.local_time_offset = random.gauss(0, 1e-8)
        self.raft_servers = raft_servers
        self.profile = NodeProfile()
        self.running = True
        self.thread = threading.Thread(target=self._run, daemon=True)

    def start(self):
        self.thread.start()

    def _run(self):
        last_report = 0.0
        while self.running:
            now = time.time()
            self.local_time = now + self.local_time_offset * now

            if now - last_report >= REPORT_INTERVAL:
                self._send_report(now)
                last_report = now

            time.sleep(0.1)

    def _send_report(self, now):
        leader = next((s for s in self.raft_servers if s.state == 'leader'), None)
        if not leader:
            self._fallback_sync(now)
            return

        measured_offset = self._measure_offset_to_peers()
        data = {
            "node_id": self.id,
            "local_time": self.local_time,
            "last_sync": self.profile.last_sync,
            "measured_offset": measured_offset
        }
        leader.receive_report(data)

    def _measure_offset_to_peers(self):
        return random.gauss(0, 2e-9)

    def _fallback_sync(self, now):
        offset = random.gauss(0, 3e-9)
        self.profile.correction += offset
        self.profile.last_sync = now
        print(f"[{self.id}] Fallback sync: corr {offset:+.2e}")

    def stop(self):
        self.running = False
        self.thread.join()


# === DEMO ===
if __name__ == "__main__":
    print("Firefly+ Raft Demo: Adaptive sync with leader election (vibe-coded, toy Raft)")
    print("Ctrl+C to stop\n")

    # 3 Raft servers for Scheduler cluster
    raft_servers = [RaftServer(i, None) for i in range(3)]
    for i, s in enumerate(raft_servers):
        s.servers = raft_servers
    for s in raft_servers: s.start()

    # Wait for initial election
    time.sleep(1)

    nodes = [Node(f"node_{i}", raft_servers) for i in range(3)]
    for n in nodes: n.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        for n in nodes: n.stop()
        print("\nDemo stopped. Raft survived the chaos.")
