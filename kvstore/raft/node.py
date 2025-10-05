import json
import os
import random
import threading
import time
from typing import Dict, List, Optional, Tuple
from urllib import request, error

from ..wal import PUT_OP, DEL_OP
from ..config import MAX_KEY_BYTES, MAX_VALUE_BYTES
from ..util import ensure_dir
from .types import LogEntry

ROLE_FOLLOWER = "follower"
ROLE_CANDIDATE = "candidate"
ROLE_LEADER = "leader"

class RaftNode:
    """
    Raft-lite: leader election + heartbeats + write replication + failover.
    - stdlib only (urllib for RPC)
    - single-leader, majority-ack replication
    - no per-follower backtracking (kept simple for demo)
    """

    def __init__(
        self,
        data_dir: str,
        node_id: str,
        self_addr: str,
        peers: Dict[str, str],
        engine,
        cluster_id: str = "kv-demo",
        heartbeat_ms: int = 150,
        election_min_ms: int = 300,
        election_max_ms: int = 500,
    ):
        ensure_dir(data_dir)
        self.raft_dir = os.path.join(data_dir, "raft")
        ensure_dir(self.raft_dir)
        self.state_path = os.path.join(self.raft_dir, "state.json")

        self.cluster_id = cluster_id
        self.node_id = node_id
        self.self_addr = self_addr
        self.peers = dict(peers)
        self.engine = engine

        self.current_term = 0
        self.voted_for: Optional[str] = None

        self.role = ROLE_FOLLOWER
        self.leader_id: Optional[str] = None
        self.commit_index = 0
        self.last_applied = 0
        self.last_log_index = 0
        self.last_log_term = 0

        self._election_min_ms = election_min_ms
        self._election_max_ms = election_max_ms
        self._heartbeat_ms = heartbeat_ms

        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._last_heartbeat = 0.0
        self._threads: List[threading.Thread] = []

        self._load_state()

    def start(self):
        t1 = threading.Thread(target=self._election_loop, name=f"raft-elect-{self.node_id}", daemon=True)
        t2 = threading.Thread(target=self._heartbeat_loop, name=f"raft-hb-{self.node_id}", daemon=True)
        t1.start(); t2.start()
        self._threads.extend([t1, t2])

    def stop(self):
        self._stop.set()

    def _load_state(self):
        if os.path.exists(self.state_path):
            try:
                with open(self.state_path, "r", encoding="utf-8") as f:
                    s = json.load(f)
                    self.current_term = int(s.get("current_term", 0))
                    self.voted_for = s.get("voted_for", None)
            except Exception:
                pass

    def _store_state(self):
        tmp = self.state_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"current_term": self.current_term, "voted_for": self.voted_for}, f)
            f.flush(); os.fsync(f.fileno())
        os.replace(tmp, self.state_path)

    def majority(self) -> int:
        return (len(self.peers) // 2) + 1

    def is_leader(self) -> bool:
        with self._lock:
            return self.role == ROLE_LEADER

    def leader_addr(self) -> Optional[str]:
        with self._lock:
            lid = self.leader_id
        return self.peers.get(lid) if lid else None

    def _election_loop(self):
        while not self._stop.is_set():
            tout = random.uniform(self._election_min_ms/1000.0, self._election_max_ms/1000.0)
            fired = self._stop.wait(tout)
            if fired: break
            with self._lock:
                if time.monotonic() - self._last_heartbeat < tout:
                    continue
                self.role = ROLE_CANDIDATE
                self.current_term += 1
                self.voted_for = self.node_id
                self._store_state()
                term = self.current_term
                last_idx = self.last_log_index
                last_term = self.last_log_term
            votes = 1
            for pid, addr in self.peers.items():
                if pid == self.node_id: continue
                try:
                    ok, peer_term = self._rpc_post_json(addr + "/raft/request_vote", {
                        "term": term,
                        "candidate_id": self.node_id,
                        "last_log_index": last_idx,
                        "last_log_term": last_term,
                    })
                    if not isinstance(ok, dict):
                        continue
                    if peer_term and isinstance(peer_term, int) and peer_term > term:
                        with self._lock:
                            if peer_term > self.current_term:
                                self.current_term = peer_term
                                self.voted_for = None
                                self.role = ROLE_FOLLOWER
                                self._store_state()
                        break
                    if ok.get("vote_granted"):
                        votes += 1
                except Exception:
                    pass
            with self._lock:
                if self.role == ROLE_FOLLOWER:
                    continue
                if votes >= self.majority():
                    self.role = ROLE_LEADER
                    self.leader_id = self.node_id
                    self._last_heartbeat = time.monotonic()
                else:
                    self.role = ROLE_FOLLOWER
                    self.voted_for = None
                    self._store_state()

    def _heartbeat_loop(self):
        while not self._stop.is_set():
            time.sleep(self._heartbeat_ms / 1000.0)
            with self._lock:
                if self.role != ROLE_LEADER:
                    continue
                term = self.current_term
                leader_id = self.node_id
                commit_index = self.commit_index
                prev_idx = self.last_log_index
                prev_term = self.last_log_term
            payload = {
                "term": term,
                "leader_id": leader_id,
                "prev_log_index": prev_idx,
                "prev_log_term": prev_term,
                "entries": [], 
                "leader_commit": commit_index,
            }
            for pid, addr in self.peers.items():
                if pid == self.node_id: continue
                try:
                    _, peer_term = self._rpc_post_json(addr + "/raft/append_entries", payload)
                    if isinstance(peer_term, int) and peer_term > term:
                        with self._lock:
                            self.current_term = peer_term
                            self.voted_for = None
                            self.role = ROLE_FOLLOWER
                            self.leader_id = None
                            self._store_state()
                except Exception:
                    pass


    def replicate_and_commit(self, ops: List[Tuple[int, bytes, bytes]], wait_majority: bool = True) -> bool:
        """
        Leader-only: replicate ops to followers; return True if committed by majority.
        ops: list of (op, key_bytes, val_bytes)
        """
        with self._lock:
            if self.role != ROLE_LEADER:
                return False
            self.last_log_index += 1
            entry_index = self.last_log_index
            self.last_log_term = self.current_term
            entries = []
            for op, k, v in ops:
                if not (1 <= len(k) <= MAX_KEY_BYTES): raise ValueError("key size invalid")
                if op == PUT_OP and not (0 <= len(v) <= MAX_VALUE_BYTES): raise ValueError("value size invalid")
                entries.append({
                    "index": entry_index,
                    "term": self.current_term,
                    "op": op,
                    "key": k.decode("utf-8", "strict"),
                    "value": (v.decode("utf-8", "strict") if op == PUT_OP else None),
                })
            term = self.current_term
            prev_idx = entry_index - 1
            prev_term = self.last_log_term if prev_idx == entry_index else self.last_log_term
            leader_commit = self.commit_index
            leader_id = self.node_id
            payload = {
                "term": term, "leader_id": leader_id,
                "prev_log_index": prev_idx, "prev_log_term": prev_term,
                "entries": entries, "leader_commit": leader_commit,
            }

        acks = 1
        for pid, addr in self.peers.items():
            if pid == self.node_id: continue
            try:
                res, peer_term = self._rpc_post_json(addr + "/raft/append_entries", payload, timeout=2.0)
                if isinstance(peer_term, int) and peer_term > term:
                    with self._lock:
                        self.current_term = peer_term
                        self.voted_for = None
                        self.role = ROLE_FOLLOWER
                        self.leader_id = None
                        self._store_state()
                    return False
                if isinstance(res, dict) and res.get("success"):
                    acks += 1
            except Exception:
                pass

        if wait_majority:
            if acks >= self.majority():
                with self._lock:
                    self.commit_index = max(self.commit_index, entry_index)
                return True
            return False
        else:
            return True

    def _rpc_post_json(self, url: str, obj: dict, timeout: float = 1.0):
        data = json.dumps(obj).encode("utf-8")
        req = request.Request(url, data=data, headers={"Content-Type": "application/json"}, method="POST")
        try:
            with request.urlopen(req, timeout=timeout) as resp:
                body = resp.read()
                peer_term = resp.headers.get("X-Raft-Term")
                try:
                    peer_term = int(peer_term) if peer_term is not None else None
                except Exception:
                    peer_term = None
                if not body:
                    return {}, peer_term
                return json.loads(body.decode("utf-8")), peer_term
        except error.HTTPError as e:
            try:
                body = e.read()
                js = json.loads(body.decode("utf-8"))
                return js, None
            except Exception:
                raise
    
    def health(self) -> dict:
        with self._lock:
            return {
                "nodeId": self.node_id,
                "addr": self.self_addr,
                "role": self.role,
                "term": self.current_term,
                "leaderId": self.leader_id,
                "leaderAddr": self.leader_addr(),
                "commitIndex": self.commit_index,
            }

    def probe_peer_health(self, addr: str, timeout: float = 0.8) -> dict:
        """Probe a peer's /raft/health; returns dict with up/down and optional fields."""
        url = addr.rstrip("/") + "/raft/health"
        try:
            with request.urlopen(url, timeout=timeout) as r:
                js = json.load(r)
            return {"up": True, **js}
        except Exception:
            return {"up": False, "addr": addr}