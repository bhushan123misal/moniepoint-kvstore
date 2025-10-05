import json
from typing import Tuple, Dict, Any
from ..wal import PUT_OP

def handle_request_vote(raft_node, body_bytes: bytes) -> Tuple[int, Dict[str, Any], Dict[str, str]]:
    try:
        args = json.loads(body_bytes.decode("utf-8") if body_bytes else "{}")
    except Exception:
        return 400, {"error": "bad json"}, {}

    term = int(args.get("term", 0))
    cand = str(args.get("candidate_id", ""))
    last_idx = int(args.get("last_log_index", 0))
    last_term = int(args.get("last_log_term", 0))

    with raft_node._lock:
        if term < raft_node.current_term:
            return 200, {"term": raft_node.current_term, "vote_granted": False}, {"X-Raft-Term": str(raft_node.current_term)}

        if term > raft_node.current_term:
            raft_node.current_term = term
            raft_node.voted_for = None
            raft_node.role = "follower"
            raft_node.leader_id = None
            raft_node._store_state()

        up_to_date = (last_term > raft_node.last_log_term) or \
                     (last_term == raft_node.last_log_term and last_idx >= raft_node.last_log_index)

        if (raft_node.voted_for is None or raft_node.voted_for == cand) and up_to_date:
            raft_node.voted_for = cand
            raft_node._store_state()
            return 200, {"term": raft_node.current_term, "vote_granted": True}, {"X-Raft-Term": str(raft_node.current_term)}

        return 200, {"term": raft_node.current_term, "vote_granted": False}, {"X-Raft-Term": str(raft_node.current_term)}

def handle_append_entries(raft_node, body_bytes: bytes):
    try:
        args = json.loads(body_bytes.decode("utf-8") if body_bytes else "{}")
    except Exception:
        return 400, {"error": "bad json"}, {}

    term = int(args.get("term", 0))
    leader_id = str(args.get("leader_id", ""))
    entries = args.get("entries", [])
    leader_commit = int(args.get("leader_commit", 0))

    with raft_node._lock:
        if term < raft_node.current_term:
            return 200, {"term": raft_node.current_term, "success": False, "match_index": raft_node.last_log_index}, {"X-Raft-Term": str(raft_node.current_term)}

        if term > raft_node.current_term:
            raft_node.current_term = term
            raft_node.voted_for = None
            raft_node._store_state()

        raft_node.role = "follower"
        raft_node.leader_id = leader_id
        raft_node._last_heartbeat = __import__("time").monotonic()

    applied_upto = 0
    for e in entries:
        op = int(e.get("op"))
        k = str(e.get("key", "")).encode("utf-8")
        v_str = e.get("value", None)
        v = (v_str.encode("utf-8") if (v_str is not None) else b"")
        try:
            if op == PUT_OP:
                raft_node.engine.put(k, v)
            else:
                raft_node.engine.delete(k)
            applied_upto = int(e.get("index", 0))
        except Exception:
            return 200, {"term": raft_node.current_term, "success": False, "match_index": raft_node.last_log_index}, {"X-Raft-Term": str(raft_node.current_term)}

    with raft_node._lock:
        if applied_upto:
            raft_node.last_log_index = max(raft_node.last_log_index, applied_upto)
            raft_node.last_log_term = max(raft_node.last_log_term, term)
            raft_node.commit_index = max(raft_node.commit_index, min(leader_commit, raft_node.last_log_index))

        return 200, {"term": raft_node.current_term, "success": True, "match_index": raft_node.last_log_index}, {"X-Raft-Term": str(raft_node.current_term)}
