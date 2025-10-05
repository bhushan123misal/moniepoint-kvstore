import os
import argparse
import threading
import signal

from kvstore.engine import KVEngine
from kvstore.httpserver import make_server
from kvstore.raft.node import RaftNode

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", default=os.environ.get("DATA_DIR", "./data"))
    ap.add_argument("--host", default=os.environ.get("HOST", "127.0.0.1"))
    ap.add_argument("--port", type=int, default=int(os.environ.get("PORT", "7070")))
    ap.add_argument("--node-id", default=os.environ.get("NODE_ID", "n1"))
    ap.add_argument("--cluster-id", default=os.environ.get("CLUSTER_ID", "kv-demo-1"))
    ap.add_argument("--peers", default=os.environ.get("PEERS", ""))
    ap.add_argument("--heartbeat-ms", type=int, default=int(os.environ.get("HEARTBEAT_MS", "150")))
    ap.add_argument("--election-min-ms", type=int, default=int(os.environ.get("ELECTION_MIN_MS", "800")))
    ap.add_argument("--election-max-ms", type=int, default=int(os.environ.get("ELECTION_MAX_MS", "1500")))
    return ap.parse_args()

def parse_peers(peers_str: str):
    d = {}
    if not peers_str:
        return d
    for part in peers_str.split(","):
        part = part.strip()
        if not part: continue
        nid, addr = part.split("=", 1)
        d[nid.strip()] = addr.strip()
    return d

def main():
    args = parse_args()
    print(f"[kv] starting data_dir={args.data_dir} host={args.host} port={args.port} node={args.node_id}")

    engine = KVEngine(
        data_dir=args.data_dir,
    )
    engine.start()

    peers = parse_peers(args.peers)
    self_addr = f"http://{args.host}:{args.port}" 
    raft_node = RaftNode(
        data_dir=args.data_dir,
        node_id=args.node_id,
        self_addr=self_addr,
        peers=peers,
        engine=engine,
        cluster_id=args.cluster_id,
        heartbeat_ms=args.heartbeat_ms,
        election_min_ms=args.election_min_ms,
        election_max_ms=args.election_max_ms,
    )
    raft_node.start()

    httpd = make_server(engine, raft_node, args.host, args.port)

    shutting_down = {"flag": False}
    def _shutdown(sig, _):
        if shutting_down["flag"]:
            return
        shutting_down["flag"] = True
        print(f"[kv] signal {sig} received; shutting down...")
        def _stop_server():
            try:
                httpd.shutdown()
            except Exception:
                pass
        t = threading.Thread(target=_stop_server, daemon=True)
        t.start()
        try:
            raft_node.stop()
            engine.stop()
        finally:
            os._exit(0)

    for s in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(s, _shutdown)
        except Exception:
            pass

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        _shutdown(signal.SIGINT, None)

if __name__ == "__main__":
    main()
