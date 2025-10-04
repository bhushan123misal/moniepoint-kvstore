import os
import argparse
import threading
import signal
import sys

from kvstore.engine import KVEngine
from kvstore.httpserver import make_server
from kvstore.config import (
    DEFAULT_SNAPSHOT_INTERVAL_S, DEFAULT_WAL_MAX_BYTES, DEFAULT_MEMTABLE_MB
)

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", default=os.environ.get("DATA_DIR", "./data"))
    ap.add_argument("--host", default=os.environ.get("HOST", "0.0.0.0"))
    ap.add_argument("--port", type=int, default=int(os.environ.get("PORT", "7070")))
    ap.add_argument("--snapshot-interval", type=int, default=int(os.environ.get("SNAPSHOT_INTERVAL_S", DEFAULT_SNAPSHOT_INTERVAL_S)))
    ap.add_argument("--wal-max-bytes", type=int, default=int(os.environ.get("WAL_MAX_BYTES", DEFAULT_WAL_MAX_BYTES)))
    ap.add_argument("--memtable-mb", type=int, default=int(os.environ.get("MEMTABLE_MB", DEFAULT_MEMTABLE_MB)))
    return ap.parse_args()

def main():
    args = parse_args()
    print(f"[kv] starting data_dir={args.data_dir} host={args.host} port={args.port}")

    engine = KVEngine(
        data_dir=args.data_dir,
        snapshot_interval_s=args.snapshot_interval,
        wal_max_bytes=args.wal_max_bytes,
        memtable_mb=args.memtable_mb,
    )
    engine.start()

    httpd = make_server(engine, args.host, args.port)

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
