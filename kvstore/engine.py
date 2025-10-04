import os
import struct
import bisect
import threading
import traceback
from typing import Optional, Iterable, Tuple, List

from .wal import WAL, PUT_OP, DEL_OP
from .util import ensure_dir, atomic_replace, safe_fsync_file, now_ts
from .config import (
    DEFAULT_SNAPSHOT_INTERVAL_S, DEFAULT_WAL_MAX_BYTES, DEFAULT_MEMTABLE_MB,
    MAX_KEY_BYTES, MAX_VALUE_BYTES
)

class KVEngine:
    """
    In-RAM index + WAL + periodic snapshots.
    Range uses in-memory sorted keys.
    """
    def __init__(self, data_dir: str,
                 snapshot_interval_s: int = DEFAULT_SNAPSHOT_INTERVAL_S,
                 wal_max_bytes: int = DEFAULT_WAL_MAX_BYTES,
                 memtable_mb: int = DEFAULT_MEMTABLE_MB):
        self.data_dir = data_dir
        ensure_dir(self.data_dir)
        self.wal_path = os.path.join(self.data_dir, "wal.log")
        self.snapshot_path = os.path.join(self.data_dir, "snapshot.dat")
        self.snapshot_tmp_path = os.path.join(self.data_dir, "snapshot.tmp")

        self.store: dict[bytes, bytes] = {}
        self.sorted_keys: List[bytes] = []

        self.lock = threading.RLock()
        self.wal_lock = threading.Lock()
        self.wal = WAL(self.wal_path, self.wal_lock)

        self.snapshot_interval_s = snapshot_interval_s
        self.wal_max_bytes = wal_max_bytes
        self.max_mem_bytes = memtable_mb * 1024 * 1024
        self.cur_mem_bytes = 0

        self._stop_event = threading.Event()
        self._snap_thread = threading.Thread(target=self._snapshot_loop, name="SnapshotThread", daemon=True)

    def start(self):
        self._recover()
        self._snap_thread.start()

    def stop(self):
        try:
            self.snapshot(blocking=True)
        except Exception:
            try:
                safe_fsync_file(self.wal.f)
            except Exception:
                pass
        self.wal.close()
        self._stop_event.set()

    def put(self, k: bytes, v: bytes):
        self._validate_kv(k, v)
        self.wal.append(PUT_OP, k, v)
        with self.lock:
            old = self.store.get(k)
            self.store[k] = v
            self._insert_sorted_key(k)
            self._account_after_put(k, old, v)
        self._maybe_rotate_or_snapshot()

    def delete(self, k: bytes):
        self._validate_key(k)
        self.wal.append(DEL_OP, k, b"")
        with self.lock:
            old = self.store.pop(k, None)
            self._remove_sorted_key(k)
            if old is not None:
                self.cur_mem_bytes -= self._mem_cost(k, old)
        self._maybe_rotate_or_snapshot()

    def batch(self, ops: Iterable[Tuple[int, bytes, bytes]]):
        pending = []
        for op, k, v in ops:
            self._validate_key(k)
            if op == PUT_OP:
                self._validate_val(v)
            pending.append((op, k, (v if op == PUT_OP else b"")))
        self.wal.append_many_group_commit(pending)
        with self.lock:
            for op, k, v in pending:
                old = self.store.get(k)
                if op == PUT_OP:
                    self.store[k] = v
                    self._insert_sorted_key(k)
                    self._account_after_put(k, old, v)
                else:
                    if old is not None:
                        self.cur_mem_bytes -= self._mem_cost(k, old)
                    self.store.pop(k, None)
                    self._remove_sorted_key(k)
        self._maybe_rotate_or_snapshot()

    def get(self, k: bytes) -> Optional[bytes]:
        with self.lock:
            return self.store.get(k)

    def iter_range(self, start: bytes, end: bytes, after: Optional[bytes], limit: int) -> Iterable[Tuple[bytes, bytes]]:
        if start > end:
            return []
        with self.lock:
            keys = self.sorted_keys
            i = bisect.bisect_left(keys, start)
            if after:
                i = max(i, bisect.bisect_right(keys, after))
            out = []
            while i < len(keys) and len(out) < limit:
                k = keys[i]
                if k > end:
                    break
                v = self.store.get(k)
                if v is not None:
                    out.append((k, v))
                i += 1
            return out

    def _validate_key(self, k: bytes):
        if not (1 <= len(k) <= MAX_KEY_BYTES):
            raise ValueError("key size invalid")

    def _validate_val(self, v: bytes):
        if not (0 <= len(v) <= MAX_VALUE_BYTES):
            raise ValueError("value size invalid")

    def _validate_kv(self, k: bytes, v: bytes):
        self._validate_key(k); self._validate_val(v)

    def _insert_sorted_key(self, k: bytes):
        i = bisect.bisect_left(self.sorted_keys, k)
        if i >= len(self.sorted_keys) or self.sorted_keys[i] != k:
            self.sorted_keys.insert(i, k)

    def _remove_sorted_key(self, k: bytes):
        i = bisect.bisect_left(self.sorted_keys, k)
        if i < len(self.sorted_keys) and self.sorted_keys[i] == k:
            self.sorted_keys.pop(i)

    def _mem_cost(self, k: bytes, v: bytes) -> int:
        return len(k) + len(v) + 16

    def _account_after_put(self, k: bytes, old: Optional[bytes], newv: bytes):
        if old is None:
            self.cur_mem_bytes += self._mem_cost(k, newv)
        else:
            self.cur_mem_bytes += (len(newv) - len(old))

    def _recover(self):
        if os.path.exists(self.snapshot_path):
            with open(self.snapshot_path, "rb") as f:
                while True:
                    hdr = f.read(8)
                    if not hdr:
                        break
                    if len(hdr) < 8:
                        break
                    klen, vlen = struct.unpack("<II", hdr)
                    if not (0 < klen <= MAX_KEY_BYTES) or not (0 <= vlen <= MAX_VALUE_BYTES):
                        break
                    k = f.read(klen)
                    if len(k) < klen:
                        break
                    v = f.read(vlen)
                    if len(v) < vlen:
                        break
                    self.store[k] = v
                    self._insert_sorted_key(k)
                    self.cur_mem_bytes += self._mem_cost(k, v)

        def apply_fn(op, k, v):
            if op == PUT_OP:
                old = self.store.get(k)
                self.store[k] = v
                self._insert_sorted_key(k)
                self._account_after_put(k, old, v)
            elif op == DEL_OP:
                old = self.store.pop(k, None)
                self._remove_sorted_key(k)
                if old is not None:
                    self.cur_mem_bytes -= self._mem_cost(k, old)

        self.wal.replay(apply_fn, MAX_KEY_BYTES, MAX_VALUE_BYTES)

    def snapshot(self, blocking: bool = True):
        with self.lock:
            items = list(self.store.items())
        tmp = self.snapshot_tmp_path
        with open(tmp, "wb") as f:
            for k, v in sorted(items, key=lambda kv: kv[0]):
                f.write(struct.pack("<II", len(k), len(v)))
                f.write(k); f.write(v)
            f.flush(); os.fsync(f.fileno())
        atomic_replace(tmp, self.snapshot_path)

    def _maybe_rotate_or_snapshot(self):
        if self.cur_mem_bytes >= self.max_mem_bytes:
            self.snapshot(blocking=True)
            self.wal.rotate()
            with self.lock:
                self.cur_mem_bytes = sum(self._mem_cost(k, v) for k, v in self.store.items())
            return
        if self.wal.size_bytes() >= self.wal_max_bytes:
            self.snapshot(blocking=True)
            self.wal.rotate()

    def _snapshot_loop(self):
        last = now_ts()
        while not self._stop_event.is_set():
            self._stop_event.wait(1.0)
            now = now_ts()
            if now - last >= self.snapshot_interval_s:
                try:
                    self.snapshot(blocking=True)
                    if self.wal.size_bytes() >= self.wal_max_bytes // 2:
                        self.wal.rotate()
                except Exception:
                    traceback.print_exc()
                last = now
