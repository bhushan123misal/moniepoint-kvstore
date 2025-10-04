import os
import struct
import zlib
import threading
from typing import Iterable, Tuple, Callable

REC_HDR = struct.Struct("<BII")
CRC32 = struct.Struct("<I")
PUT_OP = 1
DEL_OP = 2

class WAL:
    """Append-only WAL with per-record CRC and crash-safe replay."""
    def __init__(self, wal_path: str, lock: threading.Lock):
        self.path = wal_path
        self.lock = lock
        self.f = open(self.path, "ab+", buffering=0)

    def append(self, op: int, k: bytes, v: bytes = b""):
        payload = REC_HDR.pack(op, len(k), len(v)) + k + v
        crc = CRC32.pack(zlib.crc32(payload))
        with self.lock:
            self.f.write(payload + crc)
            self.f.flush(); os.fsync(self.f.fileno())

    def append_many_group_commit(self, records: Iterable[Tuple[int, bytes, bytes]]):
        with self.lock:
            for op, k, v in records:
                payload = REC_HDR.pack(op, len(k), len(v)) + k + v
                crc = CRC32.pack(zlib.crc32(payload))
                self.f.write(payload + crc)
            self.f.flush(); os.fsync(self.f.fileno())

    def size_bytes(self) -> int:
        with self.lock:
            self.f.flush()
            return os.path.getsize(self.path)

    def replay(self, apply_fn: Callable[[int, bytes, bytes], None], max_key_bytes: int, max_val_bytes: int):
        """Replays valid records; truncates at first corruption/partial tail."""
        good_end = 0
        with open(self.path, "rb") as f:
            while True:
                hdr = f.read(REC_HDR.size)
                if len(hdr) == 0:
                    break
                if len(hdr) < REC_HDR.size:
                    break
                op, klen, vlen = REC_HDR.unpack(hdr)
                if klen > max_key_bytes or vlen > max_val_bytes:
                    break
                k = f.read(klen)
                if len(k) < klen:
                    break
                v = f.read(vlen)
                if len(v) < vlen:
                    break
                crc_bytes = f.read(CRC32.size)
                if len(crc_bytes) < CRC32.size:
                    break
                crc_expected = CRC32.unpack(crc_bytes)[0]
                if zlib.crc32(hdr + k + v) != crc_expected:
                    break
                apply_fn(op, k, v)
                good_end = f.tell()
        cur_size = os.path.getsize(self.path)
        if good_end < cur_size:
            with open(self.path, "ab") as f:
                f.truncate(good_end)

    def rotate(self) -> None:
        with self.lock:
            try:
                self.f.flush()
                self.f.close()
            except Exception:
                pass
            self.f = open(self.path, "wb+", buffering=0)
            self.f.flush(); os.fsync(self.f.fileno())

    def close(self) -> None:
        with self.lock:
            try:
                self.f.flush()
                self.f.close()
            except Exception:
                pass
