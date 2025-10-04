import os
import time

def now_ts() -> int:
    return int(time.time())

def ensure_dir(path: str):
    if not os.path.isdir(path):
        os.makedirs(path, exist_ok=True)

def safe_fsync_file(f):
    f.flush()
    os.fsync(f.fileno())

def safe_dir_fsync(path: str):
    try:
        if os.name == "nt":
            return
        dirfd = os.open(path, os.O_RDONLY)
        try:
            os.fsync(dirfd)
        finally:
            os.close(dirfd)
    except Exception:
        pass

def atomic_replace(tmp_path: str, final_path: str):
    os.replace(tmp_path, final_path)
    safe_dir_fsync(os.path.dirname(final_path) or ".")
