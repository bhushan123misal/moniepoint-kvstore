import os
import time
import json
import random
import urllib.request
import urllib.error

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

def rand_ms(a, b) -> float:
    return random.uniform(a, b) / 1000.0

def http_post_json(url: str, payload: dict, timeout: float = 2.0) -> tuple[int, dict]:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read()
            return resp.status, (json.loads(body.decode()) if body else {})
    except urllib.error.HTTPError as e:
        try:
            body = e.read()
            return e.code, (json.loads(body.decode()) if body else {})
        except Exception:
            return e.code, {}
    except Exception:
        return 0, {}

def http_get_json(url: str, timeout: float = 2.0) -> tuple[int, dict]:
    req = urllib.request.Request(url, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read()
            return resp.status, (json.loads(body.decode()) if body else {})
    except urllib.error.HTTPError as e:
        try:
            body = e.read()
            return e.code, (json.loads(body.decode()) if body else {})
        except Exception:
            return e.code, {}
    except Exception:
        return 0, {}
