from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import urllib.parse
import json
import sys

from .engine import KVEngine
from .wal import PUT_OP, DEL_OP
from .config import DEFAULT_RANGE_LIMIT

class KVHandler(BaseHTTPRequestHandler):
    engine: KVEngine = None
    server_version = "KVHTTP/1.0"

    def _send(self, code: int, body: bytes = b"", headers: dict | None = None):
        self.send_response(code)
        if headers:
            for k, v in headers.items():
                self.send_header(k, v)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if body:
            self.wfile.write(body)

    # PUT /kv/{key}
    def do_PUT(self):
        p = urllib.parse.urlparse(self.path)
        parts = p.path.split("/")
        if len(parts) != 3 or parts[1] != "kv" or not parts[2]:
            self._send(404, b"")
            return
        key = urllib.parse.unquote(parts[2]).encode("utf-8", "strict")
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            self._send(411, b"missing length"); return
        value = self.rfile.read(length)
        try:
            KVHandler.engine.put(key, value)
            self._send(200, b"OK\n")
        except ValueError as ve:
            self._send(413, str(ve).encode())
        except OSError as oe:
            if getattr(oe, "errno", None) == 28:
                self._send(507, b"Insufficient Storage\n")
            else:
                self._send(500, f"I/O error: {oe}".encode())

    # DELETE /kv/{key}
    def do_DELETE(self):
        p = urllib.parse.urlparse(self.path)
        parts = p.path.split("/")
        if len(parts) != 3 or parts[1] != "kv" or not parts[2]:
            self._send(404, b""); return
        key = urllib.parse.unquote(parts[2]).encode("utf-8", "strict")
        try:
            KVHandler.engine.delete(key)
            self._send(200, b"OK\n")
        except ValueError as ve:
            self._send(413, str(ve).encode())
        except OSError as oe:
            if getattr(oe, "errno", None) == 28:
                self._send(507, b"Insufficient Storage\n")
            else:
                self._send(500, f"I/O error: {oe}".encode())

    # GET /kv/{key} or /kv/range
    def do_GET(self):
        p = urllib.parse.urlparse(self.path)

        if p.path == "/kv/range":
            qs = urllib.parse.parse_qs(p.query or "")
            def _get1(name, default=None):
                vals = qs.get(name)
                return vals[0] if vals else default
            start = (_get1("start", "")).encode("utf-8")
            end = (_get1("end", "\uffff")).encode("utf-8")
            after = _get1("after", None)
            after_b = after.encode("utf-8") if after is not None else None
            try:
                limit = int(_get1("limit", str(DEFAULT_RANGE_LIMIT)))
                limit = max(1, min(100000, limit))
            except ValueError:
                self._send(400, b"bad limit"); return
            try:
                rows = KVHandler.engine.iter_range(start, end, after_b, limit)
            except Exception as e:
                self._send(400, f"range error: {e}".encode()); return

            self.send_response(200)
            self.send_header("Content-Type", "application/x-ndjson")
            self.end_headers()
            for k, v in rows:
                obj = {"k": k.decode("utf-8", "strict"),
                       "v": v.decode("utf-8", "strict")}
                line = (json.dumps(obj) + "\n").encode("utf-8")
                self.wfile.write(line)
            return
        
        if p.path.startswith("/kv/"):
            parts = p.path.split("/")
            if len(parts) != 3 or not parts[2]:
                self._send(404, b""); return
            key = urllib.parse.unquote(parts[2]).encode("utf-8", "strict")
            v = KVHandler.engine.get(key)
            if v is None:
                self._send(404, b"")
            else:
                self._send(200, v, {"Content-Type": "application/octet-stream"})
            return

        self._send(404, b"")

    # POST /kv/batch
    def do_POST(self):
        p = urllib.parse.urlparse(self.path)
        if p.path != "/kv/batch":
            self._send(404, b""); return
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            self._send(411, b"missing length"); return
        body = self.rfile.read(length).decode("utf-8", "strict")
        ops = []
        for idx, line in enumerate(body.splitlines()):
            line = line.strip()
            if not line: continue
            if line.startswith("PUT "):
                try:
                    _prefix, rest = line.split(" ", 1)
                    key, value = rest.split(" ", 1)
                except ValueError:
                    self._send(400, f"bad PUT line {idx+1}".encode()); return
                ops.append((PUT_OP, key.encode("utf-8"), value.encode("utf-8")))
            elif line.startswith("DEL "):
                try:
                    _prefix, key = line.split(" ", 1)
                except ValueError:
                    self._send(400, f"bad DEL line {idx+1}".encode()); return
                ops.append((DEL_OP, key.encode("utf-8"), b""))
            else:
                self._send(400, f"bad op on line {idx+1}".encode()); return
        try:
            KVHandler.engine.batch(ops)
            self._send(200, b"OK\n")
        except ValueError as ve:
            self._send(413, str(ve).encode())
        except OSError as oe:
            if getattr(oe, "errno", None) == 28:
                self._send(507, b"Insufficient Storage\n")
            else:
                self._send(500, f"I/O error: {oe}".encode())

    def log_message(self, fmt, *args):
        sys.stdout.write("%s - - [%s] %s\n" % (
            self.address_string(),
            self.log_date_time_string(),
            fmt % args
        ))

def make_server(engine: KVEngine, host: str, port: int) -> ThreadingHTTPServer:
    KVHandler.engine = engine
    return ThreadingHTTPServer((host, port), KVHandler)
