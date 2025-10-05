from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import urllib.parse
import json
import sys

from .engine import KVEngine
from .wal import PUT_OP, DEL_OP
from .config import DEFAULT_RANGE_LIMIT
from .raft import rpc as raft_rpc

class KVHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1" 
    def _send(self, code: int, body: bytes = b"", headers: dict | None = None):
        self.send_response(code)
        if headers:
            for k, v in headers.items():
                self.send_header(k, v)
        self.send_header("Content-Type", "application/json" if body.startswith(b"{") else "application/octet-stream")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if body:
            self.wfile.write(body)

    def _send_json(self, code: int, obj: dict, extra_headers: dict | None = None):
        body = json.dumps(obj).encode("utf-8")
        headers = {"Content-Type": "application/json"}
        if extra_headers:
            headers.update(extra_headers)
        self._send(code, body, headers)

    def _maybe_raft(self):
        if self.path == "/raft/whois" and self.command == "GET":
            info = {
                "role": self.raft_node.role if self.raft_node else "single",
                "term": self.raft_node.current_term if self.raft_node else 0,
                "leaderId": self.raft_node.leader_id if self.raft_node else None,
                "leaderAddr": self.raft_node.leader_addr() if self.raft_node else None,
            }
            self._send(200, json.dumps(info).encode("utf-8"))
            return True

        if self.path == "/raft/request_vote" and self.command == "POST":
            length = int(self.headers.get("Content-Length", "0") or "0")
            body = self.rfile.read(length) if length > 0 else b""
            code, js, extra = raft_rpc.handle_request_vote(self.raft_node, body)
            self._send(code, json.dumps(js).encode("utf-8"), extra)
            return True

        if self.path == "/raft/append_entries" and self.command == "POST":
            length = int(self.headers.get("Content-Length", "0") or "0")
            body = self.rfile.read(length) if length > 0 else b""
            code, js, extra = raft_rpc.handle_append_entries(self.raft_node, body)
            self._send(code, json.dumps(js).encode("utf-8"), extra)
            return True
        
        if self.path == "/raft/health" and self.command == "GET":
            info = self.raft_node.health()
            self._send_json(200, info)
            return True

        if self.path == "/cluster/leader" and self.command == "GET":
            info = self.raft_node.health()
            leader = info.get("leaderAddr")
            self._send_json(200, {
                "clusterId": self.raft_node.cluster_id,
                "leaderAddr": leader,
                "leaderId": info.get("leaderId"),
                "term": info.get("term"),
                "source": info.get("addr")
            })
            return True

        if self.path == "/cluster/nodes" and self.command == "GET":
            out = []
            out.append({"up": True, **self.raft_node.health()})
            for nid, addr in self.raft_node.peers.items():
                if nid == self.raft_node.node_id:
                    continue
                out.append(self.raft_node.probe_peer_health(addr))
            self._send_json(200, {
                "clusterId": self.raft_node.cluster_id,
                "nodes": out
            })
            return True

        return False

        return False

    def do_GET(self):
        if self.raft_node and self._maybe_raft():
            return
        
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
                self.wfile.flush()
            self.close_connection = True 
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

    def do_PUT(self):
        if self.raft_node and self._maybe_raft():
            return

        if self.raft_node and not self.raft_node.is_leader():
            leader = self.raft_node.leader_addr()
            js = {"error":"not_leader","leader":leader}
            self._send(409, json.dumps(js).encode("utf-8"), {"Content-Type":"application/json"})
            return

        # PUT /kv/{key}
        p = urllib.parse.urlparse(self.path)
        parts = p.path.split("/")
        if len(parts) != 3 or parts[1] != "kv" or not parts[2]:
            self._send(404, b""); return

        key = urllib.parse.unquote(parts[2]).encode("utf-8", "strict")
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            self._send(411, b"missing length"); return
        value = self.rfile.read(length)

        try:
            KVHandler.engine.put(key, value)
            if self.raft_node:
                ok = self.raft_node.replicate_and_commit([(PUT_OP, key, value)], wait_majority=True)
                if not ok:
                    self._send(503, b"write not committed by majority\n"); return
            self._send(200, b"OK\n")
        except ValueError as ve:
            self._send(413, str(ve).encode())
        except OSError as oe:
            if getattr(oe, "errno", None) == 28:
                self._send(507, b"Insufficient Storage\n")
            else:
                self._send(500, f"I/O error: {oe}".encode())

    def do_DELETE(self):
        if self.raft_node and self._maybe_raft():
            return

        if self.raft_node and not self.raft_node.is_leader():
            leader = self.raft_node.leader_addr()
            js = {"error":"not_leader","leader":leader}
            self._send(409, json.dumps(js).encode("utf-8"), {"Content-Type":"application/json"})
            return

        p = urllib.parse.urlparse(self.path)
        parts = p.path.split("/")
        if len(parts) != 3 or parts[1] != "kv" or not parts[2]:
            self._send(404, b""); return
        key = urllib.parse.unquote(parts[2]).encode("utf-8", "strict")
        try:
            KVHandler.engine.delete(key)
            if self.raft_node:
                ok = self.raft_node.replicate_and_commit([(DEL_OP, key, b"")], wait_majority=True)
                if not ok:
                    self._send(503, b"write not committed by majority\n"); return
            self._send(200, b"OK\n")
        except ValueError as ve:
            self._send(413, str(ve).encode())
        except OSError as oe:
            if getattr(oe, "errno", None) == 28:
                self._send(507, b"Insufficient Storage\n")
            else:
                self._send(500, f"I/O error: {oe}".encode())

    def do_POST(self):
        if self.raft_node and self._maybe_raft():
            return

        if self.path != "/kv/batch":
            self._send(404, b""); return

        if self.raft_node and not self.raft_node.is_leader():
            leader = self.raft_node.leader_addr()
            js = {"error":"not_leader","leader":leader}
            self._send(409, json.dumps(js).encode("utf-8"), {"Content-Type":"application/json"})
            return

        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            self._send(411, b"missing length"); return
        body = self.rfile.read(length).decode("utf-8", "strict")
        ops = []
        engine_ops = []
        for idx, line in enumerate(body.splitlines()):
            line = line.strip()
            if not line: continue
            if line.startswith("PUT "):
                try:
                    _p, rest = line.split(" ", 1)
                    k, v = rest.split(" ", 1)
                except ValueError:
                    self._send(400, f"bad PUT line {idx+1}".encode()); return
                ops.append((1, k.encode("utf-8"), v.encode("utf-8")))
                engine_ops.append((1, k.encode("utf-8"), v.encode("utf-8")))
            elif line.startswith("DEL "):
                try:
                    _p, k = line.split(" ", 1)
                except ValueError:
                    self._send(400, f"bad DEL line {idx+1}".encode()); return
                ops.append((2, k.encode("utf-8"), b""))
                engine_ops.append((2, k.encode("utf-8"), b""))
            else:
                self._send(400, f"bad op on line {idx+1}".encode()); return
        try:
            KVHandler.engine.batch(engine_ops)
            if self.raft_node:
                ok = self.raft_node.replicate_and_commit(ops, wait_majority=True)
                if not ok:
                    self._send(503, b"write not committed by majority\n"); return
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

def make_server(engine: KVEngine, raft_node, host: str, port: int) -> ThreadingHTTPServer:
    KVHandler.engine = engine
    KVHandler.raft_node = raft_node
    return ThreadingHTTPServer((host, port), KVHandler)
