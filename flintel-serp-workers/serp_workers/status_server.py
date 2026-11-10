"""Tiny stdlib HTTP server: GET /health, GET /status (JSON), GET / (plain-text table of all workers)."""
import json
import logging
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

log = logging.getLogger("flintel.status")


def start_status_server(board, port: int):
    class H(BaseHTTPRequestHandler):
        def _send(self, code, body, ctype):
            data = body.encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self):
            if self.path.startswith("/health"):
                self._send(200, "ok", "text/plain")
            elif self.path.startswith("/status"):
                self._send(200, json.dumps(board.snapshot(), default=str), "application/json")
            else:
                self._send(200, board.render_text(), "text/plain; charset=utf-8")

        def log_message(self, *a):
            pass

    srv = ThreadingHTTPServer(("0.0.0.0", port), H)
    threading.Thread(target=srv.serve_forever, daemon=True, name="status-http").start()
    log.info(f"Status page on :{port}  (/ = all workers, /status = JSON, /health)")
    return srv
