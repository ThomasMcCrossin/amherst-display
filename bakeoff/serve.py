#!/usr/bin/env python3
"""Tailnet gallery server for the board bake-off; never serves raw API caches."""
import functools, http.server, os, sys

ROOT = os.path.dirname(os.path.abspath(__file__))

class Handler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        if self.path.split('?')[0].startswith(('/data/raw', '/serve.py')):
            self.send_error(404); return
        super().do_GET()
    def end_headers(self):
        self.send_header('Cache-Control', 'no-store')
        super().end_headers()

port = int(sys.argv[1]) if len(sys.argv) > 1 else 8794
http.server.ThreadingHTTPServer(('127.0.0.1', port), functools.partial(Handler, directory=ROOT)).serve_forever()
