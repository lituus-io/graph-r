# Copyright: lituus-io, all rights reserved.
# Author: terekete <spicyzhug@gmail.com>
#
# The whole loop, end to end, against a local conditional-GET server: crawl,
# absorb, DISCARD the index, reopen the store in a fresh session, and sync
# again — unchanged pages must revalidate via the graph's stored validators
# and transfer no body. This is the stateless design promise, observable from
# Python: bodies and vectors never outlive a call, yet re-syncing stays free.

import http.server
import socketserver
import threading

import graph_r

PAGES = {
    "/docs": (
        '"r1"',
        "<html><body><h1>Root</h1><p>root words about widgets</p>"
        '<a href="/docs/install">install</a><a href="/docs/api">api</a></body></html>',
    ),
    "/docs/install": (
        '"i1"',
        "<html><body><h1>Install on Linux</h1>"
        "<p>installation on linux uses the widget toolchain</p></body></html>",
    ),
    "/docs/api": (
        '"a1"',
        "<html><body><h1>API reference</h1><p>functions the widget api exposes</p></body></html>",
    ),
}


class EtagHandler(http.server.BaseHTTPRequestHandler):
    bodies_served = 0

    def do_GET(self):  # noqa: N802 - http.server contract
        entry = PAGES.get(self.path)
        if entry is None:
            self.send_response(404)
            self.end_headers()
            return
        etag, body = entry
        if self.headers.get("If-None-Match") == etag:
            self.send_response(304)
            self.send_header("ETag", etag)
            self.end_headers()
            return
        type(self).bodies_served += 1
        payload = body.encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/html")
        self.send_header("ETag", etag)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, *args):  # keep pytest output clean
        pass


def serve():
    httpd = socketserver.ThreadingTCPServer(("127.0.0.1", 0), EtagHandler)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    return httpd, httpd.server_address[1]


def test_sync_absorbs_then_revalidates_for_free_across_sessions(tmp_path):
    EtagHandler.bodies_served = 0
    httpd, port = serve()
    base = f"http://127.0.0.1:{port}/docs"
    path = str(tmp_path / "kb")
    try:
        # Session one: first sync downloads all three bodies and absorbs them.
        store = graph_r.Store.open_or_create(path)
        report = store.sync(base, depth=2)
        assert report.added == 3, repr(report)
        assert report.upserted == 3, repr(report)
        assert EtagHandler.bodies_served == 3
        assert len(store) == 3

        # Lookups answer locally with URL + anchor, never bodies.
        hits = store.query("install on linux")
        assert hits[0].url.endswith("/docs/install")
        assert hits[0].anchor is not None and "Install" in hits[0].anchor.label

        # The index died with the call; the store is all that persists. End
        # the session entirely (lock released on drop).
        del store

        # Session two: a fresh process-equivalent. The graph seeds the crawl,
        # so nothing unchanged transfers a body — and the checks are still
        # RECORDED (touched), so the adaptive freshness intervals grow.
        store = graph_r.Store.open(path)
        report = store.sync(base, depth=2)
        assert EtagHandler.bodies_served == 3, "an unchanged site must transfer zero bodies"
        assert report.added == 0, repr(report)
        assert report.unchanged == 3, repr(report)
        assert report.touched == 3, "revalidations must reach the freshness engine"
        assert len(store) == 3

        # Session three: one page changes; exactly one body transfers.
        PAGES["/docs/install"] = (
            '"i2"',
            "<html><body><h1>Install on Linux</h1>"
            "<p>installation now covers arm64 widgets too</p></body></html>",
        )
        report = store.sync(base, depth=2)
        assert EtagHandler.bodies_served == 4, "only the changed page re-downloads"
        # The crawl side is honest about ITS (fresh, ephemeral) index: the
        # changed page arrives as `added`. The graph side is the memory, and
        # it counts the change: three touches — two revalidations plus the
        # content change, which cuts that page's interval.
        assert report.added == 1, repr(report)
        assert report.unchanged == 2, repr(report)
        assert report.touched == 3, repr(report)
        hits = store.query("arm64 installation")
        assert hits and hits[0].url.endswith("/docs/install")
    finally:
        httpd.shutdown()


def test_sync_report_is_inspectable(tmp_path):
    EtagHandler.bodies_served = 0
    httpd, port = serve()
    try:
        store = graph_r.Store.create(str(tmp_path / "kb"))
        report = store.sync(f"http://127.0.0.1:{port}/docs", depth=1)
        assert "SyncReport" in repr(report)
        assert report.edges >= 2, "root links to install and api"
        assert report.segments >= 3, "each page has at least its H1"
        assert report.failed == 0
    finally:
        httpd.shutdown()
