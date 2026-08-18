# Copyright: lituus-io, all rights reserved.
# Author: terekete <spicyzhug@gmail.com>
#
# The GitHub tree-API sync path, end to end from Python against a loopback
# fake GitHub. The counts are the contract: an unchanged repository costs
# exactly one request across sessions, and every skipped file still reaches
# the freshness engine as a recorded check.

import http.server
import json
import socketserver
import threading

import pytest

import graph_r

FILES = {}  # path -> (sha, body); mutated per test


class FakeGithub(http.server.BaseHTTPRequestHandler):
    tree_calls = 0
    blob_fetches = 0

    def do_GET(self):  # noqa: N802 - http.server contract
        if "/git/trees/" in self.path:
            type(self).tree_calls += 1
            tree = [
                {"path": p, "mode": "100644", "type": "blob", "sha": sha, "size": len(body)}
                for p, (sha, body) in FILES.items()
            ]
            self._json({"sha": "root", "truncated": False, "tree": tree})
        elif self.path.startswith("/repos/"):
            self._json({"default_branch": "main"})
        elif self.path.startswith("/raw/o/r/main/"):
            rel = self.path[len("/raw/o/r/main/"):]
            if rel in FILES:
                type(self).blob_fetches += 1
                body = FILES[rel][1].encode()
                self.send_response(200)
                # Like the real raw host: everything is text/plain.
                self.send_header("Content-Type", "text/plain; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            else:
                self.send_response(404)
                self.end_headers()
        else:
            self.send_response(404)
            self.end_headers()

    def _json(self, obj):
        body = json.dumps(obj).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


def serve():
    httpd = socketserver.ThreadingTCPServer(("127.0.0.1", 0), FakeGithub)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    return httpd, httpd.server_address[1]


def seed_files():
    FILES.clear()
    FILES.update({
        "stacks/README.md": ("sha-r1", "# Stacks\n\nexample corpus index words"),
        "stacks/big_query/tables/Pulumi.yaml": (
            "sha-y1",
            "name: bq-tables\nruntime: yaml\ndescription: bigquery table example stack",
        ),
        "stacks/big_query/tables/README.md": (
            "sha-m1",
            "# BigQuery Tables\n\nHow to declare a bigquery table stack",
        ),
    })
    FakeGithub.tree_calls = 0
    FakeGithub.blob_fetches = 0


def bases(port):
    return {"api_base": f"http://127.0.0.1:{port}", "raw_base": f"http://127.0.0.1:{port}/raw"}


def test_unchanged_repo_costs_one_request_across_sessions(tmp_path):
    seed_files()
    httpd, port = serve()
    path = str(tmp_path / "kb")
    try:
        # Session one: everything is new — one tree call, three blob fetches.
        store = graph_r.Store.open_or_create(path)
        report = store.sync("github:o/r@main//stacks", token="sekrit", **bases(port))
        assert report.added == 3, repr(report)
        assert report.upserted == 3, repr(report)
        assert FakeGithub.tree_calls == 1
        assert FakeGithub.blob_fetches == 3

        # Lookups answer locally with the raw URL + anchor.
        hit = store.query("bigquery table stack")[0]
        assert "/raw/o/r/main/stacks/big_query/tables/" in hit.url

        del store  # the graph is all that persists

        # Session two: fresh process-equivalent. ONE tree call, ZERO fetches,
        # and all three checks reach the freshness engine.
        store = graph_r.Store.open(path)
        report = store.sync("github:o/r@main//stacks", token="sekrit", **bases(port))
        assert FakeGithub.tree_calls == 2
        assert FakeGithub.blob_fetches == 3, "an unchanged repo transfers nothing"
        assert report.added == 0, repr(report)
        assert report.unchanged == 3, repr(report)
        assert report.touched == 3, "revalidations must grow the TTL intervals"

        # Session three: one blob SHA moves → exactly one fetch, interval cut.
        FILES["stacks/big_query/tables/README.md"] = (
            "sha-m2",
            "# BigQuery Tables\n\nNow covering partitioned and clustered tables",
        )
        report = store.sync("github:o/r@main//stacks", token="sekrit", **bases(port))
        assert FakeGithub.blob_fetches == 4, "only the changed file transfers"
        assert report.unchanged == 2, repr(report)
        assert report.touched == 3, "2 revalidations + 1 content change"
        hit = store.query("partitioned clustered tables")[0]
        assert hit.url.endswith("/stacks/big_query/tables/README.md")
    finally:
        httpd.shutdown()


def test_github_url_form_and_depth_filter(tmp_path):
    seed_files()
    httpd, port = serve()
    try:
        store = graph_r.Store.create(str(tmp_path / "kb"))
        # The canonical config value shape, with depth = directory levels:
        # depth 0 keeps only files directly in stacks/.
        report = store.sync(
            "https://github.com/o/r/tree/main/stacks", depth=0, **bases(port)
        )
        assert report.added == 1, repr(report)
        assert len(store) == 1
    finally:
        httpd.shutdown()


def test_crawl_options_are_rejected_for_github_specs(tmp_path):
    store = graph_r.Store.create(str(tmp_path / "kb"))
    with pytest.raises(ValueError):
        store.sync("github:o/r@main", scope="host")
    with pytest.raises(ValueError):
        store.sync("github:o/r@main", path_contains=["/x"])
    with pytest.raises(ValueError):
        store.sync("github:o/r@main", api_base="http://x")  # half a pair
    with pytest.raises(ValueError):
        store.sync("https://docs.example.com/", api_base="http://x", raw_base="http://y")
