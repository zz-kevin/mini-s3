"""End-to-end tests against a real cluster: 4 node processes + a gateway on free ports."""

import os
import signal
import socket
import subprocess
import sys
import time

import httpx
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_for(url, seconds=10):
    deadline = time.time() + seconds
    while time.time() < deadline:
        try:
            if httpx.get(url, timeout=0.5).status_code == 200:
                return
        except httpx.HTTPError:
            pass
        time.sleep(0.1)
    raise RuntimeError(f"{url} never came up")


def until(predicate, seconds=10):
    deadline = time.time() + seconds
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(0.1)
    return False


class LocalCluster:
    def __init__(self, tmp):
        self.tmp = tmp
        self.procs = {}
        self.ports = {n: free_port() for n in "abcd"}
        self.gw_port = free_port()
        self.gw = f"http://127.0.0.1:{self.gw_port}"
        for n in self.ports:
            self.start(n)
        nodes = ",".join(f"{n}=http://127.0.0.1:{p}" for n, p in self.ports.items())
        self.procs["gateway"] = self._spawn("mini_s3.gateway:app", self.gw_port, {
            "NODES": nodes, "META_DB": str(tmp / "meta.db"),
            "HEALTH_INTERVAL": "0.2", "REPAIR_INTERVAL": "0.3",
        })
        wait_for(f"{self.gw}/_cluster")
        until(lambda: all(n["alive"] for n in self.status()["nodes"]))

    def _spawn(self, module, port, env):
        return subprocess.Popen(
            [sys.executable, "-m", "uvicorn", module, "--port", str(port), "--log-level", "warning"],
            cwd=ROOT, env={**os.environ, **env}, start_new_session=True,
        )

    def start(self, n):
        self.procs[n] = self._spawn("mini_s3.node:app", self.ports[n],
                                    {"NODE_ID": n, "DATA_DIR": str(self.tmp / n)})
        wait_for(f"http://127.0.0.1:{self.ports[n]}/health")

    def kill(self, n, wait=True):
        os.killpg(self.procs[n].pid, signal.SIGKILL)
        self.procs[n].wait()
        if wait:
            until(lambda: not self.node(n)["alive"])

    def status(self):
        return httpx.get(f"{self.gw}/_cluster").json()

    def node(self, n):
        return next(x for x in self.status()["nodes"] if x["id"] == n)

    def blob_files(self, n):
        d = self.tmp / n
        return [p for p in d.iterdir() if not p.name.startswith(".")] if d.exists() else []

    def stop(self):
        for p in self.procs.values():
            if p.poll() is None:
                os.killpg(p.pid, signal.SIGKILL)
                p.wait()


@pytest.fixture
def c(tmp_path):
    cluster = LocalCluster(tmp_path)
    yield cluster
    cluster.stop()


def test_put_get_list_delete(c):
    r = httpx.put(f"{c.gw}/photos/cats/1.jpg", content=b"meow", headers={"content-type": "image/jpeg"})
    assert r.status_code == 201
    assert len(r.json()["replicas"]) == 3

    r = httpx.get(f"{c.gw}/photos/cats/1.jpg")
    assert r.content == b"meow" and r.headers["content-type"] == "image/jpeg"

    httpx.put(f"{c.gw}/photos/dogs/1.jpg", content=b"woof")
    keys = [o["key"] for o in httpx.get(f"{c.gw}/photos", params={"prefix": "cats/"}).json()["objects"]]
    assert keys == ["cats/1.jpg"]

    assert httpx.delete(f"{c.gw}/photos/cats/1.jpg").status_code == 200
    assert httpx.get(f"{c.gw}/photos/cats/1.jpg").status_code == 404
    # The blobs themselves are removed by the background pass.
    assert until(lambda: sum(len(c.blob_files(n)) for n in "abcd") == 3)


def test_overwrite_cleans_up_old_version(c):
    httpx.put(f"{c.gw}/b1/k", content=b"v1")
    httpx.put(f"{c.gw}/b1/k", content=b"v2")
    assert httpx.get(f"{c.gw}/b1/k").content == b"v2"
    assert until(lambda: sum(len(c.blob_files(n)) for n in "abcd") == 3)


def test_bad_bucket_name(c):
    assert httpx.put(f"{c.gw}/Bad_Bucket/k", content=b"x").status_code == 400


def test_reads_survive_node_crash_and_repair_restores_copies(c):
    for i in range(30):
        httpx.put(f"{c.gw}/b1/k{i}", content=f"value {i}".encode())
    c.kill("a")
    for i in range(30):
        assert httpx.get(f"{c.gw}/b1/k{i}").content == f"value {i}".encode()
    # Every object should get back to 3 copies on the 3 surviving nodes.
    assert until(lambda: c.status()["under_replicated"] == 0 and c.status()["repairs"] > 0)
    assert all(len(c.blob_files(n)) == 30 for n in "bcd")


def test_writes_need_a_quorum(c):
    c.kill("a")
    c.kill("b")
    # 2 nodes up still meets the quorum of 2 ...
    assert httpx.put(f"{c.gw}/b1/k", content=b"x").status_code == 201
    c.kill("c")
    # ... but 1 doesn't, and the write is refused rather than silently under-replicated.
    r = httpx.put(f"{c.gw}/b1/k2", content=b"x")
    assert r.status_code == 503 and "need 2" in r.json()["detail"]
    # Data written earlier is still readable from the one survivor.
    assert httpx.get(f"{c.gw}/b1/k").content == b"x"


def test_corrupt_replica_is_detected_and_replaced(c):
    httpx.put(f"{c.gw}/b1/k", content=b"precious data")
    blob_id_files = {n: c.blob_files(n) for n in "abcd"}
    holders = [n for n, files in blob_id_files.items() if files]
    # Flip bits on every copy but one, as if disks went bad.
    for n in holders[:-1]:
        blob_id_files[n][0].write_bytes(b"garbage!")
    r = httpx.get(f"{c.gw}/b1/k")
    assert r.content == b"precious data"
    assert r.headers["x-served-by"] == holders[-1]
    # Repair re-copies good data over the corrupt replicas.
    assert until(lambda: all(f[0].read_bytes() == b"precious data"
                             for n, f in blob_id_files.items() if n in holders))


def test_delete_while_node_down_finishes_when_it_returns(c):
    r = httpx.put(f"{c.gw}/b1/k", content=b"x").json()
    down = r["replicas"][0]
    c.kill(down)
    assert httpx.delete(f"{c.gw}/b1/k").status_code == 200
    assert c.status()["pending_deletes"] >= 1
    c.start(down)
    assert until(lambda: c.status()["pending_deletes"] == 0)
    assert c.blob_files(down) == []


def test_node_rejoin_trims_extra_copies(c):
    for i in range(20):
        httpx.put(f"{c.gw}/b1/k{i}", content=b"x")
    c.kill("a")
    assert until(lambda: c.status()["under_replicated"] == 0 and c.status()["repairs"] > 0)
    c.start("a")
    # Back to exactly 3 copies per object: 60 blob files across the 4 nodes.
    assert until(lambda: sum(len(c.blob_files(n)) for n in "abcd") == 60, seconds=15)
    for i in range(20):
        assert httpx.get(f"{c.gw}/b1/k{i}").content == b"x"


def test_reads_right_after_a_crash_dont_lose_track_of_copies(c):
    """Regression: reads in the window before the health checker notices a crash
    used to treat "can't connect" as "copy is bad" and orphan the node's files."""
    for i in range(30):
        httpx.put(f"{c.gw}/b1/k{i}", content=b"x")
    c.kill("a", wait=False)
    for i in range(30):  # immediately, before the gateway knows "a" is down
        assert httpx.get(f"{c.gw}/b1/k{i}").content == b"x"
    until(lambda: not c.node("a")["alive"])
    assert until(lambda: c.status()["under_replicated"] == 0 and c.status()["repairs"] > 0)
    c.start("a")
    # Exactly 3 copies per object once things settle: nothing orphaned on "a".
    assert until(lambda: sum(len(c.blob_files(n)) for n in "abcd") == 90, seconds=15)
