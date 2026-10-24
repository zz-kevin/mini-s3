"""Raft gateway tests: 3 gateway processes + 4 storage nodes, with gateways crashed and
restarted to check elections, failover, quorum, catch-up, and persistence."""

import os
import signal
import sqlite3
import time
import uuid

import httpx
import pytest

from test_cluster import LocalCluster, free_port, until, wait_for

GATEWAYS = ("g1", "g2", "g3")


class RaftCluster(LocalCluster):
    def __init__(self, tmp):
        self.tmp = tmp
        self.procs = {}
        self.ports = {n: free_port() for n in "abcd"}
        self.gw_ports = {g: free_port() for g in GATEWAYS}
        for n in self.ports:
            self.start(n)
        self.nodes_env = ",".join(f"{n}=http://127.0.0.1:{p}" for n, p in self.ports.items())
        for g in GATEWAYS:
            self.start_gw(g, wait=False)
        assert until(lambda: self.leader() is not None, seconds=10)
        until(lambda: all(n["alive"] for n in self.status()["nodes"]))

    def url(self, g):
        return f"http://127.0.0.1:{self.gw_ports[g]}"

    def start_gw(self, g, wait=True):
        peers = ",".join(f"{o}={self.url(o)}" for o in GATEWAYS if o != g)
        self.procs[g] = self._spawn("mini_s3.gateway:app", self.gw_ports[g], {
            "GATEWAY_ID": g, "PEERS": peers, "NODES": self.nodes_env, "META_DB": str(self.tmp / f"{g}.db"),
            "HEALTH_INTERVAL": "0.2", "REPAIR_INTERVAL": "0.3", "ORPHAN_INTERVAL": "0.5", "ORPHAN_GRACE": "1",
        })
        if wait:
            wait_for(f"{self.url(g)}/_raft")

    def kill(self, name, wait=False):
        os.killpg(self.procs[name].pid, signal.SIGKILL)
        self.procs[name].wait()

    def alive_gateways(self):
        return [g for g in GATEWAYS if self.procs[g].poll() is None]

    def raft(self, g):
        try:
            return httpx.get(f"{self.url(g)}/_raft", timeout=0.5).json()
        except httpx.HTTPError:
            return None

    def leader(self):
        leaders = [s for s in map(self.raft, self.alive_gateways()) if s and s["role"] == "leader"]
        return leaders[0] if len(leaders) == 1 else None

    def req(self, method, path, retry_for=6.0, **kw):
        """Like a real client: try every live gateway until one answers for real."""
        deadline = time.time() + retry_for
        r = None
        while time.time() < deadline:
            for g in self.alive_gateways():
                try:
                    r = httpx.request(method, f"{self.url(g)}{path}", timeout=5, **kw)
                except httpx.TransportError:
                    continue
                if not (r.status_code == 503 and r.headers.get("x-ms3-retry")):
                    return r
            time.sleep(0.1)
        return r

    def status(self):
        return self.req("GET", "/_cluster").json()

    def objects_in(self, g):
        with sqlite3.connect(self.tmp / f"{g}.db") as db:
            return db.execute("SELECT bucket, key, blob_id, checksum FROM objects ORDER BY bucket, key").fetchall()


@pytest.fixture
def rc(tmp_path):
    cluster = RaftCluster(tmp_path)
    yield cluster
    cluster.stop()


def test_one_leader_and_followers_forward(rc):
    leader = rc.leader()["id"]
    followers = [g for g in GATEWAYS if g != leader]
    for i, g in enumerate(followers):
        r = httpx.put(f"{rc.url(g)}/b1/k{i}", content=f"v{i}".encode())
        assert r.status_code == 201
        assert r.headers["x-ms3-via"] == g and r.headers["x-ms3-gateway"] == leader
    for g in GATEWAYS:  # every gateway gives the same answer
        assert httpx.get(f"{rc.url(g)}/b1/k0").content == b"v0"
    # All three gateways' metadata converges to the same rows.
    assert until(lambda: rc.objects_in("g1") == rc.objects_in("g2") == rc.objects_in("g3") != [])


def test_leader_crash_elects_new_leader_and_keeps_data(rc):
    for i in range(20):
        assert rc.req("PUT", f"/b1/k{i}", content=f"value {i}".encode()).status_code == 201
    old = rc.leader()
    rc.kill(old["id"])
    t = time.time()
    assert until(lambda: rc.leader() is not None, seconds=5)
    new = rc.leader()
    assert new["id"] != old["id"] and new["term"] > old["term"]
    assert time.time() - t < 3, "election took too long"
    for i in range(20):  # nothing committed before the crash was lost
        assert rc.req("GET", f"/b1/k{i}").content == f"value {i}".encode()
    assert rc.req("PUT", "/b1/after", content=b"x").status_code == 201


def test_no_writes_without_a_majority_of_gateways(rc):
    assert rc.req("PUT", "/b1/before", content=b"x").status_code == 201
    leader = rc.leader()["id"]
    follower = next(g for g in GATEWAYS if g != leader)
    rc.kill(leader)
    rc.kill(follower)
    # One gateway alone can't elect itself or commit anything: refuse instead of diverging.
    r = rc.req("PUT", "/b1/lonely", content=b"x", retry_for=2)
    assert r.status_code == 503
    rc.start_gw(follower)
    assert until(lambda: rc.leader() is not None, seconds=5)
    assert rc.req("PUT", "/b1/after", content=b"y").status_code == 201
    assert rc.req("GET", "/b1/before").content == b"x"
    assert rc.req("GET", "/b1/lonely").status_code == 404


def test_restarted_gateway_catches_up(rc):
    follower = next(g for g in GATEWAYS if g != rc.leader()["id"])
    rc.kill(follower)
    for i in range(30):
        assert rc.req("PUT", f"/b1/k{i}", content=b"x").status_code == 201
    rc.start_gw(follower)
    leader = rc.leader()["id"]
    assert until(lambda: rc.objects_in(follower) == rc.objects_in(leader) and len(rc.objects_in(follower)) == 30)


def test_full_restart_keeps_metadata(rc):
    for i in range(10):
        rc.req("PUT", f"/b1/k{i}", content=f"{i}".encode())
    for g in GATEWAYS:
        rc.kill(g)
    for g in GATEWAYS:
        rc.start_gw(g, wait=False)
    assert until(lambda: rc.leader() is not None, seconds=10)
    for i in range(10):
        assert rc.req("GET", f"/b1/k{i}").content == f"{i}".encode()


def test_orphaned_copies_are_swept(rc):
    # A blob no metadata mentions, as if a leader crashed right after writing it.
    blob_id = uuid.uuid4().hex
    httpx.put(f"http://127.0.0.1:{rc.ports['a']}/blobs/{blob_id}", content=b"orphan")
    path = rc.tmp / "a" / blob_id
    old = time.time() - 60
    os.utime(path, (old, old))
    rc.req("PUT", "/b1/real", content=b"keep me")
    assert until(lambda: not path.exists(), seconds=10)
    assert rc.req("GET", "/b1/real").content == b"keep me"

