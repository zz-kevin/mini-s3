"""API server: S3-style object API, metadata, health checks, and the repair worker.

Run: NODES="a=http://127.0.0.1:9001,b=http://127.0.0.1:9002,..." uvicorn mini_s3.gateway:app --port 7480
"""

import asyncio
import hashlib
import os
import re
import time
import uuid
from contextlib import asynccontextmanager

import aiohttp
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import FileResponse

from .metadata import Metadata
from .ring import HashRing

REPLICAS = int(os.environ.get("REPLICAS", "3"))
WRITE_QUORUM = int(os.environ.get("WRITE_QUORUM", "2"))
MAX_OBJECT_BYTES = int(os.environ.get("MAX_OBJECT_MB", "64")) * 1024 * 1024
HEALTH_INTERVAL = float(os.environ.get("HEALTH_INTERVAL", "1.0"))
REPAIR_INTERVAL = float(os.environ.get("REPAIR_INTERVAL", "2.0"))
# Consecutive failed health checks before a node counts as down. One slow answer
# shouldn't trigger a wave of re-replication ("flapping").
FAILS_BEFORE_DOWN = int(os.environ.get("FAILS_BEFORE_DOWN", "2"))
BUCKET = re.compile(r"^[a-z0-9][a-z0-9.-]{1,62}$")
STATIC = os.path.join(os.path.dirname(__file__), "static")


# Anything that means "this node didn't give us a good answer".
NODE_ERRORS = (aiohttp.ClientError, asyncio.TimeoutError, ValueError)


def _timeout(seconds):
    return aiohttp.ClientTimeout(total=seconds)


def parse_nodes(spec):
    nodes = {}
    for part in filter(None, (p.strip() for p in spec.split(","))):
        node_id, _, url = part.partition("=")
        if not url:
            raise ValueError(f"Bad NODES entry {part!r}; expected id=url")
        nodes[node_id] = url.rstrip("/")
    return nodes


class Cluster:
    """Everything the gateway knows: node addresses, liveness, the ring, and stats."""

    def __init__(self, nodes, meta):
        self.nodes = nodes
        self.meta = meta
        self.ring = HashRing(nodes)
        self.health = {n: {"alive": False, "since": time.time(), "blobs": 0, "bytes": 0, "misses": 0}
                       for n in nodes}
        self.stats = {"repairs": 0, "read_repairs": 0, "read_failovers": 0, "trimmed": 0, "under_replicated": 0, "lost": 0,
                      "last_repair_pass": None}
        self.http = None

    def alive(self):
        return {n for n, h in self.health.items() if h["alive"]}

    async def check_health(self):
        async def probe(node_id, url):
            try:
                async with self.http.get(f"{url}/health", timeout=_timeout(0.5)) as r:
                    r.raise_for_status()
                    return node_id, await r.json()
            except NODE_ERRORS:
                return node_id, None

        for node_id, info in await asyncio.gather(*(probe(n, u) for n, u in self.nodes.items())):
            h = self.health[node_id]
            if info:
                h["misses"] = 0
                h["blobs"], h["bytes"] = info["blobs"], info["bytes"]
                if not h["alive"]:
                    h["alive"], h["since"] = True, time.time()
            else:
                h["misses"] += 1
                if h["alive"] and h["misses"] >= FAILS_BEFORE_DOWN:
                    h["alive"], h["since"] = False, time.time()

    # ----- blob I/O on nodes -----

    async def put_blob(self, node_id, blob_id, data, checksum):
        try:
            async with self.http.put(f"{self.nodes[node_id]}/blobs/{blob_id}", data=data,
                                     headers={"x-checksum": checksum}, timeout=_timeout(10)) as r:
                return r.status == 201
        except NODE_ERRORS:
            return False

    async def fetch_blob(self, node_id, blob_id, checksum):
        """Returns (data, problem). problem is None on success, "unreachable" if the node
        didn't answer, or "bad" if the node answered but its copy is missing or corrupt."""
        try:
            async with self.http.get(f"{self.nodes[node_id]}/blobs/{blob_id}", timeout=_timeout(10)) as r:
                if r.status == 404:
                    return None, "bad"
                if r.status != 200:
                    return None, "unreachable"
                data = await r.read()
        except NODE_ERRORS:
            return None, "unreachable"
        if hashlib.sha256(data).hexdigest() != checksum:
            return None, "bad"
        return data, None

    async def get_blob(self, node_id, blob_id, checksum):
        return (await self.fetch_blob(node_id, blob_id, checksum))[0]

    async def delete_blob(self, node_id, blob_id):
        try:
            async with self.http.delete(f"{self.nodes[node_id]}/blobs/{blob_id}", timeout=_timeout(5)) as r:
                return r.status == 200
        except NODE_ERRORS:
            return False

    # ----- repair worker -----

    async def repair_once(self):
        """Bring every object back to REPLICAS live copies and finish pending deletes."""
        alive = self.alive()
        under, lost = 0, 0
        jobs = []
        for blob_id, p in self.meta.placements().items():
            live = [n for n in p["nodes"] if n in alive]
            if len(live) > REPLICAS:
                self._trim(blob_id, p, live, alive)
                continue
            if len(live) == REPLICAS:
                continue
            under += 1
            if not live:
                lost += 1  # every copy is on a down node; wait for one to come back
                continue
            targets = [n for n in self.ring.preference_list(f"{p['bucket']}/{p['key']}")
                       if n in alive and n not in p["nodes"]][: REPLICAS - len(live)]
            for target in targets:
                jobs.append(self._copy(blob_id, p["checksum"], live, target))

        # Publish the damage before repairing so observers see it while copies are in flight.
        self.stats["under_replicated"] = under
        self.stats["lost"] = lost

        sem = asyncio.Semaphore(16)

        async def limited(job):
            async with sem:
                return await job

        results = await asyncio.gather(*(limited(j) for j in jobs))
        self.stats["repairs"] += sum(results)

        # Pending deletes run in parallel too; one at a time made long repair passes
        # (and slow healing) whenever many old versions piled up.
        async def collect(g):
            async with sem:
                if await self.delete_blob(g["node_id"], g["blob_id"]):
                    self.meta.clear_garbage(g["blob_id"], g["node_id"])

        await asyncio.gather(*(collect(g) for g in self.meta.garbage() if g["node_id"] in alive))

        self.stats["under_replicated"] = under - sum(results)
        self.stats["lost"] = lost
        self.stats["last_repair_pass"] = time.time()

    def _trim(self, blob_id, p, live, alive):
        """A node came back after its data was re-copied elsewhere: drop the extra copies.

        Keep the copies on the object's home nodes (first REPLICAS live nodes on the ring)
        and only trim once every home node actually holds a copy.
        """
        home = [n for n in self.ring.preference_list(f"{p['bucket']}/{p['key']}") if n in alive][:REPLICAS]
        if not all(n in live for n in home):
            return
        for extra in (n for n in live if n not in home):
            self.meta.retire_replica(blob_id, extra)
            self.stats["trimmed"] += 1

    async def _copy(self, blob_id, checksum, sources, target):
        for source in sources:
            data = await self.get_blob(source, blob_id, checksum)
            if data is None:
                continue
            if await self.put_blob(target, blob_id, data, checksum):
                self.meta.add_replica(blob_id, target)
                return True
        return False


async def _loop(interval, fn):
    while True:
        try:
            await fn()
        except Exception as e:  # keep background loops alive; log and retry next tick
            print(f"[gateway] background task error: {e!r}")
        await asyncio.sleep(interval)


@asynccontextmanager
async def lifespan(app):
    nodes = parse_nodes(os.environ.get("NODES", ""))
    if not nodes:
        raise RuntimeError("Set NODES, e.g. NODES='a=http://127.0.0.1:9001,b=http://127.0.0.1:9002'")
    os.makedirs(os.path.dirname(os.path.abspath(os.environ.get("META_DB", "data/meta.db"))), exist_ok=True)
    cluster = Cluster(nodes, Metadata(os.environ.get("META_DB", "data/meta.db")))
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=200)) as http:
        cluster.http = http
        await cluster.check_health()
        app.state.cluster = cluster
        tasks = [asyncio.create_task(_loop(HEALTH_INTERVAL, cluster.check_health)),
                 asyncio.create_task(_loop(REPAIR_INTERVAL, cluster.repair_once))]
        yield
        for t in tasks:
            t.cancel()


app = FastAPI(title="mini-s3", lifespan=lifespan)


def _cluster(request: Request) -> Cluster:
    return request.app.state.cluster


def _check_bucket(bucket):
    if not BUCKET.match(bucket):
        raise HTTPException(400, "Bucket names are 2-63 chars: lowercase letters, digits, dots, hyphens")


@app.get("/")
def status_page():
    return FileResponse(os.path.join(STATIC, "status.html"))


@app.get("/_cluster")
def cluster_status(request: Request):
    c = _cluster(request)
    now = time.time()
    return {
        "replicas": REPLICAS,
        "write_quorum": WRITE_QUORUM,
        "nodes": [{"id": n, "url": c.nodes[n], **h, "for_seconds": round(now - h["since"], 1)}
                  for n, h in c.health.items()],
        **c.meta.counts(),
        **c.stats,
    }


@app.put("/{bucket}/{key:path}", status_code=201)
async def put_object(bucket: str, key: str, request: Request):
    _check_bucket(bucket)
    c = _cluster(request)
    data = await request.body()
    if len(data) > MAX_OBJECT_BYTES:
        raise HTTPException(413, f"Objects are limited to {MAX_OBJECT_BYTES // (1024 * 1024)} MB")

    alive = c.alive()
    # Walk the ring from the key's position, skipping dead nodes ("sloppy" placement);
    # the repair worker moves copies home when nodes recover or fail.
    targets = [n for n in c.ring.preference_list(f"{bucket}/{key}") if n in alive][:REPLICAS]
    if len(targets) < WRITE_QUORUM:
        raise HTTPException(503, f"Only {len(targets)} storage nodes are up; need {WRITE_QUORUM} to accept writes")

    checksum = hashlib.sha256(data).hexdigest()
    blob_id = uuid.uuid4().hex
    ok = await asyncio.gather(*(c.put_blob(n, blob_id, data, checksum) for n in targets))
    written = [n for n, success in zip(targets, ok) if success]
    if len(written) < WRITE_QUORUM:
        await asyncio.gather(*(c.delete_blob(n, blob_id) for n in written))
        raise HTTPException(503, f"Only {len(written)} of {len(targets)} replicas confirmed the write; need {WRITE_QUORUM}")

    c.meta.put(bucket, key, blob_id, len(data), checksum,
               request.headers.get("content-type", "application/octet-stream"), written)
    return {"bucket": bucket, "key": key, "etag": checksum, "size": len(data), "replicas": written}


@app.get("/{bucket}/{key:path}")
async def get_object(bucket: str, key: str, request: Request):
    _check_bucket(bucket)
    c = _cluster(request)
    obj = c.meta.get(bucket, key)
    if not obj:
        raise HTTPException(404, "No such key")

    alive = c.alive()
    order = [n for n in c.ring.preference_list(f"{bucket}/{key}") if n in obj["replicas"]]
    order.sort(key=lambda n: n not in alive)  # try live nodes first
    bad = []  # nodes that answered but whose copy is missing or corrupt
    for i, node_id in enumerate(order):
        data, problem = await c.fetch_blob(node_id, obj["blob_id"], obj["checksum"])
        if data is not None:
            if i > 0:
                c.stats["read_failovers"] += 1
            for b in bad:
                # Read repair: overwrite the bad copy with the good bytes we just read.
                if await c.put_blob(b, obj["blob_id"], data, obj["checksum"]):
                    c.stats["read_repairs"] += 1
                else:
                    # Stop counting the bad copy; the repair worker will place a new one.
                    c.meta.retire_replica(obj["blob_id"], b)
            return Response(data, media_type=obj["content_type"],
                            headers={"etag": obj["checksum"], "x-served-by": node_id})
        # An unreachable node is the health checker's call, not proof the copy is bad.
        if problem == "bad":
            bad.append(node_id)
    raise HTTPException(503, "No replica of this object is reachable right now")


@app.delete("/{bucket}/{key:path}")
def delete_object(bucket: str, key: str, request: Request):
    _check_bucket(bucket)
    # Blob removal happens in the repair pass, so deletes succeed even while nodes are down.
    if not _cluster(request).meta.delete(bucket, key):
        raise HTTPException(404, "No such key")
    return {"deleted": f"{bucket}/{key}"}


@app.get("/{bucket}")
def list_objects(bucket: str, request: Request, prefix: str = "", limit: int = 1000):
    _check_bucket(bucket)
    return {"bucket": bucket, "objects": _cluster(request).meta.list(bucket, prefix, min(limit, 10000))}
