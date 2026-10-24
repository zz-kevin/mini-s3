"""API server: S3-style object API, metadata, health checks, and the repair worker.

Several gateways run together. They keep identical metadata with Raft (see raft.py):
one is the leader and does all the work; the others forward client requests to it and
take over if it dies.

Run: GATEWAY_ID=g1 PEERS="g2=http://127.0.0.1:7481,g3=http://127.0.0.1:7482" \
     NODES="a=http://127.0.0.1:9001,..." uvicorn mini_s3.gateway:app --port 7480
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
from fastapi.responses import FileResponse, JSONResponse

from .metadata import Metadata
from .raft import NotLeader, Raft
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
# Copies on a node that no metadata mentions (e.g. a leader crashed after writing blobs
# but before committing the object) are deleted once they're this old.
ORPHAN_GRACE = float(os.environ.get("ORPHAN_GRACE", "120"))
ORPHAN_INTERVAL = float(os.environ.get("ORPHAN_INTERVAL", "30"))
# Tells clients a 503 is about leadership (an election, a forwarding hop) and is worth retrying.
RETRY_HEADER = {"x-ms3-retry": "leader"}


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
        self.raft = None
        self.health_checked_term = -1  # the term in which this gateway last finished probing the nodes
        self.health_lock = asyncio.Lock()
        self.ring = HashRing(nodes)
        self.health = {n: {"alive": False, "since": time.time(), "blobs": 0, "bytes": 0, "misses": 0}
                       for n in nodes}
        self.stats = {"repairs": 0, "read_repairs": 0, "read_failovers": 0, "trimmed": 0, "orphans_swept": 0, "under_replicated": 0, "lost": 0,
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

        term = self.raft.term
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
        # Only now is the view complete; marking it at the start let a request that
        # raced the first probe see every node as down.
        self.health_checked_term = term

    async def check_health_if_leader(self):
        if self.raft.role == "leader":
            await self.check_health()

    async def fresh_health(self):
        """A new leader's view of the nodes is stale until it probes them itself."""
        if self.health_checked_term != self.raft.term:
            async with self.health_lock:  # many requests may arrive at once; probe once
                if self.health_checked_term != self.raft.term:
                    await self.check_health()

    def on_leader(self):
        # Start from a clean slate: last leader's stats and node view don't carry over.
        for h in self.health.values():
            h.update(alive=False, misses=0, since=time.time())
        self.health_checked_term = -1
        asyncio.get_running_loop().create_task(self.fresh_health())

    async def propose(self, cmd):
        try:
            return await self.raft.propose(cmd)
        except NotLeader as e:
            raise HTTPException(503, str(e), headers=RETRY_HEADER)

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
        if self.raft.role != "leader" or not self.raft.lease_valid():
            return
        alive = self.alive()
        trims = []
        under, lost = 0, 0
        jobs = []
        for blob_id, p in self.meta.placements().items():
            live = [n for n in p["nodes"] if n in alive]
            if len(live) > REPLICAS:
                trims += self._trim(blob_id, p, live, alive)
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

        copied = [c for c in await asyncio.gather(*(limited(j) for j in jobs)) if c]
        # Copies still tracked for a blob that was replaced or deleted: queue them for deletion.
        strays = [{"op": "retire_replica", "blob_id": r["blob_id"], "node": r["node_id"]}
                  for r in self.meta.stray_replicas()]
        # One log entry for the whole pass instead of one per copy.
        changes = [{"op": "add_replica", "blob_id": b, "node": n} for b, n in copied] + trims + strays
        if changes:
            await self.raft.propose({"op": "batch", "cmds": changes})
        self.stats["repairs"] += len(copied)
        self.stats["trimmed"] += len(trims)

        # Pending deletes run in parallel too; one at a time made long repair passes
        # (and slow healing) whenever many old versions piled up.
        async def collect(g):
            async with sem:
                if await self.delete_blob(g["node_id"], g["blob_id"]):
                    return {"op": "clear_garbage", "blob_id": g["blob_id"], "node": g["node_id"]}

        cleared = [c for c in await asyncio.gather(*(collect(g) for g in self.meta.garbage()
                                                     if g["node_id"] in alive)) if c]
        if cleared:
            await self.raft.propose({"op": "batch", "cmds": cleared})

        self.stats["under_replicated"] = under - len(copied)
        self.stats["lost"] = lost
        self.stats["last_repair_pass"] = time.time()

    def _trim(self, blob_id, p, live, alive):
        """A node came back after its data was re-copied elsewhere: drop the extra copies.

        Keep the copies on the object's home nodes (first REPLICAS live nodes on the ring)
        and only trim once every home node actually holds a copy.
        """
        home = [n for n in self.ring.preference_list(f"{p['bucket']}/{p['key']}") if n in alive][:REPLICAS]
        if not all(n in live for n in home):
            return []
        return [{"op": "retire_replica", "blob_id": blob_id, "node": extra} for extra in live if extra not in home]

    async def _copy(self, blob_id, checksum, sources, target):
        for source in sources:
            data = await self.get_blob(source, blob_id, checksum)
            if data is None:
                continue
            if await self.put_blob(target, blob_id, data, checksum):
                return blob_id, target
        return None

    async def sweep_orphans(self):
        """Delete copies no metadata refers to, once they're older than ORPHAN_GRACE."""
        if self.raft.role != "leader" or not self.raft.lease_valid():
            return
        for node_id in self.alive():
            try:
                async with self.http.get(f"{self.nodes[node_id]}/blobs", timeout=_timeout(10)) as r:
                    listing = await r.json()
            except NODE_ERRORS:
                continue
            known = self.meta.known_blobs(node_id)
            for b in listing:
                if b["id"] not in known and b["age"] > ORPHAN_GRACE:
                    if await self.delete_blob(node_id, b["id"]):
                        self.stats["orphans_swept"] += 1


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
    db_path = os.environ.get("META_DB", "data/meta.db")
    os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
    cluster = Cluster(nodes, Metadata(db_path))
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=200)) as http:
        cluster.http = http
        cluster.raft = Raft(os.environ.get("GATEWAY_ID", "g1"), parse_nodes(os.environ.get("PEERS", "")),
                            cluster.meta, http, on_leader=cluster.on_leader)
        app.state.cluster = cluster
        tasks = cluster.raft.start() + [
            asyncio.create_task(_loop(HEALTH_INTERVAL, cluster.check_health_if_leader)),
            asyncio.create_task(_loop(REPAIR_INTERVAL, cluster.repair_once)),
            asyncio.create_task(_loop(ORPHAN_INTERVAL, cluster.sweep_orphans)),
        ]
        yield
        for t in tasks:
            t.cancel()


# No built-in /docs, /redoc or /openapi.json: those paths would shadow buckets with those names.
app = FastAPI(title="mini-s3", lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)

LOCAL_PATHS = ("/raft/", "/_raft", "/static/")


@app.middleware("http")
async def route_to_leader(request: Request, call_next):
    """Followers forward client requests to the leader, so clients can use any gateway."""
    path = request.url.path
    c = request.app.state.cluster
    if path == "/" or path.startswith(LOCAL_PATHS) or c.raft.role == "leader":
        response = await call_next(request)
        response.headers["x-ms3-gateway"] = c.raft.me
        if path == "/_cluster":  # the dashboard may poll other gateways if its own dies
            response.headers["access-control-allow-origin"] = "*"
        return response
    leader_url = c.raft.peers.get(c.raft.leader_id)
    if not leader_url or request.headers.get("x-ms3-forwarded"):
        return JSONResponse({"detail": "No leader elected right now; retry shortly"}, 503, headers=RETRY_HEADER)
    headers = {"x-ms3-forwarded": c.raft.me}
    if "content-type" in request.headers:
        headers["content-type"] = request.headers["content-type"]
    try:
        async with c.http.request(request.method, f"{leader_url}{path}", params=request.query_params,
                                  data=await request.body(), headers=headers, timeout=_timeout(30)) as r:
            body = await r.read()
            passthrough = {k: v for k, v in r.headers.items()
                           if k.lower() in ("content-type", "etag", "x-served-by", "x-ms3-retry", "x-ms3-gateway")}
            if path == "/_cluster":
                passthrough["access-control-allow-origin"] = "*"
            return Response(body, r.status, headers={**passthrough, "x-ms3-via": c.raft.me})
    except NODE_ERRORS:
        return JSONResponse({"detail": "Leader unreachable; retry shortly"}, 503, headers=RETRY_HEADER)


@app.post("/raft/vote")
async def raft_vote(request: Request):
    return request.app.state.cluster.raft.handle_vote(await request.json())


@app.post("/raft/append")
async def raft_append(request: Request):
    return request.app.state.cluster.raft.handle_append(await request.json())


@app.get("/_raft")
def raft_status(request: Request):
    return JSONResponse(request.app.state.cluster.raft.status(),
                        headers={"access-control-allow-origin": "*"})


def _cluster(request: Request) -> Cluster:
    return request.app.state.cluster


async def _leader_for_reads(c):
    if not await c.raft.confirm_leadership():
        raise HTTPException(503, "Leadership is changing; retry shortly", headers=RETRY_HEADER)


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
        "raft": c.raft.status(),
    }


@app.put("/{bucket}/{key:path}", status_code=201)
async def put_object(bucket: str, key: str, request: Request):
    _check_bucket(bucket)
    c = _cluster(request)
    data = await request.body()
    if len(data) > MAX_OBJECT_BYTES:
        raise HTTPException(413, f"Objects are limited to {MAX_OBJECT_BYTES // (1024 * 1024)} MB")

    await c.fresh_health()
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

    await c.propose({"op": "put", "bucket": bucket, "key": key, "blob_id": blob_id, "size": len(data),
                     "checksum": checksum, "nodes": written,
                     "content_type": request.headers.get("content-type", "application/octet-stream"),
                     "updated_at": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())})
    return {"bucket": bucket, "key": key, "etag": checksum, "size": len(data), "replicas": written}


@app.get("/{bucket}/{key:path}")
async def get_object(bucket: str, key: str, request: Request):
    _check_bucket(bucket)
    c = _cluster(request)
    await _leader_for_reads(c)
    obj = c.meta.get(bucket, key)
    if not obj:
        raise HTTPException(404, "No such key")

    await c.fresh_health()
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
                    try:
                        await c.raft.propose({"op": "retire_replica", "blob_id": obj["blob_id"], "node": b})
                    except NotLeader:
                        pass  # the next leader's repair pass will notice the bad copy
            return Response(data, media_type=obj["content_type"],
                            headers={"etag": obj["checksum"], "x-served-by": node_id})
        # An unreachable node is the health checker's call, not proof the copy is bad.
        if problem == "bad":
            bad.append(node_id)
    raise HTTPException(503, "No replica of this object is reachable right now")


@app.delete("/{bucket}/{key:path}")
async def delete_object(bucket: str, key: str, request: Request):
    _check_bucket(bucket)
    # Blob removal happens in the repair pass, so deletes succeed even while nodes are down.
    if not await _cluster(request).propose({"op": "delete", "bucket": bucket, "key": key}):
        raise HTTPException(404, "No such key")
    return {"deleted": f"{bucket}/{key}"}


@app.get("/{bucket}")
async def list_objects(bucket: str, request: Request, prefix: str = "", limit: int = 1000):
    _check_bucket(bucket)
    await _leader_for_reads(_cluster(request))
    return {"bucket": bucket, "objects": _cluster(request).meta.list(bucket, prefix, min(limit, 10000))}
