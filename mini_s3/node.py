"""Storage node: stores blobs on local disk, addressed by blob id.

Run: NODE_ID=a DATA_DIR=data/a uvicorn mini_s3.node:app --port 9001
"""

import hashlib
import os
import re
import tempfile
import time

from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.concurrency import run_in_threadpool

NODE_ID = os.environ.get("NODE_ID", "node")
DATA_DIR = os.environ.get("DATA_DIR", f"data/{NODE_ID}")
BLOB_ID = re.compile(r"^[0-9a-f]{32}$")

os.makedirs(DATA_DIR, exist_ok=True)
app = FastAPI(title=f"mini-s3 node {NODE_ID}")


def _path(blob_id):
    if not BLOB_ID.match(blob_id):
        raise HTTPException(400, "Invalid blob id")
    return os.path.join(DATA_DIR, blob_id)


@app.get("/health")
def health():
    # os.scandir reuses directory-listing metadata, so this stays cheap with many blobs.
    blobs = size = 0
    with os.scandir(DATA_DIR) as entries:
        for e in entries:
            if BLOB_ID.match(e.name):
                try:
                    size += e.stat().st_size
                except FileNotFoundError:  # deleted while we were counting
                    continue
                blobs += 1
    return {"node_id": NODE_ID, "blobs": blobs, "bytes": size}


@app.get("/blobs")
def list_blobs():
    """Every blob on this node with its age in seconds (used to find orphaned copies)."""
    now, out = time.time(), []
    with os.scandir(DATA_DIR) as entries:
        for e in entries:
            if BLOB_ID.match(e.name):
                try:
                    out.append({"id": e.name, "age": round(now - e.stat().st_mtime, 1)})
                except FileNotFoundError:
                    continue
    return out


@app.put("/blobs/{blob_id}", status_code=201)
async def put_blob(blob_id: str, request: Request):
    path = _path(blob_id)
    data = await request.body()
    expected = request.headers.get("x-checksum")
    actual = hashlib.sha256(data).hexdigest()
    if expected and expected != actual:
        raise HTTPException(422, "Checksum mismatch: data was corrupted in transit")
    # fsync blocks, so run it off the event loop to keep serving other requests.
    await run_in_threadpool(_write_atomic, path, data)
    return {"blob_id": blob_id, "checksum": actual, "size": len(data)}


def _write_atomic(path, data):
    """Write to a temp file then rename, so a crash never leaves a half-written blob."""
    fd, tmp = tempfile.mkstemp(dir=DATA_DIR, prefix=".tmp-")
    with os.fdopen(fd, "wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


@app.get("/blobs/{blob_id}")
def get_blob(blob_id: str):
    try:
        with open(_path(blob_id), "rb") as f:
            data = f.read()
    except FileNotFoundError:
        raise HTTPException(404, "Blob not found")
    return Response(data, media_type="application/octet-stream",
                    headers={"x-checksum": hashlib.sha256(data).hexdigest()})


@app.delete("/blobs/{blob_id}")
def delete_blob(blob_id: str):
    try:
        os.remove(_path(blob_id))
    except FileNotFoundError:
        pass  # already gone; deletes are idempotent
    return {"deleted": blob_id}
