"""Command-line interface: ./ms3 <command>  (run ./ms3 --help)"""

import argparse
import asyncio
import json
import sys

import httpx

from . import chaos, cluster

GATEWAY = f"http://127.0.0.1:{cluster.GATEWAY_PORT}"


def _split(path):
    bucket, _, key = path.partition("/")
    if not bucket or not key:
        sys.exit(f"Expected bucket/key, got {path!r}")
    return bucket, key


def _request(method, url, **kw):
    try:
        r = httpx.request(method, url, timeout=30, **kw)
    except httpx.ConnectError:
        sys.exit("Can't reach the gateway. Start the cluster with: ./ms3 cluster up")
    # mini-s3 errors are always JSON; an HTML error page means another app owns the port.
    if r.status_code >= 400 and "text/html" in r.headers.get("content-type", ""):
        sys.exit(f"Something other than mini-s3 answered on port {cluster.GATEWAY_PORT} "
                 "(Docker or another app may be using it). Is the cluster running? Try: ./ms3 cluster up")
    if r.status_code >= 400:
        try:
            detail = r.json().get("detail", r.text)
        except ValueError:
            detail = r.text
        sys.exit(f"Error {r.status_code}: {detail}")
    return r


def main():
    p = argparse.ArgumentParser(prog="ms3", description="mini-s3: a replicated object store")
    sub = p.add_subparsers(dest="cmd", required=True)

    c = sub.add_parser("cluster", help="manage the local cluster")
    csub = c.add_subparsers(dest="action", required=True)
    cu = csub.add_parser("up", help="start nodes and gateway")
    cu.add_argument("--nodes", type=int, default=4)
    cu.add_argument("--replicas", type=int, default=3)
    cu.add_argument("--quorum", type=int, default=2, help="replicas that must confirm a write")
    csub.add_parser("down", help="stop everything")
    ck = csub.add_parser("kill", help="crash a node (SIGKILL)")
    ck.add_argument("node")
    cs = csub.add_parser("start", help="restart a node")
    cs.add_argument("node")

    sub.add_parser("status", help="show nodes and replication health")
    put = sub.add_parser("put", help="upload a file")
    put.add_argument("file")
    put.add_argument("path", help="bucket/key")
    get = sub.add_parser("get", help="download an object")
    get.add_argument("path", help="bucket/key")
    get.add_argument("-o", "--output", help="file to write (default: stdout)")
    ls = sub.add_parser("ls", help="list a bucket")
    ls.add_argument("bucket")
    ls.add_argument("--prefix", default="")
    rm = sub.add_parser("rm", help="delete an object")
    rm.add_argument("path", help="bucket/key")

    ch = sub.add_parser("chaos", help="crash a node under load and measure recovery")
    ch.add_argument("--objects", type=int, default=500)
    ch.add_argument("--node", help="node to kill (default: the one holding the most data)")
    ch.add_argument("--readers", type=int, default=16)

    a = p.parse_args()

    if a.cmd == "cluster":
        {"up": lambda: cluster.up(a.nodes, a.replicas, a.quorum), "down": cluster.down,
         "kill": lambda: cluster.kill_node(a.node), "start": lambda: cluster.start_node(a.node)}[a.action]()
    elif a.cmd == "status":
        s = _request("GET", f"{GATEWAY}/_cluster").json()
        for n in s["nodes"]:
            state = "up  " if n["alive"] else "DOWN"
            print(f"  node {n['id']}  {state}  {n['blobs']:>6} blobs  {n['bytes'] / 1e6:8.2f} MB  ({n['for_seconds']:.0f}s)")
        print(f"  objects: {s['objects']}   under-replicated: {s['under_replicated']}   "
              f"lost: {s['lost']}   repairs so far: {s['repairs']}")
    elif a.cmd == "put":
        bucket, key = _split(a.path)
        with open(a.file, "rb") as f:
            r = _request("PUT", f"{GATEWAY}/{bucket}/{key}", content=f.read())
        print(json.dumps(r.json(), indent=2))
    elif a.cmd == "get":
        bucket, key = _split(a.path)
        r = _request("GET", f"{GATEWAY}/{bucket}/{key}")
        if a.output:
            with open(a.output, "wb") as f:
                f.write(r.content)
            print(f"Saved {len(r.content)} bytes to {a.output} (served by node {r.headers.get('x-served-by')})")
        else:
            sys.stdout.buffer.write(r.content)
    elif a.cmd == "ls":
        for o in _request("GET", f"{GATEWAY}/{a.bucket}", params={"prefix": a.prefix}).json()["objects"]:
            print(f"{o['size']:>10}  {o['updated_at']}  {o['key']}")
    elif a.cmd == "rm":
        bucket, key = _split(a.path)
        _request("DELETE", f"{GATEWAY}/{bucket}/{key}")
        print(f"Deleted {a.path}")
    elif a.cmd == "chaos":
        asyncio.run(chaos.run(a.objects, a.node, a.readers))


if __name__ == "__main__":
    main()
