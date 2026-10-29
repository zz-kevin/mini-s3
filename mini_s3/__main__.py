"""Command-line interface: ./ms3 <command>  (run ./ms3 --help)"""

import argparse
import asyncio
import json
import sys
import time

import httpx

from . import agents, chaos, cluster, faults, oncall

RETRY_FOR = 5.0  # seconds to keep trying other gateways while a new leader is elected


def _split(path):
    bucket, _, key = path.partition("/")
    if not bucket or not key:
        sys.exit(f"Expected bucket/key, got {path!r}")
    return bucket, key


def _request(method, path, **kw):
    """Send a request to any gateway, failing over to the others if one is down
    or mid-election. Exits with a readable message on errors."""
    urls = cluster.gateway_urls()
    deadline = time.time() + RETRY_FOR
    r, reachable = None, False
    while True:
        for url in urls:
            try:
                r = httpx.request(method, f"{url}{path}", timeout=30, **kw)
            except httpx.TransportError:
                continue
            reachable = True
            # mini-s3 errors are always JSON; an HTML error page means another app owns the port.
            if r.status_code >= 400 and "text/html" in r.headers.get("content-type", ""):
                sys.exit(f"Something other than mini-s3 answered at {url} "
                         "(Docker or another app may be using the port). Is the cluster running? Try: ./ms3 cluster up")
            if r.status_code == 503 and r.headers.get("x-ms3-retry"):
                continue  # leadership is moving; try the next gateway
            break
        else:
            if time.time() < deadline:
                time.sleep(0.2)
                continue
            if not reachable:
                sys.exit("Can't reach any gateway. Start the cluster with: ./ms3 cluster up")
        break
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
    cu = csub.add_parser("up", help="start nodes and gateways")
    cu.add_argument("--nodes", type=int, default=4)
    cu.add_argument("--replicas", type=int, default=3)
    cu.add_argument("--quorum", type=int, default=2, help="replicas that must confirm a write")
    cu.add_argument("--gateways", type=int, default=3, help="Raft gateways (odd number)")
    csub.add_parser("down", help="stop everything")
    ck = csub.add_parser("kill", help="crash a node or gateway (SIGKILL)")
    ck.add_argument("name", help="node (a), gateway (g1), or 'leader'")
    cs = csub.add_parser("start", help="restart a node or gateway")
    cs.add_argument("name", help="node (a) or gateway (g1)")

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

    ch = sub.add_parser("chaos", help="crash a node or the leader gateway under load and measure recovery")
    ch.add_argument("--objects", type=int, default=500)
    ch.add_argument("--kill", default="node", choices=["node", "leader"],
                    help="crash a storage node (default) or the Raft leader gateway")
    ch.add_argument("--node", help="storage node to kill (default: the one holding the most data)")
    ch.add_argument("--readers", type=int, default=16)

    oc = sub.add_parser("oncall", help="break a sandbox cluster, let an agent fix it, and score it")
    oc.add_argument("--scenario", default="all", choices=["all", *faults.SCENARIOS])
    oc.add_argument("--agent", default="all",
                    help=f"all (the scripted agents), {', '.join(agents.AGENTS)}, or ollama:<model> "
                         "for a local model, e.g. ollama:qwen3:8b; separate several with commas")
    oc.add_argument("--guard", default="both", choices=["on", "off", "both"],
                    help="block unsafe actions (on), only record them (off), or run both")
    oc.add_argument("--seeds", type=int, default=1, help="episodes per combination")
    oc.add_argument("--objects", type=int, default=100)

    a = p.parse_args()

    if a.cmd == "cluster":
        {"up": lambda: cluster.up(a.nodes, a.replicas, a.quorum, a.gateways), "down": cluster.down,
         "kill": lambda: cluster.kill(a.name), "start": lambda: cluster.start(a.name)}[a.action]()
    elif a.cmd == "status":
        s = _request("GET", "/_cluster").json()
        gateways = {s["raft"]["id"]: s["raft"]}
        for g in sorted(set(cluster._require()["gateways"]) - set(gateways)):
            peer = s["raft"]["peers"].get(g, {})
            ok = peer.get("acked_ago") is not None and peer["acked_ago"] < 1
            gateways[g] = {"role": "follower" if ok else "DOWN", "match_index": peer.get("match_index")}
        for g, info in sorted(gateways.items()):
            extra = f"term {info['term']}, log {info['commit_index']}" if info["role"] == "leader" \
                else (f"log {info['match_index']}" if info["role"] == "follower" else "not responding")
            print(f"  gateway {g}  {info['role']:<8}  ({extra})")
        for n in s["nodes"]:
            state = "up  " if n["alive"] else "DOWN"
            print(f"  node {n['id']}  {state}  {n['blobs']:>6} blobs  {n['bytes'] / 1e6:8.2f} MB  ({n['for_seconds']:.0f}s)")
        print(f"  objects: {s['objects']}   under-replicated: {s['under_replicated']}   "
              f"lost: {s['lost']}   repairs so far: {s['repairs']}")
    elif a.cmd == "put":
        bucket, key = _split(a.path)
        with open(a.file, "rb") as f:
            r = _request("PUT", f"/{bucket}/{key}", content=f.read())
        print(json.dumps(r.json(), indent=2))
    elif a.cmd == "get":
        bucket, key = _split(a.path)
        r = _request("GET", f"/{bucket}/{key}")
        if a.output:
            with open(a.output, "wb") as f:
                f.write(r.content)
            print(f"Saved {len(r.content)} bytes to {a.output} (served by node {r.headers.get('x-served-by')})")
        else:
            sys.stdout.buffer.write(r.content)
    elif a.cmd == "ls":
        for o in _request("GET", f"/{a.bucket}", params={"prefix": a.prefix}).json()["objects"]:
            print(f"{o['size']:>10}  {o['updated_at']}  {o['key']}")
    elif a.cmd == "rm":
        bucket, key = _split(a.path)
        _request("DELETE", f"/{bucket}/{key}")
        print(f"Deleted {a.path}")
    elif a.cmd == "chaos":
        asyncio.run(chaos.run(a.objects, a.node, a.readers, kill=a.kill))
    elif a.cmd == "oncall":
        oncall.run([*faults.SCENARIOS] if a.scenario == "all" else [a.scenario],
                   [*agents.AGENTS] if a.agent == "all" else a.agent.split(","),
                   {"on": [True], "off": [False], "both": [True, False]}[a.guard], a.seeds, a.objects)


if __name__ == "__main__":
    main()
