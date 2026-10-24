"""Chaos test: load the cluster, crash a storage node or the leader gateway mid-traffic,
and measure what clients experience.

Writes results/chaos-<time>.md, a per-second CSV timeline, and an SVG chart.
"""

import asyncio
import csv
import hashlib
import json
import os
import random
import statistics
import time

import aiohttp

from . import cluster

ROOT = cluster.ROOT
RETRYABLE = (aiohttp.ClientError, asyncio.TimeoutError)


class Client:
    """Talks to any gateway and fails over to the others, like a real S3 client
    retrying across endpoints. A request only counts as failed if every gateway
    keeps failing for `retry_for` seconds."""

    def __init__(self, http, urls, retry_for=5.0):
        self.http, self.urls, self.retry_for = http, urls, retry_for
        self.preferred = 0

    async def request(self, method, path, data=None):
        deadline = time.monotonic() + self.retry_for
        attempt = 0
        while True:
            url = self.urls[(self.preferred + attempt) % len(self.urls)]
            try:
                async with self.http.request(method, f"{url}{path}", data=data,
                                             timeout=aiohttp.ClientTimeout(total=5)) as r:
                    body = await r.read()
                    if not (r.status == 503 and r.headers.get("x-ms3-retry")):
                        self.preferred = self.urls.index(url)
                        return r.status, body
            except RETRYABLE:
                pass
            attempt += 1
            if time.monotonic() > deadline:
                return None, b""
            if attempt % len(self.urls) == 0:
                await asyncio.sleep(0.05)

    async def status(self):
        code, body = await self.request("GET", "/_cluster")
        if code != 200:
            raise SystemExit("Can't get cluster status from any gateway. Is it running? ./ms3 cluster up")
        return json.loads(body)


async def _upload(client, n, bucket):
    sem = asyncio.Semaphore(32)
    expected, latencies = {}, []

    async def one(i):
        data = os.urandom(random.randint(1024, 64 * 1024))
        key = f"obj-{i:05d}"
        async with sem:
            t = time.perf_counter()
            code, body = await client.request("PUT", f"/{bucket}/{key}", data)
            latencies.append((time.perf_counter() - t) * 1000)
        if code != 201:
            raise SystemExit(f"Upload failed during setup ({code}): {body[:200]!r}")
        expected[key] = hashlib.sha256(data).hexdigest()

    await asyncio.gather(*(one(i) for i in range(n)))
    return expected, latencies


# Whole-test totals; the per-second timeline drops its last, partial second.
TOTALS = {"ok": 0, "fail": 0, "w_ok": 0, "w_fail": 0}


def _row(timeline, start):
    return timeline.setdefault(int(time.time() - start), {"ok": 0, "fail": 0, "w_ok": 0, "w_fail": 0, "lat": []})


def _count(timeline, start, key):
    _row(timeline, start)[key] += 1
    TOTALS[key] += 1


async def _reader(client, bucket, keys, stop, timeline, start):
    while not stop.is_set():
        t = time.perf_counter()
        code, _ = await client.request("GET", f"/{bucket}/{random.choice(keys)}")
        _count(timeline, start, "ok" if code == 200 else "fail")
        _row(timeline, start)["lat"].append((time.perf_counter() - t) * 1000)


async def _writer(client, writer_id, stop, timeline, start, written, write_lat):
    i = 0
    while not stop.is_set():
        data = os.urandom(random.randint(1024, 16 * 1024))
        key = f"w{writer_id}-{i:05d}"
        t = time.perf_counter()
        code, _ = await client.request("PUT", f"/chaos-writes/{key}", data)
        write_lat.append((time.perf_counter() - t) * 1000)
        if code == 201:
            _count(timeline, start, "w_ok")
            written[key] = hashlib.sha256(data).hexdigest()
        else:
            _count(timeline, start, "w_fail")
        i += 1


def _svg(timeline, kill_at, healed_at, killed_label, healed_label, show_under=True):
    secs = sorted(timeline)
    w, h, pad = 640, 220, 40
    max_rps = max(max(timeline[s]["ok"] + timeline[s]["fail"] for s in secs), 1)
    max_w = max(max(timeline[s]["w_ok"] + timeline[s]["w_fail"] for s in secs), 1)
    max_ur = max(max(timeline[s].get("under", 0) for s in secs), 1)
    x = lambda s: pad + (s - secs[0]) / max(secs[-1] - secs[0], 1) * (w - 2 * pad)
    y = lambda v, top: h - pad - v / top * (h - 2 * pad)
    line = lambda key, top, color: (f'<polyline points="{" ".join(f"{x(s):.1f},{y(timeline[s].get(key, 0), top):.1f}" for s in secs)}" '
                                    f'fill="none" stroke="{color}" stroke-width="2"/>')
    fails = "".join(f'<circle cx="{x(s):.1f}" cy="{y(timeline[s]["fail"] + timeline[s]["w_fail"], max_rps):.1f}" r="3" fill="#E24B4A"/>'
                    for s in secs if timeline[s]["fail"] or timeline[s]["w_fail"])
    marks = f'<line x1="{x(kill_at):.1f}" y1="{pad}" x2="{x(kill_at):.1f}" y2="{h - pad}" stroke="#E24B4A" stroke-dasharray="4 3"/>' \
            f'<text x="{x(kill_at) + 4:.1f}" y="{pad + 12}" fill="#E24B4A" font-size="11">{killed_label}</text>'
    if healed_at is not None:
        marks += f'<line x1="{x(healed_at):.1f}" y1="{pad}" x2="{x(healed_at):.1f}" y2="{h - pad}" stroke="#1D9E75" stroke-dasharray="4 3"/>' \
                 f'<text x="{x(healed_at) + 4:.1f}" y="{pad + 26}" fill="#1D9E75" font-size="11">{healed_label}</text>'
    return f'''<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {w} {h + 30}" font-family="sans-serif">
<rect width="{w}" height="{h + 30}" fill="white"/>
<line x1="{pad}" y1="{h - pad}" x2="{w - pad}" y2="{h - pad}" stroke="#888"/>
{line("ok", max_rps, "#378ADD")}{line("w_ok", max_w, "#7F77DD")}{line("under", max_ur, "#EF9F27") if show_under else ""}
{fails}{marks}
<text x="{pad}" y="{h - 18}" font-size="11" fill="#555">seconds</text>
<text x="{pad}" y="{h + 12}" font-size="11" fill="#378ADD">reads/sec (max {max_rps})</text>
<text x="{pad + 140}" y="{h + 12}" font-size="11" fill="#7F77DD">writes/sec (max {max_w})</text>
{f'<text x="{pad + 280}" y="{h + 12}" font-size="11" fill="#EF9F27">under-replicated (max {max_ur})</text>' if show_under else ""}
<text x="{pad + 460}" y="{h + 12}" font-size="11" fill="#E24B4A">failed requests</text>
</svg>'''


async def _leader_now(http, state, exclude):
    for gid, g in state["gateways"].items():
        if gid == exclude:
            continue
        try:
            async with http.get(f"http://127.0.0.1:{g['port']}/_raft", timeout=aiohttp.ClientTimeout(total=0.3)) as r:
                s = await r.json()
                if s["role"] == "leader":
                    return s
        except (*RETRYABLE, ValueError):
            continue
    return None


async def run(n_objects=500, victim=None, readers=16, kill="node", writers=4, settle=3.0, timeout=60.0):
    state = cluster._require()
    bucket = "chaos"
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=100)) as http:
        client = Client(http, cluster.gateway_urls())
        status = await client.status()
        down = [n["id"] for n in status["nodes"] if not n["alive"]]
        down += [g for g, p in status["raft"]["peers"].items() if p["acked_ago"] is None or p["acked_ago"] > 1]
        if down:
            fix = "\n".join(f"  ./ms3 cluster start {n}" for n in down)
            raise SystemExit(f"{', '.join(down)} already down. The test needs a healthy cluster; run:\n{fix}")

        print(f"1/4  Uploading {n_objects} objects (1-64 KB each)...")
        expected, put_lat = await _upload(client, n_objects, bucket)
        await asyncio.sleep(1)
        status = await client.status()
        if kill == "leader":
            victim = status["raft"]["id"]
            what = f"the leader gateway {victim}"
        else:
            victim = victim or max(status["nodes"], key=lambda n: n["blobs"])["id"]
            what = f"storage node {victim}"

        print(f"2/4  {readers} clients reading and {writers} writing, then killing {what}...")
        timeline, stop, start = {}, asyncio.Event(), time.time()
        TOTALS.update(ok=0, fail=0, w_ok=0, w_fail=0)
        written, write_lat = {}, []
        tasks = [asyncio.create_task(_reader(client, bucket, list(expected), stop, timeline, start))
                 for _ in range(readers)]
        tasks += [asyncio.create_task(_writer(client, i, stop, timeline, start, written, write_lat))
                  for i in range(writers)]
        await asyncio.sleep(settle)
        kill_at = time.time() - start
        cluster.kill(victim)

        print("3/4  Waiting for the cluster to recover...")
        healed_at, new_leader, repairs_before = None, None, status["repairs"]
        while time.time() - start < kill_at + timeout and healed_at is None:
            if kill == "leader":
                new_leader = await _leader_now(http, state, exclude=victim)
                if new_leader:
                    healed_at = time.time() - start
            else:
                s = await client.status()
                _row(timeline, start)["under"] = s["under_replicated"]
                victim_down = not next(n for n in s["nodes"] if n["id"] == victim)["alive"]
                if victim_down and s["repairs"] > repairs_before and s["under_replicated"] == 0:
                    healed_at = time.time() - start
            await asyncio.sleep(0.05 if kill == "leader" else 0.25)
        await asyncio.sleep(settle)  # keep the load running a bit after recovery
        stop.set()
        await asyncio.gather(*tasks)
        timeline.pop(max(timeline), None)  # the final second is only partly measured

        print("4/4  Verifying every object...")
        bad = []
        for b, objects in (("chaos", expected), ("chaos-writes", written)):
            for key, checksum in objects.items():
                code, body = await client.request("GET", f"/{b}/{key}")
                if code != 200 or hashlib.sha256(body).hexdigest() != checksum:
                    bad.append(f"{b}/{key}")
        final = await client.status()

    reads_ok, reads_fail, writes_ok, writes_fail = TOTALS["ok"], TOTALS["fail"], TOTALS["w_ok"], TOTALS["w_fail"]
    lats = [l for r in timeline.values() for l in r["lat"]]
    p = lambda xs, q: statistics.quantiles(xs, n=100)[q - 1] if len(xs) >= 2 else (xs[0] if xs else 0)
    pct = lambda ok, fail: f"{100 * ok / max(ok + fail, 1):.2f}%"
    recover_secs = None if healed_at is None else healed_at - kill_at

    if kill == "leader":
        recovery_rows = f"""| Time until a new leader was elected | {f'{recover_secs * 1000:.0f} ms' if recover_secs is not None else 'not within the timeout'} |
| New leader | {new_leader['id'] if new_leader else 'none'} (term {new_leader['term'] if new_leader else '?'}) |"""
        killed_label, healed_label = "leader killed", "new leader"
    else:
        recovery_rows = f"""| Copies re-created by the repair worker | {final['repairs'] - repairs_before} |
| Time from crash to fully re-replicated | {f'{recover_secs:.0f} s' if recover_secs is not None else f'not within {timeout:.0f} s'} |"""
        killed_label, healed_label = "node killed", "fully re-replicated"

    stamp = time.strftime("%Y%m%d-%H%M%S")
    out_dir = os.path.join(ROOT, "results")
    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, f"chaos-{stamp}.csv"), "w", newline="") as f:
        wr = csv.writer(f)
        wr.writerow(["second", "reads_ok", "reads_failed", "writes_ok", "writes_failed", "read_p50_ms", "under_replicated"])
        for sec in sorted(timeline):
            r = timeline[sec]
            wr.writerow([sec, r["ok"], r["fail"], r["w_ok"], r["w_fail"],
                         round(p(r["lat"], 50), 1) if r["lat"] else "", r.get("under", "")])
    svg_name = f"chaos-{stamp}.svg"
    with open(os.path.join(out_dir, svg_name), "w") as f:
        f.write(_svg(timeline, int(kill_at), None if healed_at is None else int(healed_at), killed_label, healed_label,
                     show_under=kill != "leader"))

    report = f"""# Chaos test {stamp}

Cluster: {len(state['nodes'])} storage nodes, {len(state['gateways'])} gateways (Raft), {final['replicas']} replicas, write quorum {final['write_quorum']}.
Killed {what} with SIGKILL while {readers} clients read and {writers} clients wrote.

| Metric | Result |
|---|---|
| Objects uploaded before the crash | {n_objects} |
| Reads during test | {reads_ok + reads_fail} ({reads_fail} failed, {pct(reads_ok, reads_fail)} success) |
| Writes during test | {writes_ok + writes_fail} ({writes_fail} failed, {pct(writes_ok, writes_fail)} success) |
| Read latency p50 / p99 / max | {p(lats, 50):.1f} ms / {p(lats, 99):.1f} ms / {max(lats, default=0):.0f} ms |
| Write latency p50 / p99 / max | {p(write_lat, 50):.1f} ms / {p(write_lat, 99):.1f} ms / {max(write_lat, default=0):.0f} ms |
{recovery_rows}
| Objects intact afterwards (checksum verified) | {n_objects + len(written) - len(bad)} / {n_objects + len(written)} |

![timeline]({svg_name})
"""
    with open(os.path.join(out_dir, f"chaos-{stamp}.md"), "w") as f:
        f.write(report)
    print("\n" + report.split("\n", 1)[1].rsplit("![", 1)[0].strip())
    print(f"\nSaved results/chaos-{stamp}.md (with a chart and CSV).")
    print(f"Bring it back with: ./ms3 cluster start {victim}")
    return {"failed_reads": reads_fail, "failed_writes": writes_fail, "bad": bad, "recover_secs": recover_secs}
