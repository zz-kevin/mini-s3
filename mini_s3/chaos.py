"""Chaos demo: load the cluster, crash a node mid-traffic, and measure what happens.

Writes results/chaos-<time>.md, a per-second CSV timeline, and an SVG chart.
"""

import asyncio
import csv
import hashlib
import os
import random
import statistics
import time

import aiohttp

from . import cluster

ROOT = cluster.ROOT
GATEWAY = f"http://127.0.0.1:{cluster.GATEWAY_PORT}"


async def _upload(http, n, bucket):
    sem = asyncio.Semaphore(32)
    expected, latencies = {}, []

    async def one(i):
        data = os.urandom(random.randint(1024, 64 * 1024))
        key = f"obj-{i:05d}"
        async with sem:
            t = time.perf_counter()
            async with http.put(f"{GATEWAY}/{bucket}/{key}", data=data) as r:
                r.raise_for_status()
            latencies.append((time.perf_counter() - t) * 1000)
        expected[key] = hashlib.sha256(data).hexdigest()

    await asyncio.gather(*(one(i) for i in range(n)))
    return expected, latencies


async def _status(http):
    async with http.get(f"{GATEWAY}/_cluster") as r:
        return await r.json()


async def _reader(http, bucket, keys, stop, timeline, start):
    while not stop.is_set():
        key = random.choice(keys)
        t = time.perf_counter()
        try:
            async with http.get(f"{GATEWAY}/{bucket}/{key}", timeout=aiohttp.ClientTimeout(total=5)) as r:
                await r.read()
                ok = r.status == 200
        except (aiohttp.ClientError, asyncio.TimeoutError):
            ok = False
        sec = int(time.time() - start)
        row = timeline.setdefault(sec, {"ok": 0, "fail": 0, "lat": []})
        row["ok" if ok else "fail"] += 1
        row["lat"].append((time.perf_counter() - t) * 1000)


def _svg(timeline, kill_at, healed_at):
    secs = sorted(timeline)
    w, h, pad = 640, 220, 40
    max_rps = max(max(timeline[s]["ok"] + timeline[s]["fail"] for s in secs), 1)
    max_ur = max(max(timeline[s].get("under", 0) for s in secs), 1)
    x = lambda s: pad + (s - secs[0]) / max(secs[-1] - secs[0], 1) * (w - 2 * pad)
    y_rps = lambda v: h - pad - v / max_rps * (h - 2 * pad)
    y_ur = lambda v: h - pad - v / max_ur * (h - 2 * pad)
    ok_pts = " ".join(f"{x(s):.1f},{y_rps(timeline[s]['ok']):.1f}" for s in secs)
    ur_pts = " ".join(f"{x(s):.1f},{y_ur(timeline[s].get('under', 0)):.1f}" for s in secs)
    fails = "".join(f'<circle cx="{x(s):.1f}" cy="{y_rps(timeline[s]["fail"]):.1f}" r="3" fill="#E24B4A"/>'
                    for s in secs if timeline[s]["fail"])
    marks = f'<line x1="{x(kill_at):.1f}" y1="{pad}" x2="{x(kill_at):.1f}" y2="{h - pad}" stroke="#E24B4A" stroke-dasharray="4 3"/>' \
            f'<text x="{x(kill_at) + 4:.1f}" y="{pad + 12}" fill="#E24B4A" font-size="11">node killed</text>'
    if healed_at is not None:
        marks += f'<line x1="{x(healed_at):.1f}" y1="{pad}" x2="{x(healed_at):.1f}" y2="{h - pad}" stroke="#1D9E75" stroke-dasharray="4 3"/>' \
                 f'<text x="{x(healed_at) + 4:.1f}" y="{pad + 26}" fill="#1D9E75" font-size="11">fully re-replicated</text>'
    return f'''<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {w} {h + 30}" font-family="sans-serif">
<rect width="{w}" height="{h + 30}" fill="white"/>
<line x1="{pad}" y1="{h - pad}" x2="{w - pad}" y2="{h - pad}" stroke="#888"/>
<polyline points="{ok_pts}" fill="none" stroke="#378ADD" stroke-width="2"/>
<polyline points="{ur_pts}" fill="none" stroke="#EF9F27" stroke-width="2"/>
{fails}{marks}
<text x="{pad}" y="{h - 18}" font-size="11" fill="#555">seconds</text>
<text x="{pad}" y="{h + 12}" font-size="11" fill="#378ADD">successful reads/sec (max {max_rps})</text>
<text x="{pad + 210}" y="{h + 12}" font-size="11" fill="#EF9F27">under-replicated objects (max {max_ur})</text>
<text x="{pad + 450}" y="{h + 12}" font-size="11" fill="#E24B4A">failed reads</text>
</svg>'''


async def run(n_objects=500, victim=None, readers=16, settle=3.0, timeout=60.0):
    state = cluster._require()
    bucket = "chaos"
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=100)) as http:
        status = await _status(http)
        down = [n["id"] for n in status["nodes"] if not n["alive"]]
        if down:
            fix = "\n".join(f"  ./ms3 cluster start {n}" for n in down)
            raise SystemExit(f"Node(s) {', '.join(down)} already down. The test needs a healthy cluster; run:\n{fix}")

        print(f"1/4  Uploading {n_objects} objects (1-64 KB each)...")
        expected, put_lat = await _upload(http, n_objects, bucket)
        await asyncio.sleep(1)
        status = await _status(http)
        victim = victim or max(status["nodes"], key=lambda n: n["blobs"])["id"]
        if victim not in {n["id"] for n in status["nodes"]}:
            raise SystemExit(f"No node {victim!r}.")

        print(f"2/4  Reading with {readers} concurrent clients, then killing node {victim}...")
        timeline, stop, start = {}, asyncio.Event(), time.time()
        tasks = [asyncio.create_task(_reader(http, bucket, list(expected), stop, timeline, start))
                 for _ in range(readers)]
        await asyncio.sleep(settle)
        kill_at = int(time.time() - start)
        cluster.kill_node(victim)

        print("3/4  Waiting for the cluster to notice and re-replicate...")
        healed_at, max_under, repairs_before = None, 0, status["repairs"]
        while time.time() - start < kill_at + timeout:
            s = await _status(http)
            sec = int(time.time() - start)
            timeline.setdefault(sec, {"ok": 0, "fail": 0, "lat": []})["under"] = s["under_replicated"]
            max_under = max(max_under, s["under_replicated"])
            victim_down = not next(n for n in s["nodes"] if n["id"] == victim)["alive"]
            repaired = s["repairs"] > repairs_before
            if victim_down and repaired and s["under_replicated"] == 0 and healed_at is None:
                healed_at = sec
                await asyncio.sleep(settle)  # keep reading a bit after healing
                break
            await asyncio.sleep(0.25)
        stop.set()
        await asyncio.gather(*tasks)
        timeline.pop(max(timeline), None)  # the final second is only partly measured

        print("4/4  Verifying every object...")
        bad = []
        for key, checksum in expected.items():
            async with http.get(f"{GATEWAY}/{bucket}/{key}") as r:
                body = await r.read()
                if r.status != 200 or hashlib.sha256(body).hexdigest() != checksum:
                    bad.append(key)
        copies_restored = (await _status(http))["repairs"] - repairs_before

    total_ok = sum(r["ok"] for r in timeline.values())
    total_fail = sum(r["fail"] for r in timeline.values())
    lats = [l for r in timeline.values() for l in r["lat"]]
    p = lambda xs, q: statistics.quantiles(xs, n=100)[q - 1] if len(xs) >= 2 else (xs[0] if xs else 0)
    heal_secs = None if healed_at is None else healed_at - kill_at

    stamp = time.strftime("%Y%m%d-%H%M%S")
    out_dir = os.path.join(ROOT, "results")
    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, f"chaos-{stamp}.csv"), "w", newline="") as f:
        wr = csv.writer(f)
        wr.writerow(["second", "reads_ok", "reads_failed", "p50_ms", "under_replicated"])
        for sec in sorted(timeline):
            r = timeline[sec]
            wr.writerow([sec, r["ok"], r["fail"], round(p(r["lat"], 50), 1) if r["lat"] else "", r.get("under", "")])
    svg_name = f"chaos-{stamp}.svg"
    with open(os.path.join(out_dir, svg_name), "w") as f:
        f.write(_svg(timeline, kill_at, healed_at))

    report = f"""# Chaos test {stamp}

Cluster: {len(state['nodes'])} nodes, {status['replicas']} replicas, write quorum {status['write_quorum']}.
Killed node **{victim}** with SIGKILL during a read load of {readers} concurrent clients.

| Metric | Result |
|---|---|
| Objects uploaded | {n_objects} |
| Upload latency p50 / p99 | {p(put_lat, 50):.1f} ms / {p(put_lat, 99):.1f} ms |
| Reads during test | {total_ok + total_fail} ({total_fail} failed, {100 * total_ok / max(total_ok + total_fail, 1):.2f}% success) |
| Read latency p50 / p99 | {p(lats, 50):.1f} ms / {p(lats, 99):.1f} ms |
| Copies re-created by the repair worker | {copies_restored} |
| Time from crash to fully re-replicated | {f'{heal_secs} s' if heal_secs is not None else f'not within {timeout:.0f} s'} |
| Objects intact afterwards (checksum verified) | {n_objects - len(bad)} / {n_objects} |

![timeline]({svg_name})
"""
    with open(os.path.join(out_dir, f"chaos-{stamp}.md"), "w") as f:
        f.write(report)
    print("\n" + report.split("\n", 1)[1].rsplit("![", 1)[0].strip())
    print(f"\nSaved results/chaos-{stamp}.md (with a chart and CSV).")
    print(f"Bring the node back with: ./ms3 cluster start {victim}")
    return {"failed_reads": total_fail, "bad": bad, "heal_secs": heal_secs}
