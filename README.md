# mini-s3

A replicated object store inspired by Amazon S3. Files are spread across storage nodes with **consistent hashing**, every object is kept on **3 nodes**, writes need a **quorum of 2**, and a background **repair worker** restores lost copies when a node dies. It comes with a chaos test that crashes a node under live traffic and measures what happens.

## Results

Four storage nodes, 1,000 objects (1–64 KB each), and 16 clients reading continuously. Midway through, one node was killed with `SIGKILL`.

| Metric | Result |
|---|---|
| Reads during the test | 8,303, **0 failed (100% success)** |
| Read latency p50 / p99 | 17 ms / 82 ms |
| Upload latency p50 / p99 (32 concurrent clients) | 73 ms / 465 ms |
| Copies re-created by the repair worker | 800 |
| Time from crash to every object back at 3 copies | **5 s** (2 s of that is failure detection) |
| Objects intact afterwards (SHA-256 verified) | **1,000 / 1,000** |
| Files on disk after the node rejoined | exactly 3,000 (extra copies cleaned up) |

The chart shows successful reads per second (blue) and under-replicated objects (orange) around the crash.

![Chaos test timeline](docs/chaos-timeline.svg)

Measured on a MacBook Pro with all processes on one machine. Latency varies from run to run (median reads were 11–18 ms on an empty cluster), but across more than a dozen runs, including ones that overwrote existing data, every run had 0 failed reads and all objects intact. Reproduce with `./ms3 chaos`.

## Architecture

```
            client (./ms3 CLI, curl, any HTTP client)
                         │  PUT / GET / DELETE /bucket/key
                         ▼
 ┌──────────────────────────── gateway ───────────────────────────┐
 │  API ─ hash ring ─ metadata (SQLite) ─ health checker ─ repair │
 └───────┬───────────────┬───────────────┬───────────────┬────────┘
         ▼               ▼               ▼               ▼
      node a          node b          node c          node d
   (blobs on disk) (blobs on disk) (blobs on disk) (blobs on disk)
```

| File | Role |
|---|---|
| [`mini_s3/ring.py`](mini_s3/ring.py) | Consistent hash ring with 64 virtual nodes per server. Adding a 5th node moves about 20% of keys, versus about 80% with `hash % n`. |
| [`mini_s3/node.py`](mini_s3/node.py) | Storage node. Atomic writes (temp file, `fsync`, rename) and a SHA-256 check on every write. |
| [`mini_s3/gateway.py`](mini_s3/gateway.py) | S3-style API, quorum writes, read failover, read repair, health checks and the repair worker. |
| [`mini_s3/metadata.py`](mini_s3/metadata.py) | Which blob backs each key and which nodes hold it, plus a queue of pending deletes. |
| [`mini_s3/chaos.py`](mini_s3/chaos.py) | Load generator and crash test. Writes a report, a CSV and a chart to `results/`. |
| [`mini_s3/static/status.html`](mini_s3/static/status.html) | Live dashboard at `http://127.0.0.1:7480/`. |

### How a write works
1. Hash `bucket/key` onto the ring and walk clockwise to get the key's **preference list** of nodes.
2. Send the bytes and their checksum to the first 3 *live* nodes in parallel.
3. If at least 2 confirm (the **write quorum**), record the new version in metadata and return `201`. Otherwise, clean up and return `503`. Writes never silently succeed with too few copies.
4. Every write gets a new blob id, so an overwrite never modifies data in place. The old version's copies are queued for deletion.

### How a read works
Try the replicas in ring order, live nodes first, and verify the SHA-256 of whatever comes back. A dead node or a corrupt copy is skipped. A node that *answers* with a missing or corrupt copy gets the good bytes written back immediately (**read repair**).

### How failures are handled
- **Detection:** the gateway checks every node each second. A node is marked down only after 2 consecutive misses, so a single slow reply doesn't set off a wave of re-copying.
- **Re-replication:** every 2 seconds the repair worker finds objects with fewer than 3 live copies and copies them, from a verified source, to the next live nodes on the ring.
- **Rejoin:** when a node comes back, objects briefly have 4 copies. Extra copies are removed once all 3 "home" nodes hold the data, so the cluster rebalances instead of growing forever.
- **Deletes while a node is down:** they succeed immediately, and the blob deletion waits in a queue until the node returns.

## Run it

Needs Python 3.10+. Everything runs locally and costs nothing.

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements-dev.txt
./ms3 cluster up                     # 4 nodes + gateway; dashboard at http://127.0.0.1:7480/
./ms3 put README.md docs/readme.md   # upload a file
./ms3 get docs/readme.md             # download it
./ms3 ls docs
./ms3 cluster kill b                 # crash a node, then watch the dashboard
./ms3 status
./ms3 cluster start b                # bring it back
./ms3 chaos --objects 1000           # full crash test with a report
./ms3 cluster down
.venv/bin/python -m pytest -q        # 14 tests, including real multi-process failure scenarios
```

Any HTTP client works too: `curl -T photo.jpg http://127.0.0.1:7480/photos/cat.jpg`, then `curl http://127.0.0.1:7480/photos/cat.jpg -o cat.jpg`.

The gateway uses port 7480. If that port is taken, run `MS3_PORT=7481 ./ms3 cluster up` and use the same `MS3_PORT` for every `./ms3` command.

## Bugs the chaos test found

Each of these passed the basic tests and only showed up when a node was killed under load.

1. **The throughput bottleneck was the HTTP client, not the server.** Uploads ran at 48/s. Profiling showed the gateway served about 1,250 trivial requests per second, but httpx's async connection pool stalled at high concurrency. Switching the internal client to aiohttp gave **10× faster uploads (48 → 508/s) and 7× faster reads (184 → 1,263/s)**.
2. **Reads in the second after a crash orphaned data.** Before the health checker noticed the crash, reads got "connection refused" and treated it as "this copy is corrupt", so the gateway stopped tracking 106 perfectly good copies. The fix separates "node unreachable" from "copy bad", and any copy that stops being tracked is queued for deletion so nothing is orphaned.
3. **A race made a healthy node look dead.** The health endpoint counted files while the repair worker was deleting them, crashed with `FileNotFoundError`, and the node "flapped" down. The fix tolerates the race and requires 2 consecutive misses before declaring a node dead.
4. **The repair metric hid the damage.** Under-replicated counts were published only after each repair pass finished, so the dashboard always showed 0. They're now published before copying starts.
5. **Cleanup slowed down healing.** When a test overwrote 1,000 existing objects, the 1,000 old versions were deleted one at a time inside the repair pass, so each pass dragged and healing took **37 s**. Running deletes in parallel, the same way copies already ran, brought the same scenario down to **6 s**, and typical read latency from 63 ms to 18 ms.

## Limitations and next steps

- **The gateway is a single point of failure**, because metadata lives in one SQLite file. Next step: replicate metadata with Raft, or run several gateways over a shared store such as DynamoDB.
- **Objects are held in memory** while passing through the gateway (64 MB limit). Next step: streaming plus multipart upload.
- **No background scrubbing.** Corruption is only found when an object is read. Next step: a scrubber that re-checks every checksum periodically.
- **No auth and no versioning API.**
- **Deploying to real machines:** run each node on its own EC2 instance (Free Tier) and point `NODES` at them. Set an AWS budget alarm first.
