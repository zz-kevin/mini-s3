# mini-s3 design

**Author:** Kevin Boansi · **Status:** implemented · **Code:** about 1,800 lines of Python, 26 tests

## Problem

Object storage such as Amazon S3 has to keep data safe and available while individual machines fail, which at scale is constant. The goal of this project was to build a small object store that **loses no acknowledged data and keeps serving requests when any single process crashes** (a storage node or the coordinator), and to prove it by measurement rather than argument.

**Goals**
- No acknowledged write is lost, and nothing returns corrupt data, if any one process dies.
- Clients keep working through a crash, with no manual intervention.
- Read-after-write consistency: once a `PUT` returns, every gateway serves the new version.
- Repair is automatic, and the cluster returns to full redundancy without operator action.

**Non-goals:** multi-datacenter replication, authentication, large-object streaming, and changing cluster membership at runtime.

## Design overview

The system has two layers with different needs, so they use different techniques:

1. **Data layer (storage nodes):** stores immutable blobs. Throughput matters more than coordination, so replication is simple: write to 3 nodes and wait for 2.
2. **Metadata layer (gateways):** records which blob each key points to and where its copies live. This must be strongly consistent, so 3 gateways replicate it with the Raft consensus algorithm.

The key idea that links them: **every write creates a new, immutable blob with a new id and a SHA-256 checksum.** Blobs never change after they're written, so the only thing that needs strong consistency is the small metadata pointer. Any copy whose checksum matches is guaranteed to be the right version. Reads therefore need only one good copy, not a read quorum, and still get read-after-write consistency.

## Key decisions

| Decision | Chosen | Alternatives considered | Why |
|---|---|---|---|
| Data placement | Consistent hashing, 64 virtual nodes per server | `hash(key) % n`; a central placement table | Adding a 5th node moves 20.2% of keys, versus 80.2% with modulo hashing (measured on 20,000 keys). No table to keep consistent. |
| Replication | N = 3 copies, write quorum W = 2 | Wait for all 3 (W = 3); chain replication | W = 3 would make every write fail while any node is down. With W = 2, a write survives one slow or dead node, and the repair worker adds the third copy. Trade-off: for a few seconds, an object can have only 2 copies. |
| Read path | Read one copy, verify its checksum, try the next on failure | Read quorum (R = 2) | Blobs are immutable and checksummed, so one verified copy is proof of correctness. This halves read traffic compared with R = 2. |
| Metadata replication | Raft across 3 gateways | Single SQLite file (the first version); an external store such as DynamoDB; leaderless replication with last-write-wins | A single file was a single point of failure. An external store would hide the core problem the project set out to solve. Leaderless replication allows conflicting concurrent writes. Raft gives one ordered history that tolerates one gateway failure, and it's the industry standard (etcd, CockroachDB). |
| Consistent reads on the leader | Leader lease: serve reads only if a majority acknowledged within 225 ms | Send every read through the Raft log; ReadIndex (a heartbeat round per read) | The lease is shorter than the 300 ms minimum election timeout, so no other leader can exist while it's valid. Reads cost no extra network round trip. Trade-off: it depends on clocks not drifting much within 225 ms, which is safe on one machine and needs care across machines. |
| Crash between storing blobs and committing metadata | A sweeper deletes untracked copies older than 2 minutes | Two-phase commit across nodes | Two-phase commit adds a round trip to every write and has its own failure modes. Orphans are rare (only when a leader dies mid-write) and harmless until cleaned up. |
| Failure detection | Health check every 1 s; a node is down after 2 misses in a row | Mark down on the first miss; gossip | One miss caused false alarms ("flapping") that triggered needless re-copying. 2 misses adds about 1 s to detection but removes false alarms. |

## How failures play out

- **A storage node crashes:** reads skip it immediately (it's unreachable, so the next copy is tried). Within about 2 s the leader marks it down, and the repair worker copies its objects to the next nodes on the ring. When it rejoins, the extra copies are trimmed back to 3.
- **The leader gateway crashes:** followers stop hearing heartbeats, and after 300–600 ms (randomized to avoid split votes) one starts an election. A gateway only votes for a candidate whose log is at least as complete as its own, so the winner has every committed write. Clients get a `503` with a retry header and switch to another gateway.
- **Two of three gateways crash:** the survivor can't reach a majority, so it refuses writes rather than accepting changes that could conflict later. Availability is traded for consistency here, deliberately.

## Results

Measured with a chaos test: 4 storage nodes and 3 gateways on one MacBook Pro, 1,000 objects, 16 reading clients and 4 writing clients, with one process killed with `SIGKILL` mid-test.

| | Leader gateway killed | Storage node killed |
|---|---|---|
| Failed reads / writes | 0 / 0 | 0 / 0 |
| Recovery | new leader in 485 ms (338–985 ms across 7 runs) | back to 3 copies in 6 s |
| Objects intact (checksum verified) | 1,365 / 1,365 | 1,678 / 1,678 |
| Read latency p50 / p99 | 26 ms / 52 ms | 27 ms / 64 ms |

After recovery, all 3 gateways held identical metadata (the same objects, copies and log position), and the files on disk matched it exactly.

## What testing under failure taught me

The basic tests passed from early on. Killing processes under load exposed 8 bugs that ordinary tests didn't. The three most instructive:

1. **"Unreachable" is not "corrupt."** In the second before a crash was detected, reads got "connection refused" and the gateway treated it as a bad copy, which orphaned 106 good copies. Failure handling has to distinguish *I can't tell* from *I know it's wrong*.
2. **Races at leadership changes.** A new leader marked its health check done when the check *started*, so a write arriving in those milliseconds saw every node as down. State that becomes valid only after an async step must be marked valid *after* that step.
3. **Measure before optimizing.** Uploads ran at 48/s, and the obvious suspects were disk writes and Python itself. Profiling showed the HTTP client library was the bottleneck. Switching it gave 10× faster uploads.

All 8 are described in the [README](../README.md#bugs-the-chaos-tests-found).

## Risks and next steps

1. **Unbounded Raft log:** it grows with every change, and a new gateway replays it from the start. Next: periodic snapshots of the metadata database, then log truncation.
2. **Disruptive rejoin:** an isolated gateway keeps bumping its term, so when it reconnects it forces a needless election (seen in testing: term 57 after a minute alone). Next: Raft pre-vote.
3. **Fixed membership:** replacing a gateway requires a restart. Next: Raft's joint-consensus membership changes.
4. **Duplicate writes on retry:** if a leader dies after committing but before replying, the client's retry writes the object again. That's harmless for whole-object `PUT`s. Next: client-supplied idempotency keys.
5. **One machine:** all results come from one laptop, so network partitions and clock drift weren't tested. Next: deploy across EC2 instances (Free Tier) and inject network delays and partitions.
6. **Objects in memory:** the 64 MB limit exists because objects are buffered whole. Next: streaming and multipart upload.
