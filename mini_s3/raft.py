"""Raft consensus for gateway metadata (leader election + log replication).

Follows the Raft paper (Ongaro & Ousterhout, "In Search of an Understandable Consensus
Algorithm") without log compaction or membership changes:

- Every gateway starts as a follower. If it hears nothing from a leader for a random
  election timeout, it becomes a candidate, bumps the term, and asks for votes.
- A candidate with votes from a majority becomes leader. Votes go only to candidates
  whose log is at least as up to date, so a new leader always has every committed entry.
- The leader appends commands to its log and replicates them with AppendEntries. An entry
  is committed once a majority stores it, then every gateway applies it to its metadata.
- The leader only counts replicas for entries from its own term (it commits a no-op on
  election), and only serves reads while a majority has acknowledged it recently (a lease).
"""

import asyncio
import random
import time

import aiohttp

ELECTION_MIN = 0.3   # seconds without a heartbeat before a follower starts an election
ELECTION_MAX = 0.6
HEARTBEAT = 0.05     # how often the leader contacts each follower
RPC_TIMEOUT = 0.25
# A leader may serve reads alone for this long after a majority acknowledged it. Must be
# shorter than ELECTION_MIN so no other leader can have been elected in the meantime.
LEASE = ELECTION_MIN * 0.75
BATCH = 500          # max log entries per AppendEntries


class NotLeader(Exception):
    """This gateway can't handle the request right now; the client should retry."""


class Raft:
    def __init__(self, me, peers, store, http, on_leader=None):
        self.me = me
        self.peers = peers  # {gateway id: url}, not including this gateway
        self.store = store
        self.http = http
        self.on_leader = on_leader
        self.term = int(store.raft_get("term", 0))
        self.voted_for = store.raft_get("voted_for")
        self.last_applied = int(store.raft_get("last_applied", 0))
        self.commit_index = self.last_applied
        self.role = "follower"
        self.leader_id = None
        self.term_start_index = 0  # index of the no-op this leader committed on election
        self.next_index, self.match_index, self.last_ack = {}, {}, {}
        self.waiters = {}  # log index -> (term, future) for proposals awaiting commit
        self.wake = {p: asyncio.Event() for p in peers}
        self.elections = 0
        self._reset_timer()

    @property
    def majority(self):
        return (len(self.peers) + 1) // 2 + 1

    def _reset_timer(self):
        self.last_heard = time.monotonic()
        self.timeout = random.uniform(ELECTION_MIN, ELECTION_MAX)

    def _persist(self):
        self.store.raft_set(term=self.term, voted_for=self.voted_for)

    def start(self):
        return [asyncio.create_task(self._election_loop())] + \
               [asyncio.create_task(self._replicate_loop(p)) for p in self.peers]

    # ----- elections -----

    async def _election_loop(self):
        while True:
            await asyncio.sleep(0.02)
            if self.role != "leader" and time.monotonic() - self.last_heard > self.timeout:
                await self._run_election()

    async def _run_election(self):
        self.term += 1
        self.role, self.voted_for, self.leader_id = "candidate", self.me, None
        self._persist()
        self._reset_timer()
        self.elections += 1
        term = self.term
        last_index, last_term = self.store.log_last()
        votes = 1
        if votes >= self.majority:  # single-gateway cluster
            return self._become_leader()

        req = {"term": term, "candidate": self.me, "last_index": last_index, "last_term": last_term}
        for next_reply in asyncio.as_completed([self._rpc(p, "/raft/vote", req) for p in self.peers]):
            resp = await next_reply
            if self.role != "candidate" or self.term != term:
                return  # someone else won, or we moved on to a newer term
            if not resp:
                continue
            if resp["term"] > self.term:
                return self._step_down(resp["term"])
            if resp["granted"]:
                votes += 1
                if votes >= self.majority:
                    return self._become_leader()

    def _become_leader(self):
        self.role, self.leader_id = "leader", self.me
        last_index = self.store.log_last()[0]
        self.next_index = {p: last_index + 1 for p in self.peers}
        self.match_index = {p: 0 for p in self.peers}
        self.last_ack = {p: 0.0 for p in self.peers}
        # Committing an entry from our own term also commits everything before it,
        # so the leader's applied state is complete before it serves reads.
        self.term_start_index = self._append({"op": "noop"})
        self._advance_commit()
        print(f"[raft {self.me}] leader for term {self.term}")
        if self.on_leader:
            self.on_leader()

    def _step_down(self, term, leader=None):
        if term > self.term:
            self.term, self.voted_for = term, None
            self._persist()
        was_leader = self.role == "leader"
        self.role, self.leader_id = "follower", leader
        if was_leader:
            for _, fut in self.waiters.values():
                if not fut.done():
                    fut.set_exception(NotLeader("Lost leadership before the change committed; retry"))
            self.waiters.clear()

    # ----- the leader's side of replication -----

    def _append(self, cmd):
        index = self.store.log_last()[0] + 1
        self.store.log_append([{"index": index, "term": self.term, "cmd": cmd}])
        for e in self.wake.values():
            e.set()
        return index

    async def propose(self, cmd, timeout=5.0):
        """Replicate a command and return its result once it's committed and applied."""
        if self.role != "leader":
            raise NotLeader("Not the leader")
        index = self._append(cmd)
        fut = asyncio.get_running_loop().create_future()
        self.waiters[index] = (self.term, fut)
        self._advance_commit()
        try:
            return await asyncio.wait_for(fut, timeout)
        except asyncio.TimeoutError:
            self.waiters.pop(index, None)
            raise NotLeader("Couldn't reach a majority of gateways in time; retry")

    async def _replicate_loop(self, peer):
        while True:
            try:
                await asyncio.wait_for(self.wake[peer].wait(), HEARTBEAT)
            except asyncio.TimeoutError:
                pass
            self.wake[peer].clear()
            if self.role == "leader":
                try:
                    await self._send_append(peer)
                except Exception as e:  # never let one bad round kill the loop
                    print(f"[raft {self.me}] replication to {peer} failed: {e!r}")

    async def _send_append(self, peer):
        term = self.term
        next_index = self.next_index[peer]
        prev_index = next_index - 1
        prev_term = self.store.log_term(prev_index)
        if prev_term is None:  # shouldn't happen; restart from our log's end
            self.next_index[peer] = self.store.log_last()[0] + 1
            return
        entries = self.store.log_entries(next_index, BATCH)
        req = {"term": term, "leader": self.me, "prev_index": prev_index, "prev_term": prev_term,
               "entries": entries, "commit": self.commit_index}
        sent_at = time.monotonic()
        resp = await self._rpc(peer, "/raft/append", req, timeout=RPC_TIMEOUT if not entries else 2.0)
        if not resp:
            return
        if resp["term"] > self.term:
            return self._step_down(resp["term"])
        if self.role != "leader" or self.term != term:
            return
        self.last_ack[peer] = sent_at
        if resp["success"]:
            self.match_index[peer] = max(self.match_index[peer], prev_index + len(entries))
            self.next_index[peer] = self.match_index[peer] + 1
            self._advance_commit()
            if len(entries) == BATCH:
                self.wake[peer].set()  # more to send; don't wait for the next heartbeat
        else:
            # Back up to just past the follower's last entry (or one step) and retry now.
            self.next_index[peer] = max(1, min(next_index - 1, resp.get("last_index", 0) + 1))
            self.wake[peer].set()

    def _advance_commit(self):
        if self.role != "leader":
            return
        last_index = self.store.log_last()[0]
        for n in range(last_index, self.commit_index, -1):
            if self.store.log_term(n) != self.term:
                break  # Raft only commits entries from the current term by counting replicas
            if 1 + sum(m >= n for m in self.match_index.values()) >= self.majority:
                self.commit_index = n
                break
        self._apply_committed()

    def _apply_committed(self):
        while self.last_applied < self.commit_index:
            for e in self.store.log_entries(self.last_applied + 1, BATCH):
                if e["index"] > self.commit_index:
                    break
                try:
                    result, error = self.store.apply(e["index"], e["cmd"]), None
                except Exception as exc:  # deterministic: every gateway fails the same way
                    self.store.raft_set(last_applied=e["index"])
                    result, error = None, exc
                self.last_applied = e["index"]
                waiter = self.waiters.pop(e["index"], None)
                if waiter and not waiter[1].done():
                    term, fut = waiter
                    if term != e["term"]:
                        fut.set_exception(NotLeader("The change was replaced by a newer leader; retry"))
                    elif error:
                        fut.set_exception(error)
                    else:
                        fut.set_result(result)

    # ----- reads -----

    def lease_valid(self):
        if self.role != "leader" or self.commit_index < self.term_start_index:
            return False
        now = time.monotonic()
        return 1 + sum(now - t < LEASE for t in self.last_ack.values()) >= self.majority

    async def confirm_leadership(self, wait=0.5):
        """True once this gateway is certainly the current leader with up-to-date state."""
        deadline = time.monotonic() + wait
        while self.role == "leader":
            if self.lease_valid():
                return True
            if time.monotonic() > deadline:
                return False
            for e in self.wake.values():
                e.set()
            await asyncio.sleep(HEARTBEAT / 2)
        return False

    # ----- the follower's side (RPC handlers) -----

    def handle_vote(self, req):
        if req["term"] > self.term:
            self._step_down(req["term"])
        last_index, last_term = self.store.log_last()
        up_to_date = (req["last_term"], req["last_index"]) >= (last_term, last_index)
        grant = (req["term"] == self.term and self.voted_for in (None, req["candidate"]) and up_to_date)
        if grant:
            self.voted_for = req["candidate"]
            self._persist()
            self._reset_timer()
        return {"term": self.term, "granted": grant}

    def handle_append(self, req):
        if req["term"] < self.term:
            return {"term": self.term, "success": False}
        if req["term"] > self.term or self.role != "follower":
            self._step_down(req["term"], req["leader"])
        self.leader_id = req["leader"]
        self._reset_timer()

        local_prev_term = self.store.log_term(req["prev_index"])
        if local_prev_term != req["prev_term"]:
            last_index = self.store.log_last()[0]
            hint = last_index if local_prev_term is None else min(last_index, req["prev_index"] - 1)
            return {"term": self.term, "success": False, "last_index": hint}

        new, truncate_from = [], None
        for i, e in enumerate(req["entries"]):
            existing = self.store.log_term(e["index"])
            if existing is None:
                new = req["entries"][i:]
                break
            if existing != e["term"]:  # conflict: drop our entry and everything after it
                new, truncate_from = req["entries"][i:], e["index"]
                break
        if new:
            self.store.log_append(new, truncate_from=truncate_from)

        last_new = req["prev_index"] + len(req["entries"])
        if req["commit"] > self.commit_index:
            self.commit_index = min(req["commit"], last_new)
            self._apply_committed()
        return {"term": self.term, "success": True}

    # ----- plumbing -----

    async def _rpc(self, peer, path, body, timeout=RPC_TIMEOUT):
        try:
            async with self.http.post(f"{self.peers[peer]}{path}", json=body,
                                      timeout=aiohttp.ClientTimeout(total=timeout)) as r:
                return await r.json() if r.status == 200 else None
        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError):
            return None

    def status(self):
        now = time.monotonic()
        return {
            "id": self.me, "role": self.role, "term": self.term, "leader": self.leader_id,
            "commit_index": self.commit_index, "last_applied": self.last_applied,
            "last_index": self.store.log_last()[0], "elections_started": self.elections,
            "peers": {p: {"url": self.peers[p], "match_index": self.match_index.get(p, 0),
                          "acked_ago": round(now - self.last_ack[p], 2) if self.last_ack.get(p) else None}
                      for p in self.peers},
        }
