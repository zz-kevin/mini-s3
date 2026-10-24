"""Metadata store (SQLite): which blob backs each bucket/key and which nodes hold it,
plus the Raft log that every change goes through.

Every change is a *command* (a small dict) applied by `apply()`. Gateways replicate
the commands with Raft and apply them in the same order, so every gateway's tables
end up identical. Commands must therefore be deterministic: anything like a timestamp
is decided once by the leader and carried inside the command.
"""

import json
import sqlite3
import threading

SCHEMA = """
CREATE TABLE IF NOT EXISTS objects (
    bucket TEXT NOT NULL,
    key TEXT NOT NULL,
    blob_id TEXT NOT NULL,
    size INTEGER NOT NULL,
    checksum TEXT NOT NULL,
    content_type TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (bucket, key)
);
CREATE TABLE IF NOT EXISTS replicas (
    blob_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    PRIMARY KEY (blob_id, node_id)
);
-- Blobs that should be deleted from a node but couldn't be yet (node was down).
CREATE TABLE IF NOT EXISTS garbage (
    blob_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    PRIMARY KEY (blob_id, node_id)
);
-- Raft: the replicated command log, plus term/vote/last-applied bookkeeping.
CREATE TABLE IF NOT EXISTS raft_log (
    idx INTEGER PRIMARY KEY,
    term INTEGER NOT NULL,
    cmd TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS raft_state (
    k TEXT PRIMARY KEY,
    v TEXT
);
"""


class Metadata:
    def __init__(self, path):
        self._conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.executescript(SCHEMA)
        self._lock = threading.Lock()

    def _q(self, sql, args=()):
        with self._lock:
            return [dict(r) for r in self._conn.execute(sql, args).fetchall()]

    # ----- reads -----

    def get(self, bucket, key):
        rows = self._q("SELECT * FROM objects WHERE bucket = ? AND key = ?", (bucket, key))
        if not rows:
            return None
        obj = rows[0]
        obj["replicas"] = [r["node_id"] for r in self._q(
            "SELECT node_id FROM replicas WHERE blob_id = ? ORDER BY node_id", (obj["blob_id"],))]
        return obj

    def list(self, bucket, prefix="", limit=1000):
        return self._q(
            """SELECT key, size, checksum, content_type, updated_at FROM objects
               WHERE bucket = ? AND key >= ? AND key < ? ORDER BY key LIMIT ?""",
            (bucket, prefix, prefix + "￿", limit),
        )

    def placements(self):
        """Every live object's blob, checksum, and replica nodes (for the repair worker)."""
        rows = self._q(
            """SELECT o.bucket, o.key, o.blob_id, o.checksum, r.node_id
               FROM objects o LEFT JOIN replicas r ON r.blob_id = o.blob_id"""
        )
        out = {}
        for r in rows:
            p = out.setdefault(r["blob_id"], {"bucket": r["bucket"], "key": r["key"],
                                              "checksum": r["checksum"], "nodes": []})
            if r["node_id"]:
                p["nodes"].append(r["node_id"])
        return out

    def stray_replicas(self, limit=500):
        """Copies tracked for a blob that no object points to any more."""
        return self._q("""SELECT r.blob_id, r.node_id FROM replicas r
                          LEFT JOIN objects o ON o.blob_id = r.blob_id
                          WHERE o.blob_id IS NULL LIMIT ?""", (limit,))

    def garbage(self, limit=500):
        return self._q("SELECT blob_id, node_id FROM garbage LIMIT ?", (limit,))

    def known_blobs(self, node_id):
        """Blob ids the metadata says should exist on a node (tracked or pending delete)."""
        rows = self._q("SELECT blob_id FROM replicas WHERE node_id = ? "
                       "UNION SELECT blob_id FROM garbage WHERE node_id = ?", (node_id, node_id))
        return {r["blob_id"] for r in rows}

    def counts(self):
        return {
            "objects": self._q("SELECT COUNT(*) AS n FROM objects")[0]["n"],
            "bytes": self._q("SELECT COALESCE(SUM(size), 0) AS n FROM objects")[0]["n"],
            "pending_deletes": self._q("SELECT COUNT(*) AS n FROM garbage")[0]["n"],
        }

    # ----- applying commands (the replicated state machine) -----

    def apply(self, index, cmd):
        """Apply one committed log entry and record it as applied, atomically."""
        with self._lock:
            c = self._conn
            c.execute("BEGIN IMMEDIATE")
            try:
                result = self._apply(c, cmd)
                c.execute("INSERT OR REPLACE INTO raft_state(k, v) VALUES ('last_applied', ?)", (str(index),))
                c.execute("COMMIT")
                return result
            except Exception:
                c.execute("ROLLBACK")
                raise

    def _apply(self, c, cmd):
        op = cmd["op"]
        if op == "batch":
            return [self._apply(c, sub) for sub in cmd["cmds"]]
        if op == "noop":
            return None
        if op == "put":
            old = c.execute("SELECT blob_id FROM objects WHERE bucket = ? AND key = ?",
                            (cmd["bucket"], cmd["key"])).fetchone()
            c.execute(
                """INSERT INTO objects(bucket, key, blob_id, size, checksum, content_type, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(bucket, key) DO UPDATE SET blob_id = excluded.blob_id,
                     size = excluded.size, checksum = excluded.checksum,
                     content_type = excluded.content_type, updated_at = excluded.updated_at""",
                (cmd["bucket"], cmd["key"], cmd["blob_id"], cmd["size"], cmd["checksum"],
                 cmd["content_type"], cmd["updated_at"]),
            )
            c.executemany("INSERT OR IGNORE INTO replicas(blob_id, node_id) VALUES (?, ?)",
                          [(cmd["blob_id"], n) for n in cmd["nodes"]])
            if old and old["blob_id"] != cmd["blob_id"]:
                self._retire_blob(c, old["blob_id"])
            return None
        if op == "delete":
            row = c.execute("SELECT blob_id FROM objects WHERE bucket = ? AND key = ?",
                            (cmd["bucket"], cmd["key"])).fetchone()
            if row:
                c.execute("DELETE FROM objects WHERE bucket = ? AND key = ?", (cmd["bucket"], cmd["key"]))
                self._retire_blob(c, row["blob_id"])
            return bool(row)
        if op == "add_replica":
            current = c.execute("SELECT 1 FROM objects WHERE blob_id = ?", (cmd["blob_id"],)).fetchone()
            if not current:
                # The object was overwritten or deleted while this copy was being made:
                # the new copy is already garbage, so queue it for deletion instead.
                c.execute("INSERT OR IGNORE INTO garbage(blob_id, node_id) VALUES (?, ?)", (cmd["blob_id"], cmd["node"]))
                return None
            c.execute("INSERT OR IGNORE INTO replicas(blob_id, node_id) VALUES (?, ?)", (cmd["blob_id"], cmd["node"]))
            # A pending delete for this exact copy would now destroy good data; cancel it.
            c.execute("DELETE FROM garbage WHERE blob_id = ? AND node_id = ?", (cmd["blob_id"], cmd["node"]))
            return None
        if op == "retire_replica":
            c.execute("DELETE FROM replicas WHERE blob_id = ? AND node_id = ?", (cmd["blob_id"], cmd["node"]))
            c.execute("INSERT OR IGNORE INTO garbage(blob_id, node_id) VALUES (?, ?)", (cmd["blob_id"], cmd["node"]))
            return None
        if op == "clear_garbage":
            c.execute("DELETE FROM garbage WHERE blob_id = ? AND node_id = ?", (cmd["blob_id"], cmd["node"]))
            return None
        raise ValueError(f"Unknown command {op!r}")

    @staticmethod
    def _retire_blob(c, blob_id):
        """Move all of a blob's replicas to the garbage table."""
        c.execute("INSERT OR IGNORE INTO garbage(blob_id, node_id) "
                  "SELECT blob_id, node_id FROM replicas WHERE blob_id = ?", (blob_id,))
        c.execute("DELETE FROM replicas WHERE blob_id = ?", (blob_id,))

    # ----- Raft persistence -----

    def raft_get(self, k, default=None):
        rows = self._q("SELECT v FROM raft_state WHERE k = ?", (k,))
        return rows[0]["v"] if rows else default

    def raft_set(self, **values):
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            for k, v in values.items():
                self._conn.execute("INSERT OR REPLACE INTO raft_state(k, v) VALUES (?, ?)",
                                   (k, None if v is None else str(v)))
            self._conn.execute("COMMIT")

    def log_last(self):
        """(index, term) of the last log entry, or (0, 0) for an empty log."""
        rows = self._q("SELECT idx, term FROM raft_log ORDER BY idx DESC LIMIT 1")
        return (rows[0]["idx"], rows[0]["term"]) if rows else (0, 0)

    def log_term(self, index):
        if index == 0:
            return 0
        rows = self._q("SELECT term FROM raft_log WHERE idx = ?", (index,))
        return rows[0]["term"] if rows else None

    def log_entries(self, start, limit=500):
        return [{"index": r["idx"], "term": r["term"], "cmd": json.loads(r["cmd"])}
                for r in self._q("SELECT * FROM raft_log WHERE idx >= ? ORDER BY idx LIMIT ?", (start, limit))]

    def log_append(self, entries, truncate_from=None):
        """Append entries (each {"index","term","cmd"}), first dropping any entries
        at or after `truncate_from` (a follower discarding a conflicting suffix)."""
        with self._lock:
            c = self._conn
            c.execute("BEGIN IMMEDIATE")
            if truncate_from is not None:
                c.execute("DELETE FROM raft_log WHERE idx >= ?", (truncate_from,))
            c.executemany("INSERT INTO raft_log(idx, term, cmd) VALUES (?, ?, ?)",
                          [(e["index"], e["term"], json.dumps(e["cmd"])) for e in entries])
            c.execute("COMMIT")
