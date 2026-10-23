"""Metadata store (SQLite): which blob backs each bucket/key, and which nodes hold it."""

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
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
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

    def get(self, bucket, key):
        rows = self._q("SELECT * FROM objects WHERE bucket = ? AND key = ?", (bucket, key))
        if not rows:
            return None
        obj = rows[0]
        obj["replicas"] = [r["node_id"] for r in self._q(
            "SELECT node_id FROM replicas WHERE blob_id = ? ORDER BY node_id", (obj["blob_id"],))]
        return obj

    def put(self, bucket, key, blob_id, size, checksum, content_type, nodes):
        """Point bucket/key at a new blob. Returns the blob it replaced, if any."""
        with self._lock:
            c = self._conn
            c.execute("BEGIN IMMEDIATE")
            try:
                old = c.execute("SELECT blob_id FROM objects WHERE bucket = ? AND key = ?",
                                (bucket, key)).fetchone()
                c.execute(
                    """INSERT INTO objects(bucket, key, blob_id, size, checksum, content_type)
                       VALUES (?, ?, ?, ?, ?, ?)
                       ON CONFLICT(bucket, key) DO UPDATE SET blob_id = excluded.blob_id,
                         size = excluded.size, checksum = excluded.checksum,
                         content_type = excluded.content_type, updated_at = CURRENT_TIMESTAMP""",
                    (bucket, key, blob_id, size, checksum, content_type),
                )
                c.executemany("INSERT OR IGNORE INTO replicas(blob_id, node_id) VALUES (?, ?)",
                              [(blob_id, n) for n in nodes])
                old_blob = old["blob_id"] if old else None
                if old_blob:
                    self._retire(old_blob)
                c.execute("COMMIT")
                return old_blob
            except Exception:
                c.execute("ROLLBACK")
                raise

    def _retire(self, blob_id):
        """Move a blob's replicas to the garbage table (caller holds the lock)."""
        self._conn.execute(
            "INSERT OR IGNORE INTO garbage(blob_id, node_id) SELECT blob_id, node_id FROM replicas WHERE blob_id = ?",
            (blob_id,))
        self._conn.execute("DELETE FROM replicas WHERE blob_id = ?", (blob_id,))

    def delete(self, bucket, key):
        with self._lock:
            c = self._conn
            c.execute("BEGIN IMMEDIATE")
            row = c.execute("SELECT blob_id FROM objects WHERE bucket = ? AND key = ?",
                            (bucket, key)).fetchone()
            if row:
                c.execute("DELETE FROM objects WHERE bucket = ? AND key = ?", (bucket, key))
                self._retire(row["blob_id"])
            c.execute("COMMIT")
            return bool(row)

    def list(self, bucket, prefix="", limit=1000):
        return self._q(
            """SELECT key, size, checksum, content_type, updated_at FROM objects
               WHERE bucket = ? AND key >= ? AND key < ? ORDER BY key LIMIT ?""",
            (bucket, prefix, prefix + "￿", limit),
        )

    def add_replica(self, blob_id, node_id):
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            self._conn.execute("INSERT OR IGNORE INTO replicas(blob_id, node_id) VALUES (?, ?)", (blob_id, node_id))
            # A pending delete for this exact copy would now destroy good data; cancel it.
            self._conn.execute("DELETE FROM garbage WHERE blob_id = ? AND node_id = ?", (blob_id, node_id))
            self._conn.execute("COMMIT")

    def retire_replica(self, blob_id, node_id):
        """Stop tracking one copy and queue its file for deletion."""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            self._conn.execute("DELETE FROM replicas WHERE blob_id = ? AND node_id = ?", (blob_id, node_id))
            self._conn.execute("INSERT OR IGNORE INTO garbage(blob_id, node_id) VALUES (?, ?)", (blob_id, node_id))
            self._conn.execute("COMMIT")

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

    def garbage(self, limit=500):
        return self._q("SELECT blob_id, node_id FROM garbage LIMIT ?", (limit,))

    def clear_garbage(self, blob_id, node_id):
        self._q("DELETE FROM garbage WHERE blob_id = ? AND node_id = ?", (blob_id, node_id))

    def counts(self):
        return {
            "objects": self._q("SELECT COUNT(*) AS n FROM objects")[0]["n"],
            "bytes": self._q("SELECT COALESCE(SUM(size), 0) AS n FROM objects")[0]["n"],
            "pending_deletes": self._q("SELECT COUNT(*) AS n FROM garbage")[0]["n"],
        }
