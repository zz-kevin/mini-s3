"""Unit tests for the metadata state machine (the commands Raft replicates)."""

import pytest

from mini_s3.metadata import Metadata


@pytest.fixture
def m(tmp_path):
    return Metadata(str(tmp_path / "meta.db"))


def put(m, index, key, blob, nodes=("a", "b", "c")):
    m.apply(index, {"op": "put", "bucket": "b1", "key": key, "blob_id": blob, "size": 1, "checksum": "x",
                    "content_type": "text/plain", "updated_at": "2026-01-01 00:00:00", "nodes": list(nodes)})


def test_overwrite_queues_old_copies_for_deletion(m):
    put(m, 1, "k", "old")
    put(m, 2, "k", "new")
    assert m.get("b1", "k")["blob_id"] == "new"
    assert sorted(g["node_id"] for g in m.garbage()) == ["a", "b", "c"]


def test_delete_queues_copies_and_reports_existence(m):
    put(m, 1, "k", "blob")
    assert m.apply(2, {"op": "delete", "bucket": "b1", "key": "k"}) is True
    assert m.apply(3, {"op": "delete", "bucket": "b1", "key": "k"}) is False
    assert m.get("b1", "k") is None and len(m.garbage()) == 3


def test_late_copy_of_replaced_blob_goes_to_garbage(m):
    """Regression: a repair copy that finished after its object was overwritten was
    tracked as a live replica forever, so its file was never deleted."""
    put(m, 1, "k", "old")
    put(m, 2, "k", "new")
    m.apply(3, {"op": "add_replica", "blob_id": "old", "node": "d"})
    assert m.stray_replicas() == []
    assert {"blob_id": "old", "node_id": "d"} in m.garbage()


def test_add_replica_cancels_pending_delete_of_same_copy(m):
    put(m, 1, "k", "blob", nodes=("a", "b", "c"))
    m.apply(2, {"op": "retire_replica", "blob_id": "blob", "node": "c"})
    m.apply(3, {"op": "add_replica", "blob_id": "blob", "node": "c"})
    assert "c" in m.get("b1", "k")["replicas"] and m.garbage() == []


def test_last_applied_is_recorded_with_each_command(m):
    put(m, 7, "k", "blob")
    assert m.raft_get("last_applied") == "7"


def test_log_append_truncates_conflicting_suffix(m):
    m.log_append([{"index": i, "term": 1, "cmd": {"op": "noop"}} for i in (1, 2, 3)])
    m.log_append([{"index": 2, "term": 2, "cmd": {"op": "noop"}}], truncate_from=2)
    assert m.log_last() == (2, 2) and m.log_term(3) is None
