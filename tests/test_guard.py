"""Guard rules, checked against hand-built cluster snapshots (no processes needed)."""

from mini_s3 import guard


def view(alive="abcd", live_gateways=("g1", "g2", "g3"), placements=None):
    return {"nodes": list("abcd"), "gateways": ["g1", "g2", "g3"], "alive_nodes": set(alive),
            "live_gateways": set(live_gateways), "write_quorum": 2,
            "placements": placements if placements is not None else [
                {"key": "b/x", "nodes": ["a", "b", "c"]}, {"key": "b/y", "nodes": ["b", "c", "d"]}]}


def test_read_only_targets_and_unknown_names_pass():
    assert guard.check("restart", "zz", view())["allow"]


def test_wipe_denied_while_the_node_holds_tracked_copies():
    # Even with every other node up, the metadata would keep counting copies that no longer exist.
    v = guard.check("wipe_node", "c", view())
    assert not v["allow"] and "2 copies" in v["reason"]
    assert not guard.check("wipe_node", "c", view(alive="abd"))["allow"]  # also when c is the one down


def test_wipe_allowed_for_a_node_with_no_tracked_copies():
    assert guard.check("wipe_node", "d", view(placements=[{"key": "b/x", "nodes": ["a", "b", "c"]}]))["allow"]


def test_wipe_never_applies_to_gateways():
    assert not guard.check("wipe_node", "g1", view())["allow"]


def test_restarting_a_dead_node_is_always_fine():
    assert guard.check("restart", "b", view(alive="acd"))["allow"]


def test_kill_denied_when_it_strands_objects():
    assert not guard.check("kill", "c", view(alive="acd"))["allow"]
    assert guard.check("kill", "c", view())["allow"]


def test_gateway_majority_is_protected():
    assert guard.check("restart", "g1", view())["allow"]
    v = guard.check("kill", "g1", view(live_gateways=("g1", "g2")))
    assert not v["allow"] and "fewer than 2 of 3" in v["reason"]
    assert guard.check("restart", "g3", view(live_gateways=("g1", "g2")))["allow"]  # g3 is down already
