"""Guard rules, checked against hand-built cluster snapshots (no processes needed)."""

from mini_s3 import guard


def view(alive="abcd", live_gateways=("g1", "g2", "g3"), placements=None):
    return {"nodes": list("abcd"), "gateways": ["g1", "g2", "g3"], "alive_nodes": set(alive),
            "live_gateways": set(live_gateways), "write_quorum": 2,
            "placements": placements if placements is not None else [
                {"key": "b/x", "nodes": ["a", "b", "c"]}, {"key": "b/y", "nodes": ["b", "c", "d"]}]}


def test_read_only_targets_and_unknown_names_pass():
    assert guard.check("restart", "zz", view())["allow"]


def test_wipe_allowed_when_every_object_keeps_a_quorum_elsewhere():
    assert guard.check("wipe_node", "c", view())["allow"]


def test_wipe_denied_when_another_node_is_down():
    v = guard.check("wipe_node", "c", view(alive="acd"))
    assert not v["allow"] and "2 objects" in v["reason"]


def test_wipe_judges_a_down_node_as_gone_for_good():
    # c is already down, but wiping it is permanent; with b also down, b/y would have one copy.
    assert not guard.check("wipe_node", "c", view(alive="ad"))["allow"]


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
