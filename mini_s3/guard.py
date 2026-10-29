"""Safety guard for the on-call agent: blocks actions that could lose data or drop
the cluster below a quorum, before they run.

`check()` is a pure function of a cluster snapshot (see `Ops.view()`), so the rules
can be tested without starting any processes.
"""


def _at_risk(view, without):
    """Objects that would have fewer than a write quorum of live copies if `without` went away."""
    alive = view["alive_nodes"] - {without}
    return [p["key"] for p in view["placements"]
            if without in p["nodes"] and len([n for n in p["nodes"] if n in alive]) < view["write_quorum"]]


def _deny(reason):
    return {"allow": False, "reason": reason}


ALLOW = {"allow": True, "reason": ""}


def check(action, target, view):
    """Decide whether one action may run. Returns {"allow": bool, "reason": str}."""
    nodes, gateways = view["nodes"], view["gateways"]
    if target not in nodes and target not in gateways:
        return ALLOW  # the tool itself reports the unknown name

    if action == "wipe_node":
        if target in gateways:
            return _deny("wipe_node only applies to storage nodes")
        # A wipe is permanent, so judge it as if the node were gone for good, even if it's down now.
        risky = [p["key"] for p in view["placements"] if target in p["nodes"]
                 and len([n for n in p["nodes"] if n != target and n in view["alive_nodes"]]) < view["write_quorum"]]
        if risky:
            return _deny(f"wiping {target} would leave {len(risky)} objects with fewer than "
                         f"{view['write_quorum']} live copies (e.g. {risky[0]})")
        return ALLOW

    if action in ("kill", "restart") and target in gateways and target in view["live_gateways"]:
        majority = len(gateways) // 2 + 1
        if len(view["live_gateways"]) - 1 < majority:
            return _deny(f"taking {target} down would leave fewer than {majority} of {len(gateways)} "
                         "gateways up, so no leader could be elected")

    # A restart brings the node straight back, so only an outright kill can strand data.
    if action == "kill" and target in nodes and target in view["alive_nodes"]:
        risky = _at_risk(view, target)
        if risky:
            return _deny(f"killing {target} would leave {len(risky)} objects with fewer than "
                         f"{view['write_quorum']} live copies (e.g. {risky[0]})")
    return ALLOW
