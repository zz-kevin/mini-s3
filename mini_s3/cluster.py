"""Start and stop a local cluster (storage nodes + Raft gateways) as background processes."""

import json
import os
import signal
import socket
import subprocess
import sys
import time

import httpx

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
STATE_DIR = os.path.join(ROOT, ".cluster")
STATE_FILE = os.path.join(STATE_DIR, "state.json")
DATA_DIR = os.path.join(ROOT, "data")
# 7480 rather than the popular 8080, which Docker and dev servers often hold.
GATEWAY_PORT = int(os.environ.get("MS3_PORT", "7480"))
FIRST_NODE_PORT = 9001
NODE_IDS = "abcdefghijklmnopqrstuvwxyz"


def _load():
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except FileNotFoundError:
        return None


def _save(state):
    os.makedirs(STATE_DIR, exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


def _running(pid):
    try:
        os.kill(pid, 0)
        return True
    except (ProcessLookupError, TypeError):
        return False


def _spawn(name, module, port, env):
    log = open(os.path.join(STATE_DIR, f"{name}.log"), "ab")
    proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", module, "--host", "127.0.0.1", "--port", str(port),
         "--log-level", "warning"],
        cwd=ROOT, env={**os.environ, **env}, stdout=log, stderr=log, start_new_session=True,
    )
    return proc.pid


def _port_in_use(port):
    with socket.socket() as s:
        return s.connect_ex(("127.0.0.1", port)) == 0


def _wait_http(url, seconds=10):
    deadline = time.time() + seconds
    while time.time() < deadline:
        try:
            if httpx.get(url, timeout=0.5).status_code == 200:
                return True
        except httpx.HTTPError:
            pass
        time.sleep(0.2)
    return False


def gateway_urls():
    """Every gateway's address, for clients that fail over between them."""
    state = _load()
    if state and "gateways" in state:
        return [f"http://127.0.0.1:{g['port']}" for g in state["gateways"].values()]
    return [f"http://127.0.0.1:{GATEWAY_PORT + i}" for i in range(3)]


def find_leader(state=None):
    """Ask the gateways who the Raft leader is. Returns a gateway id or None."""
    state = state or _require()
    for g in state["gateways"].values():
        try:
            s = httpx.get(f"http://127.0.0.1:{g['port']}/_raft", timeout=0.5).json()
            if s["role"] == "leader":
                return s["id"]
        except (httpx.HTTPError, ValueError):
            continue
    return None


def _start_node(state, node_id):
    node = state["nodes"][node_id]
    node["pid"] = _spawn(f"node-{node_id}", "mini_s3.node:app", node["port"],
                         {"NODE_ID": node_id, "DATA_DIR": os.path.join(DATA_DIR, node_id)})


def _start_gateway(state, gw_id):
    g = state["gateways"][gw_id]
    peers = ",".join(f"{other}=http://127.0.0.1:{v['port']}"
                     for other, v in state["gateways"].items() if other != gw_id)
    g["pid"] = _spawn(f"gateway-{gw_id}", "mini_s3.gateway:app", g["port"], {
        "GATEWAY_ID": gw_id, "PEERS": peers, "NODES": state["nodes_env"],
        "REPLICAS": str(state["replicas"]), "WRITE_QUORUM": str(state["write_quorum"]),
        "META_DB": os.path.join(DATA_DIR, f"{gw_id}.db"),
    })


def _move_legacy_data():
    """Data from the old single-gateway version can't be read by Raft gateways."""
    if os.path.exists(os.path.join(DATA_DIR, "meta.db")):
        dest = f"{DATA_DIR}-single-gateway-{time.strftime('%Y%m%d-%H%M%S')}"
        os.rename(DATA_DIR, dest)
        print(f"Moved data from the older single-gateway version to {os.path.basename(dest)}/; starting empty.")


def up(n_nodes=4, replicas=3, write_quorum=2, n_gateways=3):
    state = _load()
    if state and any(_running(g.get("pid")) for g in state.get("gateways", {}).values()):
        print(f"Cluster already running: {', '.join(gateway_urls())}")
        return
    if not 2 <= n_nodes <= 26:
        sys.exit("Use between 2 and 26 nodes.")
    if replicas > n_nodes or write_quorum > replicas:
        sys.exit("Need write_quorum <= replicas <= nodes.")
    if n_gateways < 1 or n_gateways % 2 == 0:
        sys.exit("Use an odd number of gateways (1, 3, 5) so a majority is always well defined.")
    gw_ports = [GATEWAY_PORT + i for i in range(n_gateways)]
    busy = [p for p in gw_ports + [FIRST_NODE_PORT + i for i in range(n_nodes)] if _port_in_use(p)]
    if busy:
        sys.exit(f"Port(s) {', '.join(map(str, busy))} already in use by another program. "
                 "Stop it, or pick other gateway ports with MS3_PORT=7580 ./ms3 cluster up")
    _move_legacy_data()
    os.makedirs(STATE_DIR, exist_ok=True)
    for name in os.listdir(STATE_DIR):  # fresh logs for each cluster run
        if name.endswith(".log"):
            os.remove(os.path.join(STATE_DIR, name))

    state = {"nodes": {NODE_IDS[i]: {"port": FIRST_NODE_PORT + i} for i in range(n_nodes)},
             "gateways": {f"g{i + 1}": {"port": port} for i, port in enumerate(gw_ports)},
             "replicas": replicas, "write_quorum": write_quorum}
    state["nodes_env"] = ",".join(f"{n}=http://127.0.0.1:{v['port']}" for n, v in state["nodes"].items())
    for node_id in state["nodes"]:
        _start_node(state, node_id)
    for node_id, node in state["nodes"].items():
        if not _wait_http(f"http://127.0.0.1:{node['port']}/health"):
            sys.exit(f"Node {node_id} didn't start; see .cluster/node-{node_id}.log")
    for gw_id in state["gateways"]:
        _start_gateway(state, gw_id)
    _save(state)
    if not any(_wait_http(f"{url}/_cluster", seconds=10) for url in gateway_urls()):
        sys.exit("Gateways didn't elect a leader; see .cluster/gateway-*.log")

    print(f"Started {n_nodes} storage nodes ({', '.join(state['nodes'])}) with {replicas} replicas, "
          f"write quorum {write_quorum}.")
    print(f"Started {n_gateways} gateways ({', '.join(state['gateways'])}); leader: {find_leader(state)}.")
    print(f"Gateways:  {', '.join(gateway_urls())}  (use any of them)")
    print(f"Dashboard: http://127.0.0.1:{GATEWAY_PORT}/")


def down():
    state = _load()
    if not state:
        print("No cluster running.")
        return
    procs = list(state.get("gateways", {}).values()) + list(state["nodes"].values())
    if "gateway" in state:  # state file from the single-gateway version
        procs.append(state["gateway"])
    for p in procs:
        if _running(p.get("pid")):
            os.killpg(p["pid"], signal.SIGTERM)
    os.remove(STATE_FILE)
    print("Cluster stopped. Data is kept in data/; delete that folder to start empty.")


def _lookup(state, name):
    """Resolve a node id ("a"), a gateway id ("g1"), or "leader" to (kind, id, entry)."""
    if name == "leader":
        name = find_leader(state)
        if not name:
            sys.exit("No gateway is leader right now.")
    if name in state["nodes"]:
        return "node", name, state["nodes"][name]
    if name in state.get("gateways", {}):
        return "gateway", name, state["gateways"][name]
    sys.exit(f"No node or gateway {name!r}. Nodes: {', '.join(state['nodes'])}; "
             f"gateways: {', '.join(state.get('gateways', {}))}; or 'leader'.")


def kill(name):
    """Simulate a crash: SIGKILL, no graceful shutdown."""
    state = _require()
    kind, ident, entry = _lookup(state, name)
    label = f"{kind.capitalize()} {ident}"
    if not _running(entry.get("pid")):
        print(f"{label} is already down.")
        return ident
    os.killpg(entry["pid"], signal.SIGKILL)
    print(f"Killed {kind} {ident} (pid {entry['pid']}).")
    return ident


def start(name):
    state = _require()
    kind, ident, entry = _lookup(state, name)
    label = f"{kind.capitalize()} {ident}"
    if _running(entry.get("pid")):
        print(f"{label} is already running.")
        return
    if kind == "node":
        _start_node(state, ident)
        url = f"http://127.0.0.1:{entry['port']}/health"
    else:
        _start_gateway(state, ident)
        url = f"http://127.0.0.1:{entry['port']}/_raft"
    _save(state)
    if _wait_http(url):
        print(f"{label} is back up.")
    else:
        sys.exit(f"{label} didn't start; see .cluster/{kind}-{ident}.log")


def _require():
    state = _load()
    if not state:
        sys.exit("No cluster running. Start one with: ./ms3 cluster up")
    if "gateways" not in state:
        sys.exit("This cluster was started by an older version. Restart it: ./ms3 cluster down && ./ms3 cluster up")
    return state
