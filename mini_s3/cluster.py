"""Start and stop a local cluster (storage nodes + gateway) as background processes."""

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


def _start_node(state, node_id):
    node = state["nodes"][node_id]
    node["pid"] = _spawn(f"node-{node_id}", "mini_s3.node:app", node["port"],
                         {"NODE_ID": node_id, "DATA_DIR": os.path.join(ROOT, "data", node_id)})


def up(n_nodes=4, replicas=3, write_quorum=2):
    state = _load()
    if state and _running(state["gateway"]["pid"]):
        print(f"Cluster already running: http://127.0.0.1:{state['gateway']['port']}")
        return
    if not 2 <= n_nodes <= 26:
        sys.exit("Use between 2 and 26 nodes.")
    if replicas > n_nodes or write_quorum > replicas:
        sys.exit("Need write_quorum <= replicas <= nodes.")
    busy = [p for p in [GATEWAY_PORT] + [FIRST_NODE_PORT + i for i in range(n_nodes)] if _port_in_use(p)]
    if busy:
        sys.exit(f"Port(s) {', '.join(map(str, busy))} already in use by another program. "
                 "Stop it, or pick another gateway port with MS3_PORT=7481 ./ms3 cluster up")
    os.makedirs(STATE_DIR, exist_ok=True)
    for name in os.listdir(STATE_DIR):  # fresh logs for each cluster run
        if name.endswith(".log"):
            os.remove(os.path.join(STATE_DIR, name))
    state = {"nodes": {}, "gateway": {"port": GATEWAY_PORT}}
    for i in range(n_nodes):
        state["nodes"][NODE_IDS[i]] = {"port": FIRST_NODE_PORT + i}
    for node_id in state["nodes"]:
        _start_node(state, node_id)
    for node_id, node in state["nodes"].items():
        if not _wait_http(f"http://127.0.0.1:{node['port']}/health"):
            sys.exit(f"Node {node_id} didn't start; see .cluster/node-{node_id}.log")

    nodes_env = ",".join(f"{n}=http://127.0.0.1:{v['port']}" for n, v in state["nodes"].items())
    state["gateway"]["pid"] = _spawn("gateway", "mini_s3.gateway:app", GATEWAY_PORT, {
        "NODES": nodes_env, "REPLICAS": str(replicas), "WRITE_QUORUM": str(write_quorum),
        "META_DB": os.path.join(ROOT, "data", "meta.db"),
    })
    _save(state)
    if not _wait_http(f"http://127.0.0.1:{GATEWAY_PORT}/_cluster"):
        sys.exit("Gateway didn't start; see .cluster/gateway.log")
    print(f"Started {n_nodes} storage nodes ({', '.join(state['nodes'])}) "
          f"with {replicas} replicas, write quorum {write_quorum}.")
    print(f"Gateway:   http://127.0.0.1:{GATEWAY_PORT}")
    print(f"Dashboard: http://127.0.0.1:{GATEWAY_PORT}/  (live node and replication status)")


def down():
    state = _load()
    if not state:
        print("No cluster running.")
        return
    pids = [state["gateway"].get("pid")] + [n.get("pid") for n in state["nodes"].values()]
    for pid in pids:
        if _running(pid):
            os.killpg(pid, signal.SIGTERM)
    os.remove(STATE_FILE)
    print("Cluster stopped. Data is kept in data/; delete that folder to start empty.")


def kill_node(node_id):
    """Simulate a crash: SIGKILL, no graceful shutdown."""
    state = _require()
    node = state["nodes"].get(node_id)
    if not node:
        sys.exit(f"No node {node_id!r}. Nodes: {', '.join(state['nodes'])}")
    if not _running(node.get("pid")):
        print(f"Node {node_id} is already down.")
        return
    os.killpg(node["pid"], signal.SIGKILL)
    print(f"Killed node {node_id} (pid {node['pid']}).")


def start_node(node_id):
    state = _require()
    if node_id not in state["nodes"]:
        sys.exit(f"No node {node_id!r}. Nodes: {', '.join(state['nodes'])}")
    if _running(state["nodes"][node_id].get("pid")):
        print(f"Node {node_id} is already running.")
        return
    _start_node(state, node_id)
    _save(state)
    if _wait_http(f"http://127.0.0.1:{state['nodes'][node_id]['port']}/health"):
        print(f"Node {node_id} is back up.")
    else:
        sys.exit(f"Node {node_id} didn't start; see .cluster/node-{node_id}.log")


def _require():
    state = _load()
    if not state:
        sys.exit("No cluster running. Start one with: ./ms3 cluster up")
    return state
