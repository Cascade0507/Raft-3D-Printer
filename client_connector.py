import os
import json
from pathlib import Path

LOG_DIR = "logs"
STATUS_FILE = "node_status.json"
TOTAL_NODES = 8

def get_alive_nodes():
    alive_nodes = set()
    for log_file in Path(LOG_DIR).glob("raft_node_*.log"):
        with open(log_file, "r") as f:
            content = f.read()
            if "Starting Raft node on port" in content and "Running on http://" in content:
                node_id = int(log_file.name.split("_")[-1].split(".")[0])
                alive_nodes.add(node_id)
    return alive_nodes

def load_node_status():
    if not os.path.exists(STATUS_FILE):
        return {str(i): "available" for i in range(1, TOTAL_NODES + 1)}
    with open(STATUS_FILE, "r") as f:
        return json.load(f)

def save_node_status(status):
    with open(STATUS_FILE, "w") as f:
        json.dump(status, f, indent=4)

def connect_client(client_id):
    alive_nodes = get_alive_nodes()
    node_status = load_node_status()

    for node_id in alive_nodes:
        if node_status[str(node_id)] == "available":
            node_status[str(node_id)] = "occupied"
            save_node_status(node_status)
            print(f"✅ Client {client_id} connected to Node {node_id}")
            return node_id

    print(f"❌ Client {client_id} could not find an available node.")
    return None

# Example: simulate all 8 clients
if __name__ == "__main__":
    for i in range(1, 9):
        connect_client(i)
