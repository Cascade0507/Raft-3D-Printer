import json
import requests
import time
from client_connector import get_alive_nodes, load_node_status, save_node_status

STATUS_FILE = "node_status.json"

def get_available_node():
    alive = get_alive_nodes()
    status = load_node_status()

    for nid in alive:
        if status[str(nid)] == "available":
            status[str(nid)] = "occupied"
            save_node_status(status)
            return nid
    return None

def get_leader_from_status(node_id):
    port = 5000 + node_id
    url = f"http://127.0.0.1:{port}/status"

    try:
        response = requests.get(url, timeout=3)
        if response.status_code == 200:
            data = response.json()
            return data.get("leader")
    except Exception as e:
        print(f"⚠️ Couldn't query leader from Node {node_id}: {e}")
    return None

def run_client(client_id):
    node_id = get_available_node()
    if node_id is None:
        print(f"❌ Client {client_id} couldn't find a free node.")
        return

    port = 5000 + node_id
    url = f"http://127.0.0.1:{port}/status"

    try:
        response = requests.get(url, timeout=3)
        data = response.json()
        print(f"✅ Client {client_id} connected to Node {node_id} (Port {port})")
        print(f"   Node {node_id} is in state: {data['state']} (Term: {data['term']})")

        leader_id = data.get("leader")
        if leader_id:
            print(f"👑 Client {client_id} sees Leader: Node {leader_id} (Port {5000 + leader_id})")
        else:
            print(f"❓ Client {client_id} sees no leader elected yet.")

    except Exception as e:
        print(f"⚠️ Client {client_id} failed to connect to Node {node_id}: {e}")

if __name__ == "__main__":
    for cid in range(1, 9):
        run_client(cid)
        time.sleep(0.5)
