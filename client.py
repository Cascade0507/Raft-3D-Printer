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





node_id = get_available_node()
response = requests.get(f"http://raft_node_{node_id}:5000/cluster_status")
leader = response.json()['leader']


def create_printer():
    printer_id = input("enter printer id: ")
    company = input("enter company name: ")
    model = input("enter model name: ")
    data = {
        "id": printer_id,
        "company": company,
        "model": model
    }
    write = {"type":"printer","data":data}
    response = requests.post(f"http://raft_node_{leader}:5000/initiate_write",data = json.dumps(write))
    print(response)

def create_filament():
    filament_id = input("enter filament id: ")
    fil_type = input("enter filament type: ")
    color = input("enter filament color: ")
    tot_weight = input("enter total weight: ")
    rem_weight = input("enter remaining weight: ")
    data = {
        "id": filament_id,
        "type": fil_type,
        "color": color,
        "total_weight": tot_weight,
        "remaining_weight": rem_weight
    }
    write = {"type":"filament","data":data}
    response = requests.post(f"http://raft_node_{leader}:5000/initiate_write",data = json.dumps(write))
    print(response)

def create_print_job():
    job_id = input("")
    pinter_id = input("enter printer id: ")
    filament_id = input("enter filament id: ")
    filepath = input("enter file path: ")
    print_weight = input("enter print weight: ")
    #status = input("enter status: ")
    data = {
        "id": job_id,
        "printer_id": pinter_id,
        "filament_id": filament_id,
        "filepath": filepath,
        "print_weight_in_grams": print_weight,
        "status": "Queued"
    }
    write = {"type":"print_job","data":data}
    response = requests.post(f"http://raft_node_{leader}:5000/initiate_write",data = json.dumps(write))
    print(response.json())


cont = 1
print("Welcome to Raft 3D printer system, select an option:")
print("1. Create Printer ")
print("2. Create new Filament")
print("3. Create print job")


while cont:
    opt = input("Enter your option: ")
    if opt == "1":
        create_printer()
    elif opt == "2":
        create_filament()
    elif opt == "3":
        create_print_job()
    else:
        print("invalid option")
    cont = int(input("Do you want to continue? (1/0): "))
