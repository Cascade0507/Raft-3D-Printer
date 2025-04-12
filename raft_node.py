from flask import Flask, request, jsonify
import threading
import time
import requests
import random
import sys

app = Flask(__name__)

NODES = 4  # change to add more nodes

NODE_ID = int(sys.argv[1])
PEERS = [f"http://raft_node_{i}:5000" for i in range(1, NODES + 1) if i != NODE_ID]
STATE = "follower"
CURRENT_TERM = 0
VOTED_FOR = None
LEADER = None
lock = threading.Lock()

print(f"[Node {NODE_ID}] Peers: {PEERS}")

HEARTBEAT_INTERVAL = 2
ELECTION_TIMEOUT = random.uniform(5, 10)  # staggered timeout per node
last_heartbeat = time.monotonic()


@app.route('/heartbeat', methods=['POST'])
def heartbeat():
    global last_heartbeat, CURRENT_TERM, LEADER, STATE, VOTED_FOR
    data = request.get_json()
    with lock:
        if data['term'] > CURRENT_TERM:
            CURRENT_TERM = data['term']
            STATE = "follower"
            VOTED_FOR = None
        if data['term'] >= CURRENT_TERM:
            CURRENT_TERM = data['term']
            last_heartbeat = time.monotonic()
            LEADER = data['leader_id']
            if STATE != "follower":
                print(f"[Node {NODE_ID}] Changing to follower (term {CURRENT_TERM}) due to heartbeat from leader {LEADER}")
            STATE = "follower"
            return jsonify({"status": "ok"})
    return jsonify({"status": "rejected"}), 403


@app.route('/vote', methods=['POST'])
def vote():
    global VOTED_FOR, CURRENT_TERM, STATE, last_heartbeat
    data = request.get_json()
    with lock:
        if data['term'] > CURRENT_TERM:
            CURRENT_TERM = data['term']
            VOTED_FOR = None
            STATE = "follower"
        if (VOTED_FOR is None or VOTED_FOR == data['candidate_id']) and data['term'] == CURRENT_TERM:
            VOTED_FOR = data['candidate_id']
            last_heartbeat = time.monotonic()  # prevent immediate re-election
            print(f"[Node {NODE_ID}] Voting for {VOTED_FOR} in term {CURRENT_TERM}")
            return jsonify({"vote_granted": True})
    return jsonify({"vote_granted": False})


@app.route('/status', methods=['GET'])
def status():
    return jsonify({
        "node_id": NODE_ID,
        "state": STATE,
        "term": CURRENT_TERM,
        "leader": LEADER
    })


def send_heartbeat():
    global PEERS, CURRENT_TERM
    while True:
        time.sleep(HEARTBEAT_INTERVAL)
        with lock:
            if STATE == "leader":
                for peer in PEERS:
                    try:
                        requests.post(f"{peer}/heartbeat", json={
                            "term": CURRENT_TERM,
                            "leader_id": NODE_ID
                        }, timeout=1)
                    except requests.RequestException:
                        pass


def election_loop():
    global last_heartbeat, CURRENT_TERM, STATE, VOTED_FOR, LEADER
    while True:
        time.sleep(1)
        start_election = False

        with lock:
            time_since_heartbeat = time.monotonic() - last_heartbeat
            if STATE != "leader" and time_since_heartbeat > ELECTION_TIMEOUT:
                print(f"[Node {NODE_ID}] No heartbeat received in {ELECTION_TIMEOUT:.2f}s. Starting election.")
                STATE = "candidate"
                CURRENT_TERM += 1
                VOTED_FOR = NODE_ID
                LEADER = None
                start_election = True

        if start_election:
            votes = 1  # Vote for self
            for peer in PEERS:
                try:
                    resp = requests.post(f"{peer}/vote", json={
                        "term": CURRENT_TERM,
                        "candidate_id": NODE_ID
                    }, timeout=1)
                    if resp.status_code == 200 and resp.json().get("vote_granted"):
                        votes += 1
                except requests.RequestException:
                    pass

            with lock:
                print(f"[Node {NODE_ID}] Collected {votes} votes in term {CURRENT_TERM}")
                if STATE == "candidate" and votes > len(PEERS) // 2:
                    STATE = "leader"
                    LEADER = NODE_ID
                    last_heartbeat = time.monotonic()
                    print(f"[Node {NODE_ID}] Became leader for term {CURRENT_TERM} with {votes} votes.")
                elif STATE == "candidate":
                    STATE = "follower"
                    print(f"[Node {NODE_ID}] Election failed with {votes} votes. Reverting to follower.")


if __name__ == "__main__":
    threading.Thread(target=election_loop, daemon=True).start()
    threading.Thread(target=send_heartbeat, daemon=True).start()
    app.run(host="0.0.0.0", port=5000)