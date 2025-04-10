from flask import Flask, request, jsonify
import threading
import time
import requests
import random
import sys

app = Flask(__name__)

NODE_ID = int(sys.argv[1])
PEERS = [f"http://raft_node_{i}:5000" for i in range(1, 4) if i != NODE_ID]
STATE = "follower"
CURRENT_TERM = 0
VOTED_FOR = None
LEADER = None
lock = threading.Lock()

HEARTBEAT_INTERVAL = 2
ELECTION_TIMEOUT = random.randint(5, 10)
last_heartbeat = time.monotonic()


@app.route('/heartbeat', methods=['POST'])
def heartbeat():
    global last_heartbeat, CURRENT_TERM, LEADER, STATE
    data = request.get_json()
    with lock:
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
    global VOTED_FOR, CURRENT_TERM
    data = request.get_json()
    with lock:
        if (VOTED_FOR is None or VOTED_FOR == data['candidate_id']) and data['term'] >= CURRENT_TERM:
            VOTED_FOR = data['candidate_id']
            CURRENT_TERM = data['term']
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
    global last_heartbeat, CURRENT_TERM, STATE, VOTED_FOR, LEADER, ELECTION_TIMEOUT
    while True:
        time.sleep(1)
        start_election = False

        with lock:
            time_since_heartbeat = time.monotonic() - last_heartbeat
            if STATE != "leader" and time_since_heartbeat > ELECTION_TIMEOUT:
                print(f"[Node {NODE_ID}] No heartbeat received in {ELECTION_TIMEOUT}s. Starting election.")
                STATE = "candidate"
                CURRENT_TERM += 1
                VOTED_FOR = NODE_ID
                ELECTION_TIMEOUT = random.randint(5, 10)
                start_election = True  # only do election if timed out

        if start_election:
            votes = 1  # Start with 1 (self vote)
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
                if STATE == "candidate" and votes > len(PEERS) // 2:
                    STATE = "leader"
                    LEADER = NODE_ID
                    print(f"[Node {NODE_ID}] Became leader for term {CURRENT_TERM} with {votes} votes.")
                elif STATE == "candidate":
                    STATE = "follower"
                    print(f"[Node {NODE_ID}] Election failed with {votes} votes. Reverting to follower.")


if __name__ == "__main__":
    threading.Thread(target=election_loop, daemon=True).start()
    threading.Thread(target=send_heartbeat, daemon=True).start()
    app.run(host="0.0.0.0", port=5000)
