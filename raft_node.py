from flask import Flask, request, jsonify
import threading
import time
import random
import requests
import os

app = Flask(__name__)

NODES = 4  # change to add more nodes

# Configuration
TOTAL_NODES = 8
NODE_ID = int(os.getenv("NODE_ID", 1))
PORT = 5000
PEERS = [f"http://raft_node_{i}:{PORT}" for i in range(1, TOTAL_NODES + 1) if i != NODE_ID]

# State variables
STATE = "follower"
CURRENT_TERM = 0
VOTED_FOR = None
LEADER = None
last_heartbeat = time.monotonic()
last_vote_time = 0  # Track when we last voted
VOTE_TIMEOUT = 15   # Don't vote again for 15 seconds after voting
lock = threading.Lock()

print(f"[Node {NODE_ID}] Peers: {PEERS}")

# Random election timeout (will change on each timeout)
ELECTION_TIMEOUT = random.randint(5, 30)


@app.route('/vote', methods=['POST'])
def vote():
    global CURRENT_TERM, VOTED_FOR, STATE, LEADER, last_vote_time
    data = request.get_json()
    term = data['term']
    candidate_id = data['candidate_id']

    with lock:
        # If this node is already a leader, inform the requester
        if STATE == "leader" and CURRENT_TERM >= term:
            print(f"[Node {NODE_ID}] Rejecting vote request from {candidate_id} - I am already leader for term {CURRENT_TERM}")
            return jsonify({
                'vote_granted': False,
                'is_leader': True,
                'leader_id': NODE_ID,
                'leader_term': CURRENT_TERM
            })

        # Check if we're in the vote timeout period
        current_time = time.monotonic()
        if current_time - last_vote_time < VOTE_TIMEOUT and VOTED_FOR is not None:
            print(f"[Node {NODE_ID}] Rejecting vote request from {candidate_id} - in voting timeout period ({current_time - last_vote_time:.1f}s < {VOTE_TIMEOUT}s)")
            return jsonify({
                'vote_granted': False,
                'is_leader': False,
                'reason': 'vote_timeout'
            })

        # If we see a higher term, we update our term but DON'T automatically vote
        if term > CURRENT_TERM:
            CURRENT_TERM = term
            VOTED_FOR = None  # Reset vote for new term only
            # Reset to follower if we see a higher term
            if STATE != "follower":
                STATE = "follower"
        
        # Only vote if we haven't voted in this term yet
        if VOTED_FOR is None and term >= CURRENT_TERM:
            VOTED_FOR = candidate_id
            last_vote_time = current_time  # Record when we voted
            last_heartbeat = current_time  # Reset heartbeat timer when voting
            print(f"[Node {NODE_ID}] Voted for {candidate_id} in term {term}")
            return jsonify({
                'vote_granted': True,
                'is_leader': False
            })
        else:
            # We've already voted in this term
            if VOTED_FOR == candidate_id:
                print(f"[Node {NODE_ID}] Already voted for {candidate_id} in term {term}")
            else:
                print(f"[Node {NODE_ID}] Rejecting vote for {candidate_id}, already voted for {VOTED_FOR} in term {CURRENT_TERM}")
            
            return jsonify({
                'vote_granted': False,
                'is_leader': False,
                'reason': 'already_voted',
                'voted_for': VOTED_FOR
            })


@app.route('/heartbeat', methods=['POST'])
def heartbeat():
    global last_heartbeat, CURRENT_TERM, STATE, LEADER, VOTED_FOR
    data = request.get_json()
    term = data['term']
    leader_id = data['leader_id']

    with lock:
        if term >= CURRENT_TERM:
            # If we receive a heartbeat with valid term, accept the leader
            CURRENT_TERM = term
            STATE = "follower"
            LEADER = leader_id
            VOTED_FOR = None  # Reset vote when we accept a leader
            last_heartbeat = time.monotonic()
            print(f"[Node {NODE_ID}] Received heartbeat from Leader {leader_id} (term {term})")
        return '', 200


@app.route('/leader_announcement', methods=['POST'])
def leader_announcement():
    global CURRENT_TERM, STATE, LEADER, VOTED_FOR
    data = request.get_json()
    term = data['term']
    leader_id = data['leader_id']

    with lock:
        if term >= CURRENT_TERM:
            CURRENT_TERM = term
            STATE = "follower"
            LEADER = leader_id
            VOTED_FOR = None  # Reset vote
            last_heartbeat = time.monotonic()  # Reset heartbeat timer
            print(f"[Node {NODE_ID}] Received leader announcement from {leader_id} for term {term}")
            return jsonify({'success': True})
        else:
            return jsonify({'success': False})


@app.route('/status', methods=['GET'])
def status():
    with lock:
        return jsonify({
            'node_id': NODE_ID,
            'state': STATE,
            'term': CURRENT_TERM,
            'leader': LEADER,
            'voted_for': VOTED_FOR
        })


def election_loop():
    global last_heartbeat, CURRENT_TERM, STATE, VOTED_FOR, LEADER, ELECTION_TIMEOUT, last_vote_time
    while True:
        time.sleep(1)
        now = time.monotonic()

        with lock:
            time_since_heartbeat = now - last_heartbeat
            time_since_last_vote = now - last_vote_time
            
            # Only start election if:
            # 1. We're not a leader
            # 2. Heartbeat timeout has passed
            # 3. Vote timeout has passed
            timeout_reached = (STATE != "leader" and 
                              time_since_heartbeat > ELECTION_TIMEOUT and
                              time_since_last_vote > VOTE_TIMEOUT)

        if timeout_reached:
            with lock:
                CURRENT_TERM += 1
                STATE = "candidate"
                VOTED_FOR = NODE_ID  # Vote for self first
                LEADER = None
                term_started = CURRENT_TERM
                votes = 1  # Vote for self first!
                last_vote_time = now  # Reset vote timeout when starting election
                print(f"[Node {NODE_ID}] Timeout. Starting election for term {CURRENT_TERM}")
                last_heartbeat = now
                ELECTION_TIMEOUT = random.randint(5, 10)

            # Send vote requests
            for peer in PEERS:
                # Skip if we're no longer a candidate
                with lock:
                    if STATE != "candidate":
                        break
                        
                try:
                    resp = requests.post(f"{peer}/vote", json={
                        "term": term_started,
                        "candidate_id": NODE_ID
                    }, timeout=1)
                    
                    if resp.status_code == 200:
                        response_data = resp.json()
                        
                        # If the peer is already a leader, step down immediately
                        if response_data.get("is_leader", False):
                            leader_id = response_data.get("leader_id")
                            leader_term = response_data.get("leader_term")
                            
                            with lock:
                                if leader_term >= CURRENT_TERM:
                                    print(f"[Node {NODE_ID}] Found existing leader {leader_id} for term {leader_term}, stepping down")
                                    STATE = "follower"
                                    CURRENT_TERM = leader_term
                                    LEADER = leader_id
                                    VOTED_FOR = None
                                    last_heartbeat = time.monotonic()
                                    break  # Exit vote collection loop
                        
                        # If we got a vote, count it
                        elif response_data.get("vote_granted"):
                            votes += 1
                            
                except requests.RequestException:
                    continue

            # Check for majority
            with lock:
                if STATE == "candidate" and CURRENT_TERM == term_started and votes > (TOTAL_NODES // 2):
                    STATE = "leader"
                    LEADER = NODE_ID
                    print(f"[Node {NODE_ID}] Won election for term {CURRENT_TERM} with {votes} votes.")

                    # Announce leadership to all peers
                    announce_leadership(CURRENT_TERM, NODE_ID)
                    
                    # Send immediate heartbeats
                    send_heartbeats(CURRENT_TERM, NODE_ID)
                else:
                    if STATE == "candidate":
                        print(f"[Node {NODE_ID}] Election failed. Votes: {votes}")
                        # Return to follower state if election failed
                        STATE = "follower"


def announce_leadership(term, leader_id):
    """Announce leadership to all peers when elected"""
    for peer in PEERS:
        try:
            requests.post(f"{peer}/leader_announcement", json={
                "term": term,
                "leader_id": leader_id
            }, timeout=1)
        except requests.RequestException:
            pass


def send_heartbeats(term, leader_id):
    """Send immediate heartbeats to all peers"""
    for peer in PEERS:
        try:
            requests.post(f"{peer}/heartbeat", json={
                "term": term,
                "leader_id": leader_id
            }, timeout=1)
        except requests.RequestException:
            pass


def heartbeat_loop():
    global CURRENT_TERM
    while True:
        time.sleep(2)
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


if __name__ == "__main__":
    print(f"[Node {NODE_ID}] Starting Raft node on port {PORT}")
    threading.Thread(target=election_loop, daemon=True).start()
    threading.Thread(target=heartbeat_loop, daemon=True).start()
    app.run(host='0.0.0.0', port=PORT)