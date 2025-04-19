from flask import Flask, request, jsonify
import threading
import time
import random
import requests
import os
import logging

# --- Setup Logging ---
node_id_str = os.getenv("NODE_ID", "unknown")
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_filename = f"{log_dir}/raft_node_{node_id_str}.log"

logging.basicConfig(
    filename=log_filename,
    level=logging.INFO,
    format=f"%(asctime)s [Node {node_id_str}] %(levelname)s: %(message)s"
)

# Optional: also log to stdout (for development/debugging)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter(f"%(asctime)s [Node {node_id_str}] %(levelname)s: %(message)s")
console_handler.setFormatter(console_formatter)
logging.getLogger().addHandler(console_handler)

import firebase_admin
from firebase_admin import db,storage,credentials

DBURL = os.getenv("DBURL")

#firebase connection
cred = credentials.Certificate("credentials.json")
firebase_admin.initialize_app(cred,{'databaseURL':DBURL}) 

#test if the db connection is working
logging.info(db)

app = Flask(__name__)

NODES = 8  # change to add more nodes

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
VOTE_TIMEOUT = 5   # Don't vote again for 15 seconds after voting
lock = threading.Lock()

logging.info(f"[Node {NODE_ID}] Peers: {PEERS}")

# Random election timeout (will change on each timeout)
ELECTION_TIMEOUT = random.randint(5, 30)


FOLLOWER_STATUS = {}  # Track status of all nodes (active/inactive)
FOLLOWER_LAST_SEEN = {}  # Track when each follower was last seen
FOLLOWER_TIMEOUT = 15  # 30 seconds timeout for follower inactivity

@app.route('/vote', methods=['POST'])
def vote():
    global CURRENT_TERM, VOTED_FOR, STATE, LEADER, last_vote_time
    data = request.get_json()
    term = data['term']
    candidate_id = data['candidate_id']

    with lock:
        # If this node is already a leader, inform the requester
        if STATE == "leader" and CURRENT_TERM >= term - 1:
            logging.info(f"[Node {NODE_ID}] Rejecting vote request from {candidate_id} - I am already leader for term {CURRENT_TERM}")
            return jsonify({
                'vote_granted': False,
                'is_leader': True,
                'leader_id': NODE_ID,
                'leader_term': CURRENT_TERM
            })

        # Check if we're in the vote timeout period
        current_time = time.monotonic()
        if current_time - last_vote_time < VOTE_TIMEOUT and VOTED_FOR is not None:
            logging.info(f"[Node {NODE_ID}] Rejecting vote request from {candidate_id} - in voting timeout period ({current_time - last_vote_time:.1f}s < {VOTE_TIMEOUT}s)")
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
            last_vote_time = current_time
            last_heartbeat = current_time

            if STATE != "follower":
                STATE = "follower"
                LEADER = None

            logging.info(f"[Node {NODE_ID}] Voted for {candidate_id} in term {term}")
            return jsonify({
                'vote_granted': True,
                'is_leader': False
            })

        else:
            # We've already voted in this term
            if VOTED_FOR == candidate_id:
                logging.info(f"[Node {NODE_ID}] Already voted for {candidate_id} in term {term}")
            else:
                logging.info(f"[Node {NODE_ID}] Rejecting vote for {candidate_id}, already voted for {VOTED_FOR} in term {CURRENT_TERM}")
            
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
            # Accept leader and update state
            CURRENT_TERM = term
            STATE = "follower"
            LEADER = leader_id
            VOTED_FOR = None
            last_heartbeat = time.monotonic()
            logging.info(f"[Node {NODE_ID}] Received heartbeat from Leader {leader_id} (term {term})")

        # Send acknowledgment back to leader
        try:
            leader_url = f"http://raft_node_{leader_id}:{PORT}/heartbeat_response"
            requests.post(leader_url, json={
                "follower_id": NODE_ID,
                "term": term
            }, timeout=1)
        except requests.RequestException as e:
            logging.warning(f"[Node {NODE_ID}] Failed to send heartbeat response to leader {leader_id}: {e}")

        return '', 200


@app.route('/heartbeat_response', methods=['POST'])
def heartbeat_response():
    global FOLLOWER_STATUS, FOLLOWER_LAST_SEEN
    data = request.get_json()
    follower_id = data.get('follower_id')
    term = data.get('term')
    
    if STATE == "leader" and term == CURRENT_TERM:
        current_time = time.monotonic()
        
        # Check if this follower was previously marked inactive
        was_inactive = FOLLOWER_STATUS.get(follower_id) == "inactive"
        
        # Update status and last seen time
        FOLLOWER_STATUS[follower_id] = "active"
        FOLLOWER_LAST_SEEN[follower_id] = current_time
        
        # Log recovery if previously inactive
        if was_inactive:
            logging.info(f"[Node {NODE_ID}] Follower {follower_id} has recovered and is now active")
        else:
            logging.info(f"[Node {NODE_ID}] Received heartbeat response from follower {follower_id}")
            
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
            logging.info(f"[Node {NODE_ID}] Received leader announcement from {leader_id} for term {term}")
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

@app.route('/cluster_status', methods=['GET'])
def cluster_status():
    with lock:
        return jsonify({
            'leader': LEADER,
            'current_term': CURRENT_TERM,
            'node_statuses': FOLLOWER_STATUS
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
                logging.info(f"[Node {NODE_ID}] Timeout. Starting election for term {CURRENT_TERM}")
                last_heartbeat = now
                ELECTION_TIMEOUT = random.randint(5, 30)

            # Create a list to store vote request threads
            vote_threads = []
            # Create a shared votes counter that threads can increment
            vote_counter = {'count': 1}  # Start with 1 (self vote)
            vote_counter_lock = threading.Lock()
            
            # Function for vote request worker threads
            def request_vote(peer, term, node_id, counter, counter_lock):
                # Properly declare globals inside the function
                global STATE, CURRENT_TERM, LEADER, VOTED_FOR, last_heartbeat
                
                # Skip if we're no longer a candidate
                with lock:
                    if STATE != "candidate":
                        return
                    
                try:
                    resp = requests.post(f"{peer}/vote", json={
                        "term": term,
                        "candidate_id": node_id
                    }, timeout=1)
                    
                    if resp.status_code == 200:
                        response_data = resp.json()
                        
                        # If the peer is already a leader, step down immediately
                        if response_data.get("is_leader", False):
                            leader_id = response_data.get("leader_id")
                            leader_term = response_data.get("leader_term")
                            
                            with lock:
                                if leader_term >= CURRENT_TERM:
                                    logging.info(f"[Node {NODE_ID}] Found existing leader {leader_id} for term {leader_term}, stepping down")
                                    STATE = "follower"
                                    CURRENT_TERM = leader_term
                                    LEADER = leader_id
                                    VOTED_FOR = None
                                    last_heartbeat = time.monotonic()

                        
                        # If we got a vote, count it
                        elif response_data.get("vote_granted"):
                            with counter_lock:
                                counter['count'] += 1
                                logging.info(f"[Node {NODE_ID}] Received vote from peer at {peer}, total votes: {counter['count']}")
                            
                except requests.RequestException as e:
                    logging.debug(f"[Node {NODE_ID}] Failed to get vote from {peer}: {e}")
            
            # Start a thread for each peer vote request
            for peer in PEERS:
                vote_thread = threading.Thread(
                    target=request_vote,
                    args=(peer, term_started, NODE_ID, vote_counter, vote_counter_lock),
                    daemon=True
                )
                vote_thread.start()
                vote_threads.append(vote_thread)
            
            # Wait for all vote requests to complete (with timeout)
            vote_collection_timeout = min(3, ELECTION_TIMEOUT / 2)  # Don't wait too long
            start_time = time.monotonic()
            for thread in vote_threads:
                remaining_time = max(0, vote_collection_timeout - (time.monotonic() - start_time))
                thread.join(timeout=remaining_time)
                if time.monotonic() - start_time >= vote_collection_timeout:
                    logging.info(f"[Node {NODE_ID}] Vote collection timed out after {vote_collection_timeout}s")
                    break

            # Check for majority
            with lock:
                if STATE == "candidate" and CURRENT_TERM == term_started and vote_counter['count'] > (TOTAL_NODES // 2):
                    STATE = "leader"
                    LEADER = NODE_ID
                    logging.info(f"[Node {NODE_ID}] Won election for term {CURRENT_TERM} with {vote_counter['count']} votes.")

                    # Announce leadership to all peers
                    announce_leadership(CURRENT_TERM, NODE_ID)
                    
                    # Send immediate heartbeats
                    send_heartbeats(CURRENT_TERM, NODE_ID)
                else:
                    if STATE == "candidate":
                        logging.info(f"[Node {NODE_ID}] Election failed. Votes: {vote_counter['count']}")
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
    """Send immediate heartbeats to all peers asynchronously"""
    heartbeat_threads = []
    
    def send_single_heartbeat(peer, current_term, current_leader_id):
        try:
            requests.post(f"{peer}/heartbeat", json={
                "term": current_term,
                "leader_id": current_leader_id
            }, timeout=1)
        except requests.RequestException as e:
            logging.debug(f"[Node {NODE_ID}] Failed to send heartbeat to {peer}: {e}")
    
    # Send multiple rounds of heartbeats in quick succession
    for _ in range(3):  # Send 3 rounds of heartbeats
        threads_round = []
        for peer in PEERS:
            # Create and start a thread for each peer
            heartbeat_thread = threading.Thread(
                target=send_single_heartbeat,
                args=(peer, term, leader_id),
                daemon=True
            )
            heartbeat_thread.start()
            threads_round.append(heartbeat_thread)
        
        # Wait for all threads in this round to complete (with timeout)
        for thread in threads_round:
            thread.join(timeout=0.5)
        
        # Brief pause between rounds
        time.sleep(0.2)


def heartbeat_loop():
    """Periodically send heartbeats to all followers if this node is the leader"""
    global CURRENT_TERM
    while True:
        time.sleep(2)
        with lock:
            if STATE == "leader":
                current_term = CURRENT_TERM
                current_id = NODE_ID
        
        # Only proceed if we're the leader
        if STATE == "leader":
            heartbeat_threads = []
            
            def send_heartbeat_to_peer(peer, term, leader_id):
                try:
                    requests.post(f"{peer}/heartbeat", json={
                        "term": term,
                        "leader_id": leader_id
                    }, timeout=1)
                except requests.RequestException as e:
                    logging.debug(f"[Node {NODE_ID}] Failed to send heartbeat to {peer}: {e}")
            
            # Create and start a thread for each peer
            for peer in PEERS:
                heartbeat_thread = threading.Thread(
                    target=send_heartbeat_to_peer, 
                    args=(peer, current_term, current_id),
                    daemon=True
                )
                heartbeat_thread.start()
                heartbeat_threads.append(heartbeat_thread)
            
            # Optional: Wait for heartbeats to complete (with timeout)
            # This prevents heartbeat_loop from sending new heartbeats before previous ones finish
            for thread in heartbeat_threads:
                thread.join(timeout=1.0)  # Don't wait more than 1 second


def check_follower_status():
    """Periodically check if followers have timed out"""
    global FOLLOWER_STATUS, FOLLOWER_LAST_SEEN
    
    while True:
        time.sleep(5)  # Check every 5 seconds
        
        # Only relevant when we're the leader
        with lock:
            if STATE != "leader":
                continue
                
            current_time = time.monotonic()
            
            # Check each follower's status
            for node_id in range(1, TOTAL_NODES + 1):
                if node_id == NODE_ID:  # Skip self
                    continue
                    
                last_seen = FOLLOWER_LAST_SEEN.get(node_id, 0)
                time_since_last_seen = current_time - last_seen
                
                # Mark as inactive if timeout exceeded
                if time_since_last_seen > FOLLOWER_TIMEOUT:
                    if FOLLOWER_STATUS.get(node_id) != "inactive":
                        FOLLOWER_STATUS[node_id] = "inactive"
                        logging.warning(f"[Node {NODE_ID}] Follower {node_id} is inactive (no response for {time_since_last_seen:.1f}s)")


if __name__ == "__main__":
    logging.info(f"[Node {NODE_ID}] Starting Raft node on port {PORT}")
    threading.Thread(target=election_loop, daemon=True).start()
    threading.Thread(target=heartbeat_loop, daemon=True).start()
    threading.Thread(target=check_follower_status, daemon=True).start()  # Add this line
    app.run(host='0.0.0.0', port=PORT)