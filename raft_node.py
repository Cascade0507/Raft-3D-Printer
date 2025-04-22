# from flask import Flask, request, jsonify
# import threading
# import time
# import random
# import requests
# import os
# import logging
# import ast

# # --- Setup Logging ---
# node_id_str = os.getenv("NODE_ID", "unknown")
# log_dir = "logs"
# os.makedirs(log_dir, exist_ok=True)
# log_filename = f"{log_dir}/raft_node_{node_id_str}.log"

# def check_exists(content):
#     with open(f"{log_dir}/raft_node_{node_id_str}.log", 'r') as f:
#         if content in f.read():
#             return True
#         return False

# # Create a heartbeat filter that will only let heartbeat messages through to console
# class HeartbeatFilter(logging.Filter):
#     def filter(self, record):
#         return "heartbeat" in record.getMessage().lower()

# # Create a non-heartbeat filter that will block heartbeat messages
# class NonHeartbeatFilter(logging.Filter):
#     def filter(self, record):
#         return "heartbeat" not in record.getMessage().lower()

# # Set up file handler with non-heartbeat filter
# file_handler = logging.FileHandler(log_filename)
# file_handler.setLevel(logging.INFO)
# file_formatter = logging.Formatter(f"%(asctime)s [Node {node_id_str}] %(levelname)s: %(message)s")
# file_handler.setFormatter(file_formatter)
# file_handler.addFilter(NonHeartbeatFilter())  # Only log non-heartbeat messages to file

# # Set up console handler - will show all messages including heartbeats
# console_handler = logging.StreamHandler()
# console_handler.setLevel(logging.INFO)
# console_formatter = logging.Formatter(f"%(asctime)s [Node {node_id_str}] %(levelname)s: %(message)s")
# console_handler.setFormatter(console_formatter)

# # Configure the root logger without using basicConfig
# logger = logging.getLogger()
# logger.setLevel(logging.INFO)
# logger.addHandler(file_handler)
# logger.addHandler(console_handler)

# import firebase_admin
# from firebase_admin import db,storage,credentials

# DBURL = os.getenv("DBURL")

# #firebase connection
# cred = credentials.Certificate("credentials.json")
# firebase_admin.initialize_app(cred,{'databaseURL':DBURL}) 

# #test if the db connection is working
# if not check_exists("DB connection established"):
#     logging.info("DB connection established")
     

# app = Flask(__name__)

# NODES = 8  # change to add more nodes

# # Configuration
# TOTAL_NODES = 8
# NODE_ID = int(os.getenv("NODE_ID", 1))
# PORT = 5000
# PEERS = [f"http://raft_node_{i}:{PORT}" for i in range(1, TOTAL_NODES + 1) if i != NODE_ID]

# # State variables
# STATE = "follower"
# CURRENT_TERM = 0
# VOTED_FOR = None
# LEADER = None
# last_heartbeat = time.monotonic()
# last_vote_time = 0  # Track when we last voted
# VOTE_TIMEOUT = 5   # Don't vote again for 15 seconds after voting
# lock = threading.Lock()

# if not check_exists(f"[Node {NODE_ID}] Peers: {PEERS}"):
#     logging.info(f"[Node {NODE_ID}] Peers: {PEERS}")

# # Random election timeout (will change on each timeout)
# ELECTION_TIMEOUT = random.randint(5, 30)


# FOLLOWER_STATUS = {}  # Track status of all nodes (active/inactive)
# FOLLOWER_LAST_SEEN = {}  # Track when each follower was last seen
# FOLLOWER_TIMEOUT = 15  # 30 seconds timeout for follower inactivity

# @app.route('/vote', methods=['POST'])
# def vote():
#     global CURRENT_TERM, VOTED_FOR, STATE, LEADER, last_vote_time
#     data = request.get_json()
#     term = data['term']
#     candidate_id = data['candidate_id']

#     with lock:
#         # If this node is already a leader, inform the requester
#         if STATE == "leader" and CURRENT_TERM >= term - 1:
#             #logging.info(f"[Node {NODE_ID}] Rejecting vote request from {candidate_id} - I am already leader for term {CURRENT_TERM}")
#             return jsonify({
#                 'vote_granted': False,
#                 'is_leader': True,
#                 'leader_id': NODE_ID,
#                 'leader_term': CURRENT_TERM
#             })

#         # Check if we're in the vote timeout period
#         current_time = time.monotonic()
#         if current_time - last_vote_time < VOTE_TIMEOUT and VOTED_FOR is not None:
#             #logging.info(f"[Node {NODE_ID}] Rejecting vote request from {candidate_id} - in voting timeout period ({current_time - last_vote_time:.1f}s < {VOTE_TIMEOUT}s)")
#             return jsonify({
#                 'vote_granted': False,
#                 'is_leader': False,
#                 'reason': 'vote_timeout'
#             })

#         # If we see a higher term, we update our term but DON'T automatically vote
#         if term > CURRENT_TERM:
#             CURRENT_TERM = term
#             VOTED_FOR = None  # Reset vote for new term only
#             # Reset to follower if we see a higher term
#             if STATE != "follower":
#                 STATE = "follower"
        
#         # Only vote if we haven't voted in this term yet
#         if VOTED_FOR is None and term >= CURRENT_TERM:
#             VOTED_FOR = candidate_id
#             last_vote_time = current_time
#             last_heartbeat = current_time

#             if STATE != "follower":
#                 STATE = "follower"
#                 LEADER = None

#             #logging.info(f"[Node {NODE_ID}] Voted for {candidate_id} in term {term}")
#             return jsonify({
#                 'vote_granted': True,
#                 'is_leader': False
#             })

#         else:
#             # # We've already voted in this term
#             # if VOTED_FOR == candidate_id:
#             #     logging.info(f"[Node {NODE_ID}] Already voted for {candidate_id} in term {term}")
#             # else:
#             #     logging.info(f"[Node {NODE_ID}] Rejecting vote for {candidate_id}, already voted for {VOTED_FOR} in term {CURRENT_TERM}")
            
#             return jsonify({
#                 'vote_granted': False,
#                 'is_leader': False,
#                 'reason': 'already_voted',
#                 'voted_for': VOTED_FOR
#             })


# @app.route('/heartbeat', methods=['POST'])
# def heartbeat():
#     global last_heartbeat, CURRENT_TERM, STATE, LEADER, VOTED_FOR
#     data = request.get_json()
#     term = data['term']
#     leader_id = data['leader_id']

#     with lock:
#         if term >= CURRENT_TERM:
#             # Accept leader and update state
#             CURRENT_TERM = term
#             STATE = "follower"
#             LEADER = leader_id
#             VOTED_FOR = None
#             last_heartbeat = time.monotonic()
#             #logging.info(f"[Node {NODE_ID}] HEARTBEAT: Received heartbeat from Leader {leader_id} (term {term})")

#         # Send acknowledgment back to leader
#         try:
#             leader_url = f"http://raft_node_{leader_id}:{PORT}/heartbeat_response"
#             requests.post(leader_url, json={
#                 "follower_id": NODE_ID,
#                 "term": term
#             }, timeout=1)
#         except requests.RequestException as e:
#             print(f"[Node {NODE_ID}] HEARTBEAT: Failed to send heartbeat response to leader {leader_id}: {e}")

#         return '', 200


# @app.route('/heartbeat_response', methods=['POST'])
# def heartbeat_response():
#     global FOLLOWER_STATUS, FOLLOWER_LAST_SEEN
#     data = request.get_json()
#     follower_id = data.get('follower_id')
#     term = data.get('term')
    
#     if STATE == "leader" and term == CURRENT_TERM:
#         current_time = time.monotonic()
        
#         # Check if this follower was previously marked inactive
#         was_inactive = FOLLOWER_STATUS.get(follower_id) == "inactive"
        
#         # Update status and last seen time
#         FOLLOWER_STATUS[follower_id] = "active"
#         FOLLOWER_LAST_SEEN[follower_id] = current_time
        
#         # Log recovery if previously inactive
#         # if was_inactive:
#         #     logging.info(f"[Node {NODE_ID}] Follower {follower_id} has recovered and is now active")
#         # else:
#         #     logging.info(f"[Node {NODE_ID}] HEARTBEAT: Received heartbeat response from follower {follower_id}")
            
#     return '', 200


# @app.route('/leader_announcement', methods=['POST'])
# def leader_announcement():
#     global CURRENT_TERM, STATE, LEADER, VOTED_FOR
#     data = request.get_json()
#     term = data['term']
#     leader_id = data['leader_id']

#     with lock:
#         if term >= CURRENT_TERM:
#             CURRENT_TERM = term
#             STATE = "follower"
#             LEADER = leader_id
#             VOTED_FOR = None  # Reset vote
#             last_heartbeat = time.monotonic()  # Reset heartbeat timer
#             #logging.info(f"[Node {NODE_ID}] Received leader announcement from {leader_id} for term {term}")
#             return jsonify({'success': True})
#         else:
#             return jsonify({'success': False})


# @app.route('/node_status', methods=['GET'])
# def status():
#     with lock:
#         return jsonify({
#             'node_id': NODE_ID,
#             'state': STATE,
#             'term': CURRENT_TERM,
#             'leader': LEADER,
#             'voted_for': VOTED_FOR
#         })

# @app.route('/cluster_status', methods=['GET'])
# def cluster_status():
#     with lock:
#         return jsonify({
#             'leader': LEADER,
#             'current_term': CURRENT_TERM,
#             'node_statuses': FOLLOWER_STATUS
#         })
    
# @app.route('/printer', methods=['POST'])
# def create_printer():
#     data = request.get_json()
#     printer_id = str(data.get("id"))

#     if not printer_id:
#         return jsonify({"error": "Printer ID is required"}), 400

#     try:
#         db.reference(f"printers/{printer_id}").set({
#             "id": printer_id,
#             "company": data.get("company"),
#             "model": data.get("model")
#         })
#         #logging.info(f"[Node {NODE_ID}] Created printer {printer_id}")
#         return jsonify({"success": True}), 201
#     except Exception as e:
#         #logging.error(f"[Node {NODE_ID}] Failed to create printer: {e}")
#         return jsonify({"error": str(e)}), 500

# @app.route('/filament', methods=['POST'])
# def create_filament():
#     data = request.get_json()
#     filament_id = str(data.get("id"))

#     if not filament_id:
#         return jsonify({"error": "Filament ID is required"}), 400

#     try:
#         db.reference(f"filaments/{filament_id}").set({
#             "id": filament_id,
#             "type": data.get("type"),
#             "color": data.get("color"),
#             "total_weight_in_grams": data.get("total_weight_in_grams"),
#             "remaining_weight_in_grams": data.get("remaining_weight_in_grams")
#         })
#         #logging.info(f"[Node {NODE_ID}] Created filament {filament_id}")
#         return jsonify({"success": True}), 201
#     except Exception as e:
#         #logging.error(f"[Node {NODE_ID}] Failed to create filament: {e}")
#         return jsonify({"error": str(e)}), 500


# @app.route('/printjob', methods=['POST'])
# def create_print_job():
#     data = request.get_json()
#     job_id = str(data.get("id"))

#     if not job_id:
#         return jsonify({"error": "Job ID is required"}), 400

#     try:
#         db.reference(f"print_jobs/{job_id}").set({
#             "id": job_id,
#             "printer_id": data.get("printer_id"),
#             "filament_id": data.get("filament_id"),
#             "filepath": data.get("filepath"),
#             "print_weight_in_grams": data.get("print_weight_in_grams"),
#             "status": data.get("status")
#         })
#         #logging.info(f"[Node {NODE_ID}] Created print job {job_id}")
#         return jsonify({"success": True}), 201
#     except Exception as e:
#         #logging.error(f"[Node {NODE_ID}] Failed to create print job: {e}")
#         return jsonify({"error": str(e)}), 500

# @app.route('/printer/<printer_id>', methods=['GET'])
# def get_printer(printer_id):
#     try:
#         printer_data = db.reference(f"printers/{printer_id}").get()
#         if printer_data:
#             return jsonify(printer_data)
#         else:
#             return jsonify({"error": "Printer not found"}), 404
#     except Exception as e:
#         #logging.error(f"[Node {NODE_ID}] Failed to fetch printer: {e}")
#         return jsonify({"error": str(e)}), 500

# @app.route('/filament/<filament_id>', methods=['GET'])
# def get_filament(filament_id):
#     try:
#         filament_data = db.reference(f"filaments/{filament_id}").get()
#         if filament_data:
#             return jsonify(filament_data)
#         else:
#             return jsonify({"error": "Filament not found"}), 404
#     except Exception as e:
#         #logging.error(f"[Node {NODE_ID}] Failed to fetch filament: {e}")
#         return jsonify({"error": str(e)}), 500

# @app.route('/printjob/<job_id>', methods=['GET'])
# def get_print_job(job_id):
#     try:
#         job_data = db.reference(f"print_jobs/{job_id}").get()
#         if job_data:
#             return jsonify(job_data)
#         else:
#             return jsonify({"error": "Print job not found"}), 404
#     except Exception as e:
#         #logging.error(f"[Node {NODE_ID}] Failed to retrieve print job: {e}")
#         return jsonify({"error": str(e)}), 500

# @app.route('/status', methods=['POST'])
# def update_print_job_status_post():
#     data = request.get_json()
#     job_id = str(data.get("job_id"))
#     status = data.get("status")

#     if not job_id or not status:
#         return jsonify({"error": "job_id and status are required"}), 400

#     job_ref = db.reference(f"print_jobs/{job_id}")
#     if not job_ref.get():
#         return jsonify({"error": "Print job not found"}), 404

#     try:
#         job_ref.update({"status": status})
#         #logging.info(f"[Node {NODE_ID}] Updated print job {job_id} status to {status}")
#         return jsonify({"success": True})
#     except Exception as e:
#         #logging.error(f"[Node {NODE_ID}] Failed to update print job status: {e}")
#         return jsonify({"error": str(e)}), 500


# #CONSENSUS

# @app.route('/initiate_write', methods=['POST'])
# def initiate_write():
#     if STATE != "leader":
#         return jsonify({"error": "Only leader can initiate write"}), 403

#     payload = request.get_json()
#     write_type = payload.get("type")
#     data = payload.get("data")
#     write_id = str(data.get("id"))

#     if not write_type or not data or not write_id:
#         return jsonify({"error": "Invalid request"}), 400

#     # Send write_ready to all followers asynchronously
#     ack_count = 1  # self
#     ack_lock = threading.Lock()
#     write_threads = []

#     def send_ready_request(peer_url):
#         nonlocal ack_count
#         try:
#             res = requests.post(f"{peer_url}/ready_check", json={"type": write_type, "id": write_id}, timeout=1)
#             if res.status_code == 200 and res.json().get("status") == "ready":
#                 with ack_lock:
#                     ack_count += 1
#         except Exception as e:
#             print(f"[Node {NODE_ID}] Failed to get ready from {peer_url}: {e}")    #logging.warning

#     for peer in PEERS:
#         thread = threading.Thread(target=send_ready_request, args=(peer,), daemon=True)
#         thread.start()
#         write_threads.append(thread)

#     # Wait for replies
#     for t in write_threads:
#         t.join(timeout=1)

#     # Check quorum
#     if ack_count > TOTAL_NODES // 2:
#         logging.info(f"[Node {NODE_ID}] Got majority ready ({ack_count}), committing write {write_id}, type: {write_type}, data: {data}")
        
#         # Commit locally using Firebase
#         db.reference(f"{write_type}s/{write_id}").set(data)

#         # Notify all followers to commit
#         def send_commit(peer_url):
#             try:
#                 requests.post(f"{peer_url}/commit_write", json={
#                     "type": write_type,
#                     "data": data
#                 }, timeout=1)
#             except Exception as e:
#                 print(f"[Node {NODE_ID}] Failed to send commit to {peer_url}: {e}")   #logging.warning

#         for peer in PEERS:
#             threading.Thread(target=send_commit, args=(peer,), daemon=True).start()

#         return jsonify({"success": True, "committed": True}), 200
#     else:
#         logging.warning(f"[Node {NODE_ID}] Not enough ready responses ({ack_count}), aborting write")
#         return jsonify({"success": False, "committed": False, "acks": ack_count}), 500

# @app.route('/ready_check', methods=['POST'])
# def ready_check():
#     data = request.get_json()
#     #logging.info(f"[Node {NODE_ID}] Received ready check for write: {data}")
#     return jsonify({"status": "ready"}), 200


# @app.route('/commit_write', methods=['POST'])
# def commit_write():
#     data = request.get_json()
#     write_type = data.get("type")
#     obj_data = data.get("data")
#     obj_id = str(obj_data.get("id"))

#     if not write_type or not obj_id or not obj_data:
#         return jsonify({"error": "Invalid commit data"}), 400

#     try:
#         db.reference(f"{write_type}s/{obj_id}").set(obj_data)
#         logging.info(f"[Node {NODE_ID}] Committed {write_type} {obj_id} from leader")
#         return jsonify({"success": True}), 200
#     except Exception as e:
#         logging.error(f"[Node {NODE_ID}] Failed to commit {write_type}: {e}")
#         return jsonify({"error": str(e)}), 500

# @app.route('/recovery', methods=['POST', 'GET'])
# def recovery():
#     recovery_entries = []
#     last = request.get_json()
#     if False in last.values():
        
#         for line in lines:
#             if 'committing write' in line:
#                 # Example: committing write 24, type: printer, data: {...}
#                 try:
#                     # Extract parts of the line
#                     write_id = line.split('committing write')[1].split(',')[0].strip()
#                     write_type = line.split('type:')[1].split(',')[0].strip()
#                     data_str = line.split('data:')[1].strip()
#                     data_dict = ast.literal_eval(data_str)
#                     recovery_entries.append({
#                             "id": write_id,
#                             "type": write_type,
#                             "data": data_dict
#                         })
#                 except Exception as e:
#                     # Skip lines that don't match format (e.g., HTTP request logs)
#                     continue
#     else: 
#         w_type = last['type']
#         w_id = last['id']

#         matched = False
        

#         with open(f"{log_dir}/raft_node_{NODE_ID}.log", 'r') as f:
#             lines = f.readlines()

#         for line in lines:
#             if 'committing write' in line:
#                 # Example: committing write 24, type: printer, data: {...}
#                 try:
#                     # Extract parts of the line
#                     write_id = line.split('committing write')[1].split(',')[0].strip()
#                     write_type = line.split('type:')[1].split(',')[0].strip()
#                     data_str = line.split('data:')[1].strip()
#                     data_dict = ast.literal_eval(data_str)

#                     if not matched:
#                         # Check if this is the log entry we need to start from
#                         if write_id == str(w_id) and write_type == w_type:
#                             matched = True
#                             recovery_entries.append({
#                                 "id": write_id,
#                                 "type": write_type,
#                                 "data": data_dict
#                             })
#                     else:
#                         # After matched, keep appending all writes
#                         recovery_entries.append({
#                             "id": write_id,
#                             "type": write_type,
#                             "data": data_dict
#                         })
#                 except Exception as e:
#                     # Skip lines that don't match format (e.g., HTTP request logs)
#                     continue

#         if not recovery_entries:
#             return jsonify({"status": "No matching logs found."}), 404

#     return jsonify(recovery_entries), 200

# def election_loop():
#     global last_heartbeat, CURRENT_TERM, STATE, VOTED_FOR, LEADER, ELECTION_TIMEOUT, last_vote_time
#     while True:
#         time.sleep(1)
#         now = time.monotonic()

#         with lock:
#             time_since_heartbeat = now - last_heartbeat
#             time_since_last_vote = now - last_vote_time
            
#             # Only start election if:
#             # 1. We're not a leader
#             # 2. Heartbeat timeout has passed
#             # 3. Vote timeout has passed
#             timeout_reached = (STATE != "leader" and 
#                               time_since_heartbeat > ELECTION_TIMEOUT and
#                               time_since_last_vote > VOTE_TIMEOUT)

#         if timeout_reached:
#             with lock:
#                 CURRENT_TERM += 1
#                 STATE = "candidate"
#                 VOTED_FOR = NODE_ID  # Vote for self first
#                 LEADER = None
#                 term_started = CURRENT_TERM
#                 votes = 1  # Vote for self first!
#                 last_vote_time = now  # Reset vote timeout when starting election
#                 logging.info(f"[Node {NODE_ID}] Timeout. Starting election for term {CURRENT_TERM}")
#                 last_heartbeat = now
#                 ELECTION_TIMEOUT = random.randint(5, 30)

#             # Create a list to store vote request threads
#             vote_threads = []
#             # Create a shared votes counter that threads can increment
#             vote_counter = {'count': 1}  # Start with 1 (self vote)
#             vote_counter_lock = threading.Lock()
            
#             # Function for vote request worker threads
#             def request_vote(peer, term, node_id, counter, counter_lock):
#                 # Properly declare globals inside the function
#                 global STATE, CURRENT_TERM, LEADER, VOTED_FOR, last_heartbeat
                
#                 # Skip if we're no longer a candidate
#                 with lock:
#                     if STATE != "candidate":
#                         return
                    
#                 try:
#                     resp = requests.post(f"{peer}/vote", json={
#                         "term": term,
#                         "candidate_id": node_id
#                     }, timeout=1)
                    
#                     if resp.status_code == 200:
#                         response_data = resp.json()
                        
#                         # If the peer is already a leader, step down immediately
#                         if response_data.get("is_leader", False):
#                             leader_id = response_data.get("leader_id")
#                             leader_term = response_data.get("leader_term")
                            
#                             with lock:
#                                 if leader_term >= CURRENT_TERM:
#                                     #logging.info(f"[Node {NODE_ID}] Found existing leader {leader_id} for term {leader_term}, stepping down")
#                                     STATE = "follower"
#                                     CURRENT_TERM = leader_term
#                                     LEADER = leader_id
#                                     VOTED_FOR = None
#                                     last_heartbeat = time.monotonic()

                        
#                         # If we got a vote, count it
#                         elif response_data.get("vote_granted"):
#                             with counter_lock:
#                                 counter['count'] += 1
#                                 logging.info(f"[Node {NODE_ID}] Received vote from peer at {peer}, total votes: {counter['count']}")
                            
#                 except requests.RequestException as e:
#                     logging.debug(f"[Node {NODE_ID}] Failed to get vote from {peer}: {e}")
            
#             # Start a thread for each peer vote request
#             for peer in PEERS:
#                 vote_thread = threading.Thread(
#                     target=request_vote,
#                     args=(peer, term_started, NODE_ID, vote_counter, vote_counter_lock),
#                     daemon=True
#                 )
#                 vote_thread.start()
#                 vote_threads.append(vote_thread)
            
#             # Wait for all vote requests to complete (with timeout)
#             vote_collection_timeout = min(3, ELECTION_TIMEOUT / 2)  # Don't wait too long
#             start_time = time.monotonic()
#             for thread in vote_threads:
#                 remaining_time = max(0, vote_collection_timeout - (time.monotonic() - start_time))
#                 thread.join(timeout=remaining_time)
#                 if time.monotonic() - start_time >= vote_collection_timeout:
#                     logging.info(f"[Node {NODE_ID}] Vote collection timed out after {vote_collection_timeout}s")
#                     break

#             # Check for majority
#             with lock:
#                 if STATE == "candidate" and CURRENT_TERM == term_started and vote_counter['count'] > (TOTAL_NODES // 2):
#                     STATE = "leader"
#                     LEADER = NODE_ID
#                     logging.info(f"[Node {NODE_ID}] Won election for term {CURRENT_TERM} with {vote_counter['count']} votes.")

#                     # Announce leadership to all peers
#                     announce_leadership(CURRENT_TERM, NODE_ID)
                    
#                     # Send immediate heartbeats
#                     send_heartbeats(CURRENT_TERM, NODE_ID)
#                 else:
#                     if STATE == "candidate":
#                         logging.info(f"[Node {NODE_ID}] Election failed. Votes: {vote_counter['count']}")
#                         # Return to follower state if election failed
#                         STATE = "follower"

# def announce_leadership(term, leader_id):
#     """Announce leadership to all peers when elected"""
#     for peer in PEERS:
#         try:
#             requests.post(f"{peer}/leader_announcement", json={
#                 "term": term,
#                 "leader_id": leader_id
#             }, timeout=1)
#         except requests.RequestException:
#             pass


# def send_heartbeats(term, leader_id):
#     """Send immediate heartbeats to all peers asynchronously"""
#     heartbeat_threads = []
    
#     def send_single_heartbeat(peer, current_term, current_leader_id):
#         try:
#             requests.post(f"{peer}/heartbeat", json={
#                 "term": current_term,
#                 "leader_id": current_leader_id
#             }, timeout=1)
#         except requests.RequestException as e:
#             logging.debug(f"[Node {NODE_ID}] HEARTBEAT: Failed to send heartbeat to {peer}: {e}")
    
#     # Send multiple rounds of heartbeats in quick succession
#     for _ in range(3):  # Send 3 rounds of heartbeats
#         threads_round = []
#         for peer in PEERS:
#             # Create and start a thread for each peer
#             heartbeat_thread = threading.Thread(
#                 target=send_single_heartbeat,
#                 args=(peer, term, leader_id),
#                 daemon=True
#             )
#             heartbeat_thread.start()
#             threads_round.append(heartbeat_thread)
        
#         # Wait for all threads in this round to complete (with timeout)
#         for thread in threads_round:
#             thread.join(timeout=0.5)
        
#         # Brief pause between rounds
#         time.sleep(0.2)


# def heartbeat_loop():
#     """Periodically send heartbeats to all followers if this node is the leader"""
#     global CURRENT_TERM
#     while True:
#         time.sleep(2)
#         with lock:
#             if STATE == "leader":
#                 current_term = CURRENT_TERM
#                 current_id = NODE_ID
        
#         # Only proceed if we're the leader
#         if STATE == "leader":
#             heartbeat_threads = []
            
#             def send_heartbeat_to_peer(peer, term, leader_id):
#                 try:
#                     requests.post(f"{peer}/heartbeat", json={
#                         "term": term,
#                         "leader_id": leader_id
#                     }, timeout=1)
#                     logging.info(f"[Node {NODE_ID}] HEARTBEAT: Sent heartbeat to {peer}")
#                 except requests.RequestException as e:
#                     logging.debug(f"[Node {NODE_ID}] HEARTBEAT: Failed to send heartbeat to {peer}: {e}")
            
#             # Create and start a thread for each peer
#             for peer in PEERS:
#                 heartbeat_thread = threading.Thread(
#                     target=send_heartbeat_to_peer, 
#                     args=(peer, current_term, current_id),
#                     daemon=True
#                 )
#                 heartbeat_thread.start()
#                 heartbeat_threads.append(heartbeat_thread)
            
#             # Optional: Wait for heartbeats to complete (with timeout)
#             # This prevents heartbeat_loop from sending new heartbeats before previous ones finish
#             for thread in heartbeat_threads:
#                 thread.join(timeout=1.0)  # Don't wait more than 1 second


# def check_follower_status():
#     """Periodically check if followers have timed out"""
#     global FOLLOWER_STATUS, FOLLOWER_LAST_SEEN
    
#     while True:
#         time.sleep(5)  # Check every 5 seconds
        
#         # Only relevant when we're the leader
#         with lock:
#             if STATE != "leader":
#                 continue
                
#             current_time = time.monotonic()
            
#             # Check each follower's status
#             for node_id in range(1, TOTAL_NODES + 1):
#                 if node_id == NODE_ID:  # Skip self
#                     continue
                    
#                 last_seen = FOLLOWER_LAST_SEEN.get(node_id, 0)
#                 time_since_last_seen = current_time - last_seen
                
#                 # Mark as inactive if timeout exceeded
#                 if time_since_last_seen > FOLLOWER_TIMEOUT:
#                     if FOLLOWER_STATUS.get(node_id) != "inactive":
#                         FOLLOWER_STATUS[node_id] = "inactive"
#                         #logging.warning(f"[Node {NODE_ID}] Follower {node_id} is inactive (no response for {time_since_last_seen:.1f}s)")

# def log_recovery():
#     data = []
#     filtered = {}
#     while LEADER is None:
#         time.sleep(0.1)
#     with open(f"{log_dir}/raft_node_{NODE_ID}.log",'r') as f:
#         data = f.readlines()
#     # if not data:
#     #     return
#     logging.info("log recovery started")
#     for line in data:
#         if 'Committed' in line:
#             line = line.split(' ')
#             filtered[line[8]] = line[9]
#     response = {}
#     if not filtered:
#         response = requests.post(f'http://raft_node_{LEADER}:{PORT}/recovery',json={False})
#     else:
#         logging.info(filtered)
#         last = {'type':list(filtered.keys())[-1],'id':filtered[list(filtered.keys())[-1]]}
#         response = requests.post(f'http://raft_node_{LEADER}:{PORT}/recovery',json=last)
#     data_new = response.json()
#     logging.info(f"data_new {data_new}")
#     for entry in data_new:
#         if entry['type'] == 'printer':
#             re = requests.post(f"http://raft_node_{NODE_ID}:{PORT}/printer",json=entry['data'])
#         elif entry['type'] == 'filament':
#             re = requests.post(f"http://raft_node_{NODE_ID}:{PORT}/filament",json=entry['data'])
#         elif entry['type'] == 'print_job':
#             re = requests.post(f"http://raft_node_{NODE_ID}:{PORT}/printjob",json=entry['data'])
#         else:
#             return 


# if __name__ == "__main__":
#     logging.info(f"[Node {NODE_ID}] Starting Raft node on port {PORT}")
#     threading.Thread(target=election_loop, daemon=True).start()
#     threading.Thread(target=heartbeat_loop, daemon=True).start()
#     threading.Thread(target=check_follower_status, daemon=True).start()
#     threading.Thread(target=log_recovery,daemon=True).start()
#     app.run(host='0.0.0.0', port=PORT)
#     # while LEADER is None:
#     #     time.sleep(0.1)
#     # log_recovery()


from flask import Flask, request, jsonify
import threading
import time
import random
import requests
import os
import logging
import ast

# --- Setup Logging ---
node_id_str = os.getenv("NODE_ID", "unknown")
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_filename = f"{log_dir}/raft_node_{node_id_str}.log"

def check_exists(content):
    with open(f"{log_dir}/raft_node_{node_id_str}.log", 'r') as f:
        if content in f.read():
            return True
        return False

# Create a heartbeat filter that will only let heartbeat messages through to console
class HeartbeatFilter(logging.Filter):
    def filter(self, record):
        return "heartbeat" in record.getMessage().lower()

# Create a non-heartbeat filter that will block heartbeat messages
class NonHeartbeatFilter(logging.Filter):
    def filter(self, record):
        return "heartbeat" not in record.getMessage().lower()

# Set up file handler with non-heartbeat filter
file_handler = logging.FileHandler(log_filename)
file_handler.setLevel(logging.INFO)
file_formatter = logging.Formatter(f"%(asctime)s [Node {node_id_str}] %(levelname)s: %(message)s")
file_handler.setFormatter(file_formatter)
file_handler.addFilter(NonHeartbeatFilter())  # Only log non-heartbeat messages to file

# Set up console handler - will show all messages including heartbeats
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter(f"%(asctime)s [Node {node_id_str}] %(levelname)s: %(message)s")
console_handler.setFormatter(console_formatter)

# Configure the root logger without using basicConfig
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(file_handler)
logger.addHandler(console_handler)

import firebase_admin
from firebase_admin import db,storage,credentials

DBURL = os.getenv("DBURL")

#firebase connection
cred = credentials.Certificate("credentials.json")
firebase_admin.initialize_app(cred,{'databaseURL':DBURL}) 

#test if the db connection is working
if not check_exists("DB connection established"):
    logging.info("DB connection established")
     

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

if not check_exists(f"[Node {NODE_ID}] Peers: {PEERS}"):
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
            #logging.info(f"[Node {NODE_ID}] Rejecting vote request from {candidate_id} - I am already leader for term {CURRENT_TERM}")
            return jsonify({
                'vote_granted': False,
                'is_leader': True,
                'leader_id': NODE_ID,
                'leader_term': CURRENT_TERM
            })

        # Check if we're in the vote timeout period
        current_time = time.monotonic()
        if current_time - last_vote_time < VOTE_TIMEOUT and VOTED_FOR is not None:
            #logging.info(f"[Node {NODE_ID}] Rejecting vote request from {candidate_id} - in voting timeout period ({current_time - last_vote_time:.1f}s < {VOTE_TIMEOUT}s)")
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

            #logging.info(f"[Node {NODE_ID}] Voted for {candidate_id} in term {term}")
            return jsonify({
                'vote_granted': True,
                'is_leader': False
            })

        else:
            # # We've already voted in this term
            # if VOTED_FOR == candidate_id:
            #     logging.info(f"[Node {NODE_ID}] Already voted for {candidate_id} in term {term}")
            # else:
            #     logging.info(f"[Node {NODE_ID}] Rejecting vote for {candidate_id}, already voted for {VOTED_FOR} in term {CURRENT_TERM}")
            
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
            #logging.info(f"[Node {NODE_ID}] HEARTBEAT: Received heartbeat from Leader {leader_id} (term {term})")

        # Send acknowledgment back to leader
        try:
            leader_url = f"http://raft_node_{leader_id}:{PORT}/heartbeat_response"
            requests.post(leader_url, json={
                "follower_id": NODE_ID,
                "term": term
            }, timeout=1)
        except requests.RequestException as e:
            print(f"[Node {NODE_ID}] HEARTBEAT: Failed to send heartbeat response to leader {leader_id}: {e}")

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
        # if was_inactive:
        #     logging.info(f"[Node {NODE_ID}] Follower {follower_id} has recovered and is now active")
        # else:
        #     logging.info(f"[Node {NODE_ID}] HEARTBEAT: Received heartbeat response from follower {follower_id}")
            
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
            #logging.info(f"[Node {NODE_ID}] Received leader announcement from {leader_id} for term {term}")
            return jsonify({'success': True})
        else:
            return jsonify({'success': False})


@app.route('/node_status', methods=['GET'])
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
    
@app.route('/printer', methods=['POST'])
def create_printer():
    data = request.get_json()
    printer_id = str(data.get("id"))

    if not printer_id:
        return jsonify({"error": "Printer ID is required"}), 400

    try:
        db.reference(f"printers/{printer_id}").set({
            "id": printer_id,
            "company": data.get("company"),
            "model": data.get("model")
        })
        #logging.info(f"[Node {NODE_ID}] Created printer {printer_id}")
        return jsonify({"success": True}), 201
    except Exception as e:
        #logging.error(f"[Node {NODE_ID}] Failed to create printer: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/filament', methods=['POST'])
def create_filament():
    data = request.get_json()
    filament_id = str(data.get("id"))

    if not filament_id:
        return jsonify({"error": "Filament ID is required"}), 400

    try:
        db.reference(f"filaments/{filament_id}").set({
            "id": filament_id,
            "type": data.get("type"),
            "color": data.get("color"),
            "total_weight_in_grams": data.get("total_weight_in_grams"),
            "remaining_weight_in_grams": data.get("remaining_weight_in_grams")
        })
        #logging.info(f"[Node {NODE_ID}] Created filament {filament_id}")
        return jsonify({"success": True}), 201
    except Exception as e:
        #logging.error(f"[Node {NODE_ID}] Failed to create filament: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/printjob', methods=['POST'])
def create_print_job():
    data = request.get_json()
    job_id = str(data.get("id"))

    if not job_id:
        return jsonify({"error": "Job ID is required"}), 400

    try:
        db.reference(f"print_jobs/{job_id}").set({
            "id": job_id,
            "printer_id": data.get("printer_id"),
            "filament_id": data.get("filament_id"),
            "filepath": data.get("filepath"),
            "print_weight_in_grams": data.get("print_weight_in_grams"),
            "status": data.get("status")
        })
        #logging.info(f"[Node {NODE_ID}] Created print job {job_id}")
        return jsonify({"success": True}), 201
    except Exception as e:
        #logging.error(f"[Node {NODE_ID}] Failed to create print job: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/printer/<printer_id>', methods=['GET'])
def get_printer(printer_id):
    try:
        printer_data = db.reference(f"printers/{printer_id}").get()
        if printer_data:
            return jsonify(printer_data)
        else:
            return jsonify({"error": "Printer not found"}), 404
    except Exception as e:
        #logging.error(f"[Node {NODE_ID}] Failed to fetch printer: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/filament/<filament_id>', methods=['GET'])
def get_filament(filament_id):
    try:
        filament_data = db.reference(f"filaments/{filament_id}").get()
        if filament_data:
            return jsonify(filament_data)
        else:
            return jsonify({"error": "Filament not found"}), 404
    except Exception as e:
        #logging.error(f"[Node {NODE_ID}] Failed to fetch filament: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/printjob/<job_id>', methods=['GET'])
def get_print_job(job_id):
    try:
        job_data = db.reference(f"print_jobs/{job_id}").get()
        if job_data:
            return jsonify(job_data)
        else:
            return jsonify({"error": "Print job not found"}), 404
    except Exception as e:
        #logging.error(f"[Node {NODE_ID}] Failed to retrieve print job: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/status', methods=['POST'])
def update_print_job_status_post():
    data = request.get_json()
    job_id = str(data.get("job_id"))
    status = data.get("status")

    if not job_id or not status:
        return jsonify({"error": "job_id and status are required"}), 400

    job_ref = db.reference(f"print_jobs/{job_id}")
    if not job_ref.get():
        return jsonify({"error": "Print job not found"}), 404

    try:
        job_ref.update({"status": status})
        #logging.info(f"[Node {NODE_ID}] Updated print job {job_id} status to {status}")
        return jsonify({"success": True})
    except Exception as e:
        #logging.error(f"[Node {NODE_ID}] Failed to update print job status: {e}")
        return jsonify({"error": str(e)}), 500


#CONSENSUS

@app.route('/initiate_write', methods=['POST'])
def initiate_write():
    if STATE != "leader":
        return jsonify({"error": "Only leader can initiate write"}), 403

    payload = request.get_json()
    write_type = payload.get("type")
    data = payload.get("data")
    write_id = str(data.get("id"))

    if not write_type or not data or not write_id:
        return jsonify({"error": "Invalid request"}), 400

    # Send write_ready to all followers asynchronously
    ack_count = 1  # self
    ack_lock = threading.Lock()
    write_threads = []

    def send_ready_request(peer_url):
        nonlocal ack_count
        try:
            res = requests.post(f"{peer_url}/ready_check", json={"type": write_type, "id": write_id}, timeout=1)
            if res.status_code == 200 and res.json().get("status") == "ready":
                with ack_lock:
                    ack_count += 1
        except Exception as e:
            print(f"[Node {NODE_ID}] Failed to get ready from {peer_url}: {e}")    #logging.warning

    for peer in PEERS:
        thread = threading.Thread(target=send_ready_request, args=(peer,), daemon=True)
        thread.start()
        write_threads.append(thread)

    # Wait for replies
    for t in write_threads:
        t.join(timeout=1)

    # Check quorum
    if ack_count > TOTAL_NODES // 2:
        logging.info(f"[Node {NODE_ID}] Got majority ready ({ack_count}), committing write {write_id}, type: {write_type}, data: {data}")
        
        # Commit locally using Firebase
        db.reference(f"{write_type}s/{write_id}").set(data)

        # Notify all followers to commit
        def send_commit(peer_url):
            try:
                requests.post(f"{peer_url}/commit_write", json={
                    "type": write_type,
                    "data": data
                }, timeout=1)
            except Exception as e:
                print(f"[Node {NODE_ID}] Failed to send commit to {peer_url}: {e}")   #logging.warning

        for peer in PEERS:
            threading.Thread(target=send_commit, args=(peer,), daemon=True).start()

        return jsonify({"success": True, "committed": True}), 200
    else:
        logging.warning(f"[Node {NODE_ID}] Not enough ready responses ({ack_count}), aborting write")
        return jsonify({"success": False, "committed": False, "acks": ack_count}), 500

@app.route('/ready_check', methods=['POST'])
def ready_check():
    data = request.get_json()
    #logging.info(f"[Node {NODE_ID}] Received ready check for write: {data}")
    return jsonify({"status": "ready"}), 200


@app.route('/commit_write', methods=['POST'])
def commit_write():
    data = request.get_json()
    write_type = data.get("type")
    obj_data = data.get("data")
    obj_id = str(obj_data.get("id"))

    if not write_type or not obj_id or not obj_data:
        return jsonify({"error": "Invalid commit data"}), 400

    try:
        db.reference(f"{write_type}s/{obj_id}").set(obj_data)
        logging.info(f"[Node {NODE_ID}] Committed {write_type} {obj_id} from leader")
        return jsonify({"success": True}), 200
    except Exception as e:
        logging.error(f"[Node {NODE_ID}] Failed to commit {write_type}: {e}")
        return jsonify({"error": str(e)}), 500



@app.route('/recovery', methods=['POST', 'GET'])
def recovery():
    recovery_entries = []
    last = request.get_json()

    with open(f"{log_dir}/raft_node_{NODE_ID}.log", 'r') as f:
        lines = f.readlines()

    if False in last.values():
        for line in lines:
            try:
                if 'committing write' in line:
                    # Old format: committing write 24, type: printer, data: {...}
                    write_id = line.split('committing write')[1].split(',')[0].strip()
                    write_type = line.split('type:')[1].split(',')[0].strip()
                    data_str = line.split('data:')[1].strip()
                    data_dict = ast.literal_eval(data_str)
                    recovery_entries.append({
                        "id": write_id,
                        "type": write_type,
                        "data": data_dict
                    })
                elif 'Committed' in line and 'from leader' in line:
                    # New format: [Node 5] Committed printer 42 from leader
                    parts = line.strip().split()
                    write_type = parts[3]
                    write_id = parts[4]
                    # For this format, we don’t have `data`, so we only send ID/type
                    recovery_entries.append({
                        "id": write_id,
                        "type": write_type,
                        "data": {}  # or None, depends on how you want to handle missing data
                    })
            except Exception:
                continue

    else:
        w_type = last['type']
        w_id = last['id']
        matched = False

        for line in lines:
            try:
                if 'committing write' in line:
                    write_id = line.split('committing write')[1].split(',')[0].strip()
                    write_type = line.split('type:')[1].split(',')[0].strip()
                    data_str = line.split('data:')[1].strip()
                    data_dict = ast.literal_eval(data_str)

                    if not matched:
                        if write_id == str(w_id) and write_type == w_type:
                            matched = True
                            recovery_entries.append({
                                "id": write_id,
                                "type": write_type,
                                "data": data_dict
                            })
                    else:
                        recovery_entries.append({
                            "id": write_id,
                            "type": write_type,
                            "data": data_dict
                        })

                elif 'Committed' in line and 'from leader' in line:
                    parts = line.strip().split()
                    write_type = parts[3]
                    write_id = parts[4]

                    if not matched:
                        if write_id == str(w_id) and write_type == w_type:
                            matched = True
                            recovery_entries.append({
                                "id": write_id,
                                "type": write_type,
                                "data": {}
                            })
                    else:
                        recovery_entries.append({
                            "id": write_id,
                            "type": write_type,
                            "data": {}
                        })
            except Exception:
                continue

        if not recovery_entries:
            return jsonify({"status": "No matching logs found."}), 404

    return jsonify(recovery_entries), 200


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
                                    #logging.info(f"[Node {NODE_ID}] Found existing leader {leader_id} for term {leader_term}, stepping down")
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
            logging.debug(f"[Node {NODE_ID}] HEARTBEAT: Failed to send heartbeat to {peer}: {e}")
    
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
                    logging.info(f"[Node {NODE_ID}] HEARTBEAT: Sent heartbeat to {peer}")
                except requests.RequestException as e:
                    logging.debug(f"[Node {NODE_ID}] HEARTBEAT: Failed to send heartbeat to {peer}: {e}")
            
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
                        #logging.warning(f"[Node {NODE_ID}] Follower {node_id} is inactive (no response for {time_since_last_seen:.1f}s)")

def log_recovery():
    data = []
    filtered = {}
    while LEADER is None:
        time.sleep(0.1)
    with open(f"{log_dir}/raft_node_{NODE_ID}.log",'r') as f:
        data = f.readlines()
    # if not data:
    #     return
    logging.info("log recovery started")
    for line in data:
        if 'Committed' in line:
            line = line.split(' ')
            filtered[line[8]] = line[9]
    response = {}
    if not filtered:
        response = requests.post(f'http://raft_node_{LEADER}:{PORT}/recovery',json={False})
    else:
        logging.info(filtered)
        last = {'type':list(filtered.keys())[-1],'id':filtered[list(filtered.keys())[-1]]}
        response = requests.post(f'http://raft_node_{LEADER}:{PORT}/recovery',json=last)
    data_new = response.json()
    #logging.info(f"data_new {data_new}")
    for entry in data_new:
        logging.info(f"[Node {NODE_ID}] Committed {entry['type']} {entry['id']} from leader")
        if entry['type'] == 'printer':
            re = requests.post(f"http://raft_node_{NODE_ID}:{PORT}/printer",json=entry['data'])
        elif entry['type'] == 'filament':
            re = requests.post(f"http://raft_node_{NODE_ID}:{PORT}/filament",json=entry['data'])
        elif entry['type'] == 'print_job':
            re = requests.post(f"http://raft_node_{NODE_ID}:{PORT}/printjob",json=entry['data'])
        else:
            return 


if __name__ == "__main__":
    logging.info(f"[Node {NODE_ID}] Starting Raft node on port {PORT}")
    threading.Thread(target=election_loop, daemon=True).start()
    threading.Thread(target=heartbeat_loop, daemon=True).start()
    threading.Thread(target=check_follower_status, daemon=True).start()
    threading.Thread(target=log_recovery,daemon=True).start()
    app.run(host='0.0.0.0', port=PORT)
    # while LEADER is None:
    #     time.sleep(0.1)
    # log_recovery()
