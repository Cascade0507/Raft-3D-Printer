from flask import Flask, request, jsonify
from uuid import uuid4
from pyraft import RaftNode

app = Flask(__name__)

# Simulated Raft node (stub for PyRaft integration)
raft_node = RaftNode("node1", peers=[])

# In-memory storage
printers = {}
filaments = {}
print_jobs = {}

# Endpoint to create a printer
@app.route('/api/v1/printers', methods=['POST'])
def create_printer():
    data = request.json
    printer_id = str(uuid4())
    printers[printer_id] = data
    return jsonify({"id": printer_id, "data": data}), 201

# List all printers
@app.route('/api/v1/printers', methods=['GET'])
def list_printers():
    return jsonify(printers)

# Create a filament
@app.route('/api/v1/filaments', methods=['POST'])
def create_filament():
    data = request.json
    filament_id = str(uuid4())
    data['remaining_weight_in_grams'] = data['total_weight_in_grams']
    filaments[filament_id] = data
    return jsonify({"id": filament_id, "data": data}), 201

# List all filaments
@app.route('/api/v1/filaments', methods=['GET'])
def list_filaments():
    return jsonify(filaments)

# Create a print job
@app.route('/api/v1/print_jobs', methods=['POST'])
def create_print_job():
    data = request.json
    printer_id = data.get('printer_id')
    filament_id = data.get('filament_id')
    weight = data.get('print_weight_in_grams')

    if printer_id not in printers or filament_id not in filaments:
        return jsonify({"error": "Invalid printer_id or filament_id"}), 400

    # Calculate used weight in queued/running jobs using same filament
    used_weight = sum(job['print_weight_in_grams']
                      for job in print_jobs.values()
                      if job['filament_id'] == filament_id and job['status'] in ['queued', 'running'])

    available = filaments[filament_id]['remaining_weight_in_grams'] - used_weight
    if weight > available:
        return jsonify({"error": "Not enough filament available"}), 400

    job_id = str(uuid4())
    print_jobs[job_id] = {
        **data,
        'status': 'queued'
    }
    return jsonify({"id": job_id, "data": print_jobs[job_id]}), 201

# List print jobs (with optional filtering)
@app.route('/api/v1/print_jobs', methods=['GET'])
def list_print_jobs():
    status_filter = request.args.get('status')
    if status_filter:
        filtered = {k: v for k, v in print_jobs.items() if v['status'] == status_filter}
        return jsonify(filtered)
    return jsonify(print_jobs)

# Update print job status
@app.route('/api/v1/print_jobs/<job_id>/status', methods=['POST'])
def update_job_status(job_id):
    if job_id not in print_jobs:
        return jsonify({"error": "Print job not found"}), 404

    new_status = request.args.get('status')
    job = print_jobs[job_id]
    current_status = job['status']

    valid_transitions = {
        'queued': ['running', 'canceled'],
        'running': ['done', 'canceled'],
    }

    if current_status not in valid_transitions or new_status not in valid_transitions[current_status]:
        return jsonify({"error": "Invalid status transition"}), 400

    job['status'] = new_status

    # Update filament weight on 'done'
    if new_status == 'done':
        filament_id = job['filament_id']
        filaments[filament_id]['remaining_weight_in_grams'] -= job['print_weight_in_grams']

    return jsonify({"id": job_id, "data": job})

if __name__ == '__main__':
    app.run(debug=True)
