FROM python:3.9-slim

WORKDIR /app

COPY raft_node.py .
COPY credentials.json .
COPY node_status.json .
COPY client.py .


RUN pip install flask requests firebase_admin

ENTRYPOINT ["python", "raft_node.py"]
