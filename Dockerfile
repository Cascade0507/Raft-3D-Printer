FROM python:3.9-slim

WORKDIR /app

COPY raft_node.py .

RUN pip install flask requests

ENTRYPOINT ["python", "raft_node.py"]
