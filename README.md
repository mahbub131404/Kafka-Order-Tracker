How to run everything (step-by-step)

Kafka Docker – make sure your kafka container is running as before:
docker ps


If not running:
docker compose up -d


Install Python packages (inside your .venv):

cd C:\Users\mahbub404\Desktop\StreamStore
py -m venv .venv
& .\.venv\Scripts\python.exe -m pip install --upgrade pip
& .\.venv\Scripts\python.exe -m pip install flask confluent-kafka

Run tracker worker (terminal 1):
& .\.venv\Scripts\python.exe tracker_worker.py

Run delivery worker (terminal 2):
& .\.venv\Scripts\python.exe delivery_worker.py

Run Flask website (terminal 3):
& .\.venv\Scripts\python.exe app.py


Open browser at:

http://127.0.0.1:5000
