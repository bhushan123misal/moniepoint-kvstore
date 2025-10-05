# 🗝️ KVStore — Distributed Persistent Key–Value Store (Raft + Python)

A minimal, production-style **distributed key-value database** implemented in pure Python, with
- Persistent on-disk storage (WAL + snapshots)
- Raft-based leader election and log replication
- HTTP API for clients and inter-node communication
- Docker-compose cluster setup (3 nodes, automatic leader election)
- Works cross-platform (macOS, Linux, Windows via Docker)

---

# 🧑‍💻 Developer Setup — KVStore (Raft-based Key-Value Store)

This guide explains how to **run and develop KVStore locally** on your machine, without Docker,
and how to debug, test, and extend the system.

---

## 🧩 Prerequisites

| Tool | Version | Purpose |
|------|----------|----------|
| **Python** | 3.11+ | main runtime |
| **pip** | latest | package manager |
| **curl / jq** | optional | test REST API |
| **(optional)** Docker + Docker Compose | for full cluster testing |

---

## 1. Clone and set up the project

```bash
git clone https://github.com/yourname/kvstore.git
cd kvstore
python -m venv venv
source venv/bin/activate      # (on Windows: venv\Scripts\activate)
```

## 2. Run Tests
```bash
./tests/run_tests.sh
```

## 2. Test all Run a single node (standalone mode)
```bash
python main.py \
  --data-dir ./data-n1 \
  --host 127.0.0.1 \
  --port 7070 \
  --node-id n1 \
  --cluster-id local-dev \
  --peers "n1=http://127.0.0.1:7070"

# Test the node
curl http://127.0.0.1:7070/raft/whois
curl -X PUT http://127.0.0.1:7070/kv/user:1 --data-binary "hello"
curl http://127.0.0.1:7070/kv/user:1
```
## 3. Run a local 3-node cluster
```bash
python main.py --data-dir ./data-n1 --host 127.0.0.1 --port 7070 --node-id n1 --cluster-id kv-demo-1 --peers "n1=http://127.0.0.1:7070,n2=http://127.0.0.1:7071,n3=http://127.0.0.1:7072"

python main.py --data-dir ./data-n2 --host 127.0.0.1 --port 7071 --node-id n2 --cluster-id kv-demo-1 --peers "n1=http://127.0.0.1:7070,n2=http://127.0.0.1:7071,n3=http://127.0.0.1:7072"

python main.py --data-dir ./data-n3 --host 127.0.0.1 --port 7072 --node-id n3 --cluster-id kv-demo-1 --peers "n1=http://127.0.0.1:7070,n2=http://127.0.0.1:7071,n3=http://127.0.0.1:7072"
```
Alternatively, you can run the cluster with docker-compose (recommended)
```
docker compose build
docker compose up -d
```

## 4. Find Leader
Run this after ~3-5 seconds of running the cluster
```bash
curl -s http://127.0.0.1:7070/raft/whois
curl -s http://127.0.0.1:7071/raft/whois
curl -s http://127.0.0.1:7072/raft/whois
```
You will get the response like:
```
{ "role": "leader", "leaderAddr": "http://127.0.0.1:7071" }
```
Alternatively, you can run `curl http://127.0.0.1:7070/cluster/leader`and `curl http://127.0.0.1:7070/cluster/nodes` to find the leader and follower nodes

## 5. Testing
You can run a test cluster with `--data-dir test_dir` and run the commands in `tests/tests.txt` against the leader node (Sorry, I didn't get time to automate testing)

# 🗝️ API Endpoints
## 1. Find leader
```
GET /cluster/leader
```
Response
```
{
    "clusterId": "kv-demo-1",
    "leaderAddr": "http://n3:7072",
    "leaderId": "n3",
    "term": 2888,
    "source": "http://0.0.0.0:7070"
}
```

## 2. Find All Nodes
```
GET /cluster/nodes
```
Response
```
{
    "clusterId": "kv-demo-1",
    "nodes": [
        {
            "up": true,
            "nodeId": "n1",
            "addr": "http://0.0.0.0:7070",
            "role": "follower",
            "term": 2990,
            "leaderId": "n3",
            "leaderAddr": "http://n3:7072",
            "commitIndex": 0
        },
        {
            "up": true,
            "nodeId": "n2",
            "addr": "http://0.0.0.0:7071",
            "role": "follower",
            "term": 2990,
            "leaderId": "n3",
            "leaderAddr": "http://n3:7072",
            "commitIndex": 0
        },
        {
            "up": true,
            "nodeId": "n3",
            "addr": "http://0.0.0.0:7072",
            "role": "leader",
            "term": 2990,
            "leaderId": "n3",
            "leaderAddr": "http://n3:7072",
            "commitIndex": 0
        }
    ]
}
```

## 3. Find config for individual node
```
GET /raft/whois
```
Response
```
{
    "role": "follower",
    "term": 3086,
    "leaderId": "n3",
    "leaderAddr": "http://n3:7072"
}
```

## 4. Add new key (or update the key)
```
PUT /kv/<key>
```
Request Body (raw text)
```
<value corresponding to the text>
```

## 5. Get value for a key
```
GET /kv/<key>
```
Response
```
<value corresponding to the text>
```

## 6. Get all key values within a specific alphabetical ranget
```
GET /kv/range?start=<start_value>&end=<end_value>&limit=10
```
Response
```
{
    "k": "test",
    "v": "Bhushan"
}
{
    "k": "test2",
    "v": "HELLO"
}
```

## 7. Delete a key
```
DELETE /kv/<key>
```

## 7. Add keys in batch
```
POST /kv/batch
```
Request Body (raw text)
```
PUT <key1> <value1>
PUT <key2> <value2>
PUT <key3> <value3>
```