# Fault-Tolerant Distributed File Storage System

This project implements a fault-tolerant distributed file storage system in Python using FastAPI for inter-node communication.

It demonstrates the four fundamental distributed systems principles:
- Fault Tolerance
- Data Replication & Strong Consistency
- Time Synchronization
- Consensus (Raft Algorithm)

Each node runs as an independent FastAPI service and communicates via REST APIs.

---

# Team Members

| Registration No | Name |
|----------------|------|
| IT24104152 | Gayuth Waidyaratne |
| IT24410789 | Shaajeev Balarakshan |
| IT24610797 | Rithish Kaanth |
| IT24104317 | Naveen Jayawardane |

---

# Project Setup

## 1. Create Virtual Environment

### Linux / macOS
```bash
python3 -m venv venv
source venv/bin/activate
```

### Windows
```bash
python -m venv venv
venv\Scripts\activate
```

---

## 2. Install Dependencies

```bash
pip install -r requirements.txt
```

Example `requirements.txt`:
```
fastapi
uvicorn
requests
streamlit
```

---

# How to Run the System

The system runs multiple nodes (distributed servers). Each node runs on a different port.

---

## 3. Run a Single Node (Manual Mode)

```bash
python main.py --mode server --node-id node1 --port 8001
```

Additional nodes:

```bash
python main.py --mode server --node-id node2 --port 8002
python main.py --mode server --node-id node3 --port 8003
python main.py --mode server --node-id node4 --port 8004
python main.py --mode server --node-id node5 --port 8005
```

---

## 4. Run Full Cluster (Demo Mode)

```bash
python main.py --mode demo
```

This will:
- Start all nodes automatically
- Trigger Raft leader election
- Enable heartbeat monitoring
- Activate replication and synchronization

---

## 5. Run FastAPI with Uvicorn (Alternative)

```bash
uvicorn main:app --host 127.0.0.1 --port 8001
```

Other nodes:

```bash
uvicorn main:app --host 127.0.0.1 --port 8002
uvicorn main:app --host 127.0.0.1 --port 8003
uvicorn main:app --host 127.0.0.1 --port 8004
uvicorn main:app --host 127.0.0.1 --port 8005
```

---

# Client Interface (CLI)

Run the CLI client after starting the cluster:

```bash
python main.py --mode client
```

---

## Client Commands

| Command | Description |
|--------|------------|
| write <file> <data> | Write data to a file |
| read <file> | Read file content |
| delete <file> | Delete a file |
| mkdir <dir> | Create directory |
| list | List files in directory |
| status | Show cluster status |
| exit | Exit client |

---

# Client Interface (Streamlit UI)

The system also provides a Streamlit-based web interface for interacting with the cluster.

---

## Run Streamlit UI

Make sure the backend cluster is running first.

```bash
streamlit run app.py
```

or

```bash
streamlit run ui.py
```

---

## Streamlit Features

- Upload files to distributed storage
- Download and view files
- Delete files
- View cluster status and node health
- Perform read/write operations via UI
- Real-time interaction with backend system

---

## Streamlit Workflow

- UI communicates with FastAPI nodes via REST APIs
- Write operations are routed to leader node
- Read operations can be served by any healthy node

---

# System Architecture

## Metadata Layer (Raft Consensus)
- Leader election
- Log replication
- Majority commit (⌊N/2⌋ + 1)
- Strong consistency

---

## Data Layer
- Block-based storage system
- Replication Factor = 3
- Quorum-based reads and writes (R = 2, W = 2)
- Read repair mechanism

---

## Fault Tolerance Layer
- Heartbeat monitoring
- Leader re-election
- Node recovery and log replay
- Data re-replication

---

## Time Synchronization Layer
- Logical clocks
- NTP-inspired synchronization
- Clock drift correction

---

# Key Features

- No single point of failure
- Raft-based consensus
- Automatic leader election
- Fault detection and recovery
- Distributed replication
- Concurrent read/write support
- FastAPI microservices architecture
- Streamlit UI interface
- Scalable multi-node cluster

---

# Testing Scenarios

- Leader failure recovery
- Follower node failure
- Multiple node failure (minority tolerance)
- Node restart and recovery
- Concurrent client requests
- Clock skew simulation

---

# Scalability

- Add nodes by assigning new ports
- Supports multi-machine deployment
- Maintains correctness under quorum

---

# Assumptions and Limitations

- Requires majority of nodes to remain available
- Does not tolerate full network partition split
- Academic/prototype-level implementation
- Single cluster design

---

# Conclusion

This project demonstrates a complete distributed system implementing fault tolerance, replication, consensus, and time synchronization using FastAPI and Streamlit-based interfaces.
