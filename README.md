## Overview

This project implements a **fault-tolerant distributed file storage system** in Python using FastAPI for inter-node communication.

It demonstrates the four fundamental distributed systems principles required in the assignment:

1. **Fault Tolerance**
2. **Data Replication & Strong Consistency**
3. **Time Synchronization**
4. **Consensus & Agreement (Raft Algorithm)**

The system is designed with **no single point of failure**, supports concurrent operations, and maintains strong consistency across multiple distributed servers.

Each node runs as an independent FastAPI service and communicates via REST APIs.

---

## Team Members

| Registration No | Name                 |
| --------------- | -------------------- |
| IT24104152      | Gayuth Waidyaratne   |
| IT24410789      | Shaajeev Balarakshan |
| IT24610797      | Rithish Kaanth       |
| IT24104317      | Naveen Jayawardane   |

---

# System Architecture

The system consists of five major components:

### Metadata Layer (Consensus Layer)

Implements the **Raft** consensus algorithm to ensure agreement among nodes.

* Leader election
* Log replication
* Majority commit (⌊N/2⌋ + 1)
* Term-based voting
* Strong consistency guarantees

All metadata updates are coordinated through Raft before being committed.

---

### Data Layer

* Block-based file storage
* Replication factor (RF = 3)
* Quorum-based writes (W = 2)
* Quorum-based reads (R = 2)
* Read repair mechanism
* Checksum validation

Strong consistency is ensured because: R + W > N

---

### Fault Tolerance Layer

* Heartbeat-based failure detection
* Automatic leader re-election
* Re-replication of lost blocks
* Recovery and state synchronization on restart
* Log replay to catch up recovering nodes

---

### Time Synchronization Layer

* NTP-like synchronization mechanism
* Internal fallback synchronization
* Logical clocks for event ordering
* Clock skew detection and correction

Time synchronization ensures correct timeout handling, ordered event processing, and reliable leader election.

---

### Client Interface

* CLI-based distributed file client
* Supports file and directory operations
* Communicates with cluster via REST APIs exposed by FastAPI servers

---

# Key Features

* No Single Point of Failure
* Strong Consistency (R + W > N)
* Automatic Leader Election
* Log Replication & Majority Commit
* Node Failure Detection & Recovery
* Block Replication Across Nodes
* Time Synchronization Across All Nodes
* Concurrent Read/Write Support
* Microservices-based Architecture using FastAPI
* Scalable Cluster Design

---

# Distributed Systems Properties Demonstrated

---

## Fault Tolerance

The system tolerates:

* Failure of minority nodes
* Leader node crash
* Temporary network partitions
* Node restarts

Mechanisms used:

* Heartbeats for failure detection
* Majority-based consensus
* Automatic re-election
* Data re-replication
* State catch-up on recovery

---

## Data Replication & Strong Consistency

The system implements a **replicated state machine model** using Raft.

* All writes go through the elected leader
* Log entries replicated to followers
* Entries committed only after majority acknowledgement
* R + W > N ensures strong consistency
* Log-based versioning prevents inconsistencies

Guarantee:

> Once a write is committed, it will never be lost.

---

## Time Synchronization

Each node:

* Periodically synchronizes its clock
* Detects clock drift
* Adjusts local time offset
* Uses logical clocks for ordering distributed events

Impact:

* Correct ordering of log entries
* Accurate timeout handling
* Reliable leader election timing

---

## Consensus & Agreement

The system uses the Raft consensus algorithm to ensure agreement among nodes.

* Only one leader at a time
* Safe log replication
* No split-brain scenario
* Durable committed state

Handles:

* Leader failure
* Follower failure
* Network delays
* Node recovery

---

# Requirements

* Python 3.7+
* FastAPI
* Uvicorn (ASGI server)
* Runs on Linux / macOS / Windows

Install dependencies:

```
pip install -r requirements.txt
```

Example requirements.txt:

```
fastapi
uvicorn
requests
```

---

# How to Run

---

## Demo Mode (Recommended)

Start full 5-node cluster automatically:

```
python main.py --mode demo
```

This will:

* Start 5 FastAPI-based nodes
* Elect a leader
* Perform file writes
* Simulate node failure
* Recover failed node
* Demonstrate replication and consensus

---

## Run a Node Manually

```
uvicorn main:app --host 127.0.0.1 --port 9000
```

Or using your main entry:

```
python main.py --mode server --node-id <id> --nodes <total_nodes> --base-port 9000
```

Example:

```
python main.py --mode server --node-id 0 --nodes 5 --base-port 9000
```

---

## Run Client

```
python main.py --mode client --nodes 5 --base-port 9000
```

---

# Client Commands

| Command            | Description             |
| ------------------ | ----------------------- |
| write <file>       | Write content to file   |
| read <file>        | Read file               |
| delete <file>      | Delete file             |
| mkdir <dir>        | Create directory        |
| list               | List directory contents |
| rename <old> <new> | Rename file             |
| exit               | Exit client             |

---

# Testing Scenarios

The system has been tested under:

* Leader node failure
* Follower node failure
* Multiple node failures (minority)
* Node restart and state recovery
* Concurrent client writes
* Simulated clock skew
* Network delay simulation

---

# Evaluation Alignment

| Criteria                  | Implementation                             |
| ------------------------- | ------------------------------------------ |
| Fault Tolerance           | Replication + Failure detection + Recovery |
| Replication & Consistency | Raft + Majority commit                     |
| Time Synchronization      | NTP-like + Logical clocks                  |
| Consensus & Agreement     | Full Raft implementation                   |
| Integration               | Modular, scalable layered design           |

---

# Scalability

The system can scale by:

* Adding more data nodes for storage capacity
* Increasing cluster size for higher fault tolerance
* Deploying nodes across multiple machines

Consensus safety is maintained as long as a majority of nodes remain available.

---

# Assumptions & Limitations

* Assumes reliable majority network connectivity
* Does not tolerate majority network partition
* Optimized for academic demonstration rather than production throughput
* Single cluster implementation

---

# Conclusion

This distributed file storage system demonstrates strong consistency, consensus-based coordination, fault tolerance, and time synchronization using a FastAPI-based distributed architecture.
