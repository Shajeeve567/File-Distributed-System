# main.py - Integrated
from fastapi import FastAPI, UploadFile, File, Response, Request, HTTPException
import uvicorn
import asyncio
import httpx
import time
import os
import logging
import requests
import json
import argparse

from features.replication.manager import ReplicationManager
from features.fault_tolerance.recovery import RecoveryManager
from features.fault_tolerance.manager import FaultToleranceManager
from shared.config import config
from features.replication.storage import StorageManager
from features.fault_tolerance.registry import NodeRegistry
from shared.utils import logger, generate_block_id, format_size
from shared.models import LogEntry, FileMetadata

# Features
from features.consensus import consensus_impl
from features.time_sync import monitor

# Components will be initialized in lifespan
storage = None
registry = None
replication = None
recovery = None
fault_tolerance = None
time_monitor = monitor.TimeSyncMonitor()

from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown logic using FastAPI lifespan."""
    global storage, registry, replication, recovery, fault_tolerance
    
    # STARTUP
    logger.info(f"Starting node {config.NODE_ID} on port {config.PORT}")
    
    # Ensure local directory exists for this specific node
    os.makedirs(config.BLOCKS_DIR, exist_ok=True)
    
    # Initialize components with the correct NODE_ID
    storage = StorageManager()
    registry = NodeRegistry(config.NODE_ID)
    replication = ReplicationManager(config.NODE_ID, storage)
    recovery = RecoveryManager(config.NODE_ID, storage)
    fault_tolerance = FaultToleranceManager(config.NODE_ID, storage, registry, replication, recovery)
    
    from features.time_sync.logical_clocks import LamportClock
    app.state.lamport_clock = LamportClock()
    
    app.state.storage = storage
    app.state.registry = registry
    app.state.replication = replication
    app.state.recovery = recovery
    app.state.fault_tolerance = fault_tolerance

    # Initialize Raft node (Task 4)
    peer_ids = list(config.OTHER_NODES.keys())
    await consensus_impl.init_raft_node(config.NODE_ID, peer_ids)
    
    # Start background tasks
    h_task = asyncio.create_task(heartbeat_sender())
    r_task = asyncio.create_task(replication.start())
    rec_task = asyncio.create_task(recovery.start())
    f_task = asyncio.create_task(fault_tolerance.start())
    # NEW: Event-driven metadata replication (Task 4)
    async def raft_commit_listener(entry):
        if entry.op == "CREATE_FILE":
            filename = entry.file_id
            manifest = entry.payload
            await storage.save_metadata(f"manifest_{filename}", manifest)
            logger.info(f"✨ [NODE {config.NODE_ID}] MIRROR SYNC: Saved metadata for {filename}")
            
    await consensus_impl.register_commit_callback(raft_commit_listener)
    
    # Register self as active
    await registry.register_node(config.NODE_ID)
    logger.info(f"✅ Node {config.NODE_ID} fully started with Raft and Time Sync")
    
    yield
    
    # SHUTDOWN
    logger.info(f"🛑 Shutting down node {config.NODE_ID}")
    try:
        await consensus_impl.stop_raft()
        if replication: await replication.stop()
        if recovery: await recovery.stop()
        if fault_tolerance: await fault_tolerance.stop()
        
        # Cancel background tasks
        for task in [h_task, r_task, rec_task, f_task]:
            task.cancel()
    except Exception as e:
        logger.debug(f"Error during shutdown: {e}")

# Create ONLY ONE FastAPI app instance
app = FastAPI(title=f"Node {config.NODE_ID}", lifespan=lifespan)

BLOCK_SIZE = 1024 * 1024  # 1MB blocks

BLOCK_SIZE = 1024 * 1024  # 1MB blocks

@app.post("/files/{filename}")
async def upload_file(filename: str, request: Request, file: UploadFile = File(...)):
    """Upload a file - splits into blocks and replicates via Raft consensus"""
    # 1. Consensus Check - only Leader can coordinate writes
    leader = await consensus_impl.get_current_leader()
    if leader != config.NODE_ID:
        if leader and leader in config.ALL_NODES:
            # Redirect to leader or inform client
            return {
                "status": "error",
                "message": "not leader",
                "leader": leader,
                "leader_url": config.ALL_NODES[leader]
            }
        else:
            raise HTTPException(status_code=503, detail="No leader elected yet")

    content = await file.read()
    if not content:
        logger.error(f"❌ Empty file received for {filename}")
        raise HTTPException(status_code=400, detail="Empty file uploaded")
        
    logger.info(f"📤 Received {len(content)} bytes for {filename}. Splitting into stores...")
    blocks = []
    
    # Use app state components
    storage = request.app.state.storage
    registry = request.app.state.registry
    replication = request.app.state.replication

    # Get live nodes for replication
    live_nodes = await registry.get_live_nodes()
    target_nodes = [n for n in live_nodes if n != config.NODE_ID]
    
    # Split into blocks
    for i in range(0, len(content), BLOCK_SIZE):
        block_data = content[i:i+BLOCK_SIZE]
        block_id = generate_block_id(filename, i // BLOCK_SIZE)
        
        # Save block locally
        await storage.write_block(block_id, block_data)
        
        blocks.append({
            "block_id": block_id,
            "offset": i,
            "size": len(block_data)
        })
        
        # Replicate to other nodes asynchronously (Task 2: Data Replication Layer independence)
        # Binary data replication is handled separately via /replicate async pipeline
        if target_nodes:
            lamport_ts = request.app.state.lamport_clock.tick() if hasattr(request.app.state, "lamport_clock") else 1
            asyncio.create_task(replication.replicate_block(
                block_id=block_id,
                data=block_data,
                target_nodes=target_nodes,
                lamport_ts=lamport_ts
            ))
    
    lamport_ts_meta = request.app.state.lamport_clock.tick() if hasattr(request.app.state, "lamport_clock") else 1
    
    # Save file manifest via Consensus (Task 4)
    manifest = {
        "filename": filename,
        "total_size": len(content),
        "blocks": blocks,
        "created": time.time(),
        "replicated_to": target_nodes,
        "lamport_ts": lamport_ts_meta,
        "source_node": config.NODE_ID
    }
    
    # 🌟 MIRRORING (Option 1): Replicate metadata manifest to followers immediately
    if target_nodes:
        await replication.replicate_metadata(filename, manifest, target_nodes, lamport_ts_meta)
    
    # Prepare Raft Log Entry
    raft_entry = LogEntry(
        term=0, # Will be set by RaftNode
        index=0, # Will be set by RaftNode
        op="CREATE_FILE",
        file_id=filename,
        payload=manifest
    )
    
    # Replicate log entry (This ensures agreement among servers)
    # Raft is used ONLY for metadata consensus (file manifests, leadership)
    success = await consensus_impl.replicate_log(raft_entry)
    if not success:
        raise HTTPException(status_code=500, detail="Consensus failed for file metadata")
    
    # Commit locally to metadata storage
    await storage.save_metadata(f"manifest_{filename}", manifest)
    
    return {
        "status": "success",
        "filename": filename,
        "total_size": len(content),
        "blocks": len(blocks),
        "consensus": "committed"
    }

@app.get("/health")
async def health():
    return {"status": "ok", "node": config.NODE_ID, "state": (await consensus_impl._raft_node.state if consensus_impl._raft_node else "unknown")}

@app.post("/heartbeat")
async def receive_heartbeat(data: dict):
    """Receive heartbeat from another node"""
    node_id = data.get("node_id")
    await registry.register_node(node_id)
    return {"status": "ok", "time": time.time()}

# --- Raft Consensus Endpoints (Task 4) ---

@app.post("/raft/vote")
async def raft_vote(data: dict):
    return await consensus_impl.handle_vote_request(
        data["term"], data["candidate_id"], 
        data["last_log_index"], data["last_log_term"]
    )

@app.post("/raft/append_entries")
async def raft_append_entries(data: dict):
    return await consensus_impl.handle_append_entries(
        data["term"], data["leader_id"],
        data["prev_log_index"], data["prev_log_term"],
        data["entries"], data["leader_commit"]
    )

@app.post("/replicate")
async def receive_replication(data: dict, request: Request):
    """
    Component for Task 2: Receive and store a replicated block (JSON/Hex)
    Asynchronous replication layer independent of Raft.
    """
    block_id = data.get("block_id")
    block_data_hex = data.get("data")
    source_node = data.get("source_node", "unknown")
    lamport_ts = data.get("lamport_ts", 1)
    
    if hasattr(request.app.state, "lamport_clock"):
        request.app.state.lamport_clock.update(lamport_ts)
    
    if not block_id or not block_data_hex:
        raise HTTPException(status_code=400, detail="Missing block data")
    
    # Binary safety over HTTP JSON
    block_data = bytes.fromhex(block_data_hex)
    storage = request.app.state.storage
    
    await storage.write_block(block_id, block_data)
    logger.info(f"✨ [NODE {config.NODE_ID}] REPLICATED: Block {block_id} from {source_node}")
    return {"status": "ok"}

@app.post("/replicate_meta")
async def replicate_meta(data: dict, request: Request):
    """Mirror a metadata manifest to local storage (Option 1 replication)"""
    filename = data["filename"]
    manifest = data["manifest"]
    lamport_ts = data.get("lamport_ts", 1)
    
    if hasattr(request.app.state, "lamport_clock"):
        request.app.state.lamport_clock.update(lamport_ts)
        
    storage = request.app.state.storage
    
    await storage.save_metadata(f"manifest_{filename}", manifest)
    logger.info(f"✨ [NODE {config.NODE_ID}] MIRROR: Manifest replicated for {filename}")
    return {"status": "ok"}

# --- Time Synchronization Endpoints (Task 3) ---

@app.get("/time")
async def get_time():
    """Return physical OS-level time natively"""
    return {"node_id": config.NODE_ID, "time": time.time()}

async def heartbeat_sender():
    """Background task: send heartbeats for failure detection (Task 1)"""
    async with httpx.AsyncClient() as client:
        while True:
            for node_id, url in config.OTHER_NODES.items():
                try:
                    await client.post(
                        f"{url}/heartbeat",
                        json={"node_id": config.NODE_ID},
                        timeout=0.5
                    )
                except:
                    pass
            await asyncio.sleep(1.0)  # Periodic asynchronous heartbeat (~1s interval)


@app.get("/status")
@app.get("/api/status")
async def status(request: Request):
    """Comprehensive system status"""
    s_state = request.app.state
    system_status = await s_state.fault_tolerance.get_system_status()
    replication_stats = s_state.replication.get_stats()
    checkpoint_info = s_state.recovery.get_checkpoint_info()
    blocks = await s_state.storage.list_blocks()
    
    raft_state = "unknown"
    if consensus_impl._raft_node:
        raft_state = {
            "state": consensus_impl._raft_node.state,
            "term": consensus_impl._raft_node.current_term,
            "leader": consensus_impl._raft_node.leader_id,
            "commit_index": consensus_impl._raft_node.commit_index
        }
    
    return {
        "node_id": config.NODE_ID,
        "raft": raft_state,
        "system": system_status,
        "storage": {"block_count": len(blocks)},
        "replication": replication_stats,
        "checkpoint": checkpoint_info
    }

@app.get("/api/metrics")
async def metrics(request: Request):
    """Real-time metrics for algorithm monitoring dashboard (updates every 2s)"""
    s_state = request.app.state
    
    # Consensus Metrics
    raft_state = {
        "state": consensus_impl._raft_node.state if consensus_impl._raft_node else "unknown",
        "leader": consensus_impl._raft_node.leader_id if consensus_impl._raft_node else None,
        "term": consensus_impl._raft_node.current_term if consensus_impl._raft_node else 0,
        "vote_count": 1 if consensus_impl._raft_node and consensus_impl._raft_node.state == "leader" else 0  # Simplified
    }
    
    # Replication Metrics
    blocks = await s_state.storage.list_blocks()
    live_nodes = await s_state.registry.get_live_nodes()
    replication_state = {
        "files_stored": len(blocks),
        "peer_count": len(live_nodes) - 1 if config.NODE_ID in live_nodes else len(live_nodes),
        "peers": [n for n in live_nodes if n != config.NODE_ID]
    }
    
    # Fault Tolerance Metrics
    fault_state = {
        "healthy_counts": len(live_nodes),
        "suspected_counts": len([n for n, s in s_state.fault_tolerance.node_status.items() if s.value == "suspected"]),
        "failed_counts": len([n for n, s in s_state.fault_tolerance.node_status.items() if s.value == "failed"]),
        "node_status_details": {n: s.value for n, s in s_state.fault_tolerance.node_status.items()}
    }
    
    # Time Sync Metrics
    lamport = s_state.lamport_clock.get_time() if hasattr(s_state, "lamport_clock") else 0
    time_state = {
        "protocol": "OS Native NTP + Lamport Causality",
        "last_sync_timestamp": time.time(),
        "lamport_counter": lamport
    }
    
    return {
        "consensus": raft_state,
        "replication": replication_state,
        "fault_tolerance": fault_state,
        "time_sync": time_state
    }

@app.post("/replicate")
async def receive_replication(data: dict):
    """Receive replicated block from another node"""
    block_id = data.get("block_id")
    data_hex = data.get("data")
    source_node = data.get("source_node")
    if not block_id or not data_hex:
        return {"error": "Missing block_id or data"}, 400
    try:
        block_data = bytes.fromhex(data_hex)
        success = await storage.write_with_checksum(block_id, block_data)
        if success:
            logger.info(f"Received replicated block {block_id} from {source_node}")
            return {"status": "accepted", "block_id": block_id}
        return {"status": "failed"}, 500
    except Exception as e:
        return {"error": str(e)}, 500
    
@app.post("/recovery/plan")
async def plan_recovery(data: dict):
    """Plan recovery for a failed node"""
    failed_node = data.get("failed_node")
    if not failed_node:
        return {"error": "Missing failed_node"}, 400
    file_map = await fault_tolerance._get_file_map()
    plan = await recovery.plan_recovery(failed_node, file_map)
    return {"failed_node": failed_node, "recovery_plan": plan}    

@app.post("/recovery/execute")
async def execute_recovery(data: dict):
    """Execute recovery for a node"""
    recovering_node = data.get("recovering_node")
    recovery_plan = data.get("recovery_plan", {})
    if not recovering_node:
        return {"error": "Missing recovering_node"}, 400
    await recovery.execute_recovery(recovering_node, recovery_plan)
    return {"status": "complete", "recovering_node": recovering_node}

@app.get("/files/{filename}")
async def download_file(filename: str):
    """Download a file - assembles from blocks"""
    manifest = await storage.get_metadata(f"manifest_{filename}")
    if not manifest:
        return {"error": "File not found"}
    
    all_data = b""
    missing_blocks = []
    
    for block_info in manifest["blocks"]:
        data = await storage.read_block(block_info["block_id"])
        if data:
            all_data += data
        else:
            missing_blocks.append(block_info["block_id"])
    
    if missing_blocks:
        return {"error": f"File corrupted, missing {len(missing_blocks)} blocks"}
    
    return Response(
        content=all_data, 
        media_type="application/octet-stream",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )

@app.delete("/files/{filename}")
async def delete_file(filename: str, request: Request):
    """Delete a file and its blocks"""
    leader = await consensus_impl.get_current_leader()
    if leader != config.NODE_ID:
        raise HTTPException(status_code=503, detail=f"Not leader. Please contact {leader}")
        
    s_state = request.app.state
    manifest = await s_state.storage.get_metadata(f"manifest_{filename}")
    
    if not manifest:
        raise HTTPException(status_code=404, detail="File not found")
        
    # Delete blocks
    for block_info in manifest["blocks"]:
        await s_state.storage.delete_block(block_info["block_id"])
        
    # Delete manifest
    await s_state.storage.delete_metadata(f"manifest_{filename}")
    
    # In a full implementation, we would replicate this deletion via Raft
    
    return {"status": "success", "message": f"Deleted {filename}"}


if __name__ == "__main__":
    import argparse
    import sys
    import subprocess

    parser = argparse.ArgumentParser(description="Distributed File System Node")
    parser.add_argument("--mode", choices=["server", "demo", "client"], default="server", help="Run mode")
    parser.add_argument("--node-id", type=str, help="Node ID (for server mode)")
    parser.add_argument("--port", type=int, help="Port (for server mode)")
    
    args = parser.parse_args()

    if args.mode == "demo":
        logger.info(f"Starting {len(config.ALL_NODES)}-node demo cluster...")
        processes = []
        try:
            for i in range(1, len(config.ALL_NODES) + 1):
                node_id = f"node{i}"
                port = 8000 + i
                logger.info(f"Starting {node_id} on port {port}")
                p = subprocess.Popen(
                    [sys.executable, __file__, "--mode", "server", "--node-id", node_id, "--port", str(port)],
                    env={**os.environ, "NODE_ID": node_id, "PORT": str(port)}
                )
                processes.append(p)
            
            logger.info("Cluster started. Press Ctrl+C to stop.")
            for p in processes:
                p.wait()
        except KeyboardInterrupt:
            logger.info("Stopping cluster...")
            for p in processes:
                p.terminate()
                
    elif args.mode == "client":
        logger.info("🛠️  DFS Client Started")
        base_url = "http://127.0.0.1:8001" # Default to node1
        
        while True:
            cmd = input("\ndfs> ").strip().split()
            if not cmd: continue
            action = cmd[0].lower()
            
            if action == "exit": break
            
            try:
                # Helper for automatic redirection
                def perform_request(method, url, **kwargs):
                    global base_url
                    r = requests.request(method, url, **kwargs)
                    data = r.json() if "application/json" in r.headers.get("Content-Type", "") else None
                    
                    if data and isinstance(data, dict) and data.get("status") == "error" and data.get("leader_url"):
                        new_url = data["leader_url"]
                        print(f"🔄 Redirecting to leader: {new_url}")
                        
                        # CRITICAL: Reset file pointers before retry
                        if "files" in kwargs:
                            for file_key in kwargs["files"]:
                                kwargs["files"][file_key].seek(0)
                        
                        base_url = new_url
                        # Retry once with new URL
                        new_request_url = url.replace(url.split("/files")[0] if "/files" in url else url.split("/status")[0], base_url)
                        return requests.request(method, new_request_url, **kwargs)
                    return r

                if action == "write" and len(cmd) > 1:
                    filename = cmd[1]
                    content = " ".join(cmd[2:]) if len(cmd) > 2 else "Default Content"
                    with open("temp_upload.txt", "w") as f: f.write(content)
                    with open("temp_upload.txt", "rb") as f:
                        resp = perform_request("POST", f"{base_url}/files/{filename}", files={"file": f})
                        print(resp.json())
                    os.remove("temp_upload.txt")
                
                elif action == "read" and len(cmd) > 1:
                    filename = cmd[1]
                    resp = perform_request("GET", f"{base_url}/files/{filename}")
                    if resp.status_code == 200:
                        print(f"Content: {resp.text}")
                    else:
                        print(resp.json())
                
                elif action == "status":
                    resp = perform_request("GET", f"{base_url}/status")
                    print(json.dumps(resp.json(), indent=2))
                
                else:
                    print("Unknown command. Try: write <file> <content>, read <file>, status, exit")
            except Exception as e:
                print(f"Error: {e}")

    else:
        # Server Mode
        if args.node_id:
            os.environ["NODE_ID"] = args.node_id
        if args.port:
            os.environ["PORT"] = str(args.port)
        
        # Reload config with new env vars
        from shared.config import config
        uvicorn.run(app, host=config.HOST, port=config.PORT)


@app.post("/benchmark/upload_latency")
async def benchmark_upload(file: UploadFile = File(...)):
    start = time.time()

    filename = f"bench_{file.filename}"
    content = await file.read()

    await storage.write_block(filename, content)

    latency = (time.time() - start) * 1000

    BENCHMARK_LOGS.append({
        "type": "upload",
        "latency_ms": latency,
        "size": len(content),
        "timestamp": time.time(),
        "node": config.NODE_ID
    })

    return {
        "latency_ms": latency,
        "size": len(content)
    }

@app.get("/api/benchmarks")
async def get_benchmarks():
    return {"data": BENCHMARK_LOGS}