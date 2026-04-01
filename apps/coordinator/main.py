# apps/coordinator/main.py
from fastapi import FastAPI, HTTPException
from contextlib import asynccontextmanager
import logging
import os
import asyncio

# Import from shared dependencies — these will use STUBS until teammates merge their code
from shared.dependencies import (
    metadata_repo,           
    storage_data_gateway,    
    node_health_gateway,     
    consensus_engine,        
    clock_monitor            
)

from shared.models import WriteRequest, WriteResult, LogEntry, FileMetadata


from features.consensus.consensus_impl import (
    init_raft_node,
    get_current_leader,
    replicate_log,
    stop_raft
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
NODE_ID = os.getenv("COORDINATOR_ID", "coordinator-1")
PEER_IDS = []


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown."""
    # Initialize YOUR Raft consensus
    logger.info(f"Starting coordinator {NODE_ID} with peers: {PEER_IDS}")
    await init_raft_node(NODE_ID, PEER_IDS)
    leader = await get_current_leader()
    logger.info(f"Raft initialized. Current leader: {leader}")
    logger.info(f"Am I leader? {leader == NODE_ID}")
    
    yield
    
    # Shutdown YOUR Raft
    await stop_raft()
    logger.info("Coordinator stopped")


app = FastAPI(lifespan=lifespan)


# ==================== YOUR CONSENSUS ENDPOINTS ====================
@app.get("/health")
async def health_check():
    """Health check with consensus info."""
    from features.consensus.consensus_impl import _raft_node
    
    # Check if Raft is initialized
    if _raft_node is None:
        return {
            "status": "initializing",
            "node_id": NODE_ID,
            "message": "Raft not yet initialized"
        }
    
    # Get leader with timeout handling
    try:
        leader = await asyncio.wait_for(get_current_leader(), timeout=1.0)
    except asyncio.TimeoutError:
        leader = None
        logger.warning("Leader check timed out")
    
    # Using stubs — these are fast
    live_nodes = node_health_gateway.list_live_nodes()
    
    return {
        "status": "healthy" if leader else "degraded",
        "node_id": NODE_ID,
        "current_leader": leader,
        "am_i_leader": leader == NODE_ID if leader else False,
        "live_nodes": live_nodes,
        "message": "Using stubs for health, metadata, storage until teammates merge"
    }


# apps/coordinator/main.py

@app.get("/raft/status")
async def raft_status():
    """Detailed Raft status with timeout."""
    from features.consensus.consensus_impl import _raft_node
    
    if _raft_node is None:
        return {"error": "Raft not initialized"}
    
    try:
        # Timeout after 1 second
        async with asyncio.timeout(1.0):
            async with _raft_node._lock:
                return {
                    "node_id": _raft_node.node_id,
                    "state": _raft_node.state,
                    "current_term": _raft_node.current_term,
                    "voted_for": _raft_node.voted_for,
                    "leader_id": _raft_node.leader_id,
                    "log_length": len(_raft_node.log),
                    "commit_index": _raft_node.commit_index,
                    "last_applied": _raft_node.last_applied,
                    "peers": _raft_node.peer_ids
                }
    except asyncio.TimeoutError:
        return {
            "error": "Raft is busy",
            "node_id": _raft_node.node_id,
            "state": _raft_node.state
        }

@app.get("/debug/raft_internal")
async def debug_raft_internal():
    """Debug endpoint to see raw Raft state (no locks)."""
    from features.consensus.consensus_impl import _raft_node
    
    if _raft_node is None:
        return {"error": "Raft not initialized"}
    
    # Access directly without lock for debugging
    return {
        "node_id": _raft_node.node_id,
        "state": _raft_node.state,
        "current_term": _raft_node.current_term,
        "leader_id": _raft_node.leader_id,
        "voted_for": _raft_node.voted_for,
        "peer_ids": _raft_node.peer_ids,
        "log_length": len(_raft_node.log),
        "commit_index": _raft_node.commit_index,
    }

# ==================== FILE OPERATIONS (Using Stubs) ====================
@app.post("/write")
async def write_file(request: WriteRequest):
    """
    Write a file — uses stubs for now.
    """
    # Step 1: Leader check
    current_leader = await get_current_leader()
    if current_leader != NODE_ID:
        raise HTTPException(
            status_code=503,
            detail=f"Not leader. Please redirect to {current_leader}",
            headers={"X-Leader": current_leader or "unknown"}
        )
    
    logger.info(f"Leader {NODE_ID} processing write for {request.file_id}")
    
    # Step 2: Replicate log entry with timeout
    entry = LogEntry(
        term=0, index=0,
        op="write",
        file_id=request.file_id,
        payload={
            "filename": request.filename,
            "content": request.content,
            "client_ts": request.client_ts
        }
    )
    
    try:
        consensus_success = await asyncio.wait_for(replicate_log(entry), timeout=2.0)
        if not consensus_success:
            raise HTTPException(500, "Consensus replication failed")
    except asyncio.TimeoutError:
        logger.error(f"Consensus replication timed out for {request.file_id}")
        raise HTTPException(500, "Consensus replication timeout")
    
    logger.info(f"Consensus succeeded for {request.file_id}")
    
    # Step 3: Storage operations (these are stubs, should be fast)
    current_meta = metadata_repo.get_file_metadata(request.file_id)
    current_version = current_meta.version if current_meta else 0
    new_version = current_version + 1
    
    replica_nodes = metadata_repo.choose_replica_nodes(request.file_id, 3)
    logger.info(f"Stub returned replica nodes: {replica_nodes}")
    
    succeeded_nodes = []
    for node in replica_nodes:
        if storage_data_gateway.store_on_node(node, request):
            succeeded_nodes.append(node)
            logger.info(f"Stub stored on {node}")
    
    updated_meta = FileMetadata(
        file_id=request.file_id,
        filename=request.filename,
        version=new_version,
        primary_node=succeeded_nodes[0] if succeeded_nodes else "",
        replica_nodes=succeeded_nodes,
        status="READY"
    )
    metadata_repo.upsert_file_metadata(updated_meta)
    
    return WriteResult(
        success=True,
        version=new_version,
        committed_nodes=succeeded_nodes,
        leader_id=NODE_ID
    )


@app.get("/read/{file_id}")
async def read_file(file_id: str):
    """
    Read a file — uses stubs for now.
    """
    # Get metadata via stub
    metadata = metadata_repo.get_file_metadata(file_id)
    if not metadata:
        raise HTTPException(status_code=404, detail="File not found")
    
    # Read from primary node via stub
    content = storage_data_gateway.read_from_node(metadata.primary_node, file_id)
    if not content:
        # Try replicas via stub
        for node in metadata.replica_nodes:
            content = storage_data_gateway.read_from_node(node, file_id)
            if content:
                break
    
    if not content:
        raise HTTPException(status_code=404, detail="Content not found")
    
    return {
        "file_id": file_id,
        "filename": metadata.filename,
        "content": content,
        "version": metadata.version,
        "note": "Using stubs for storage and metadata"
    }


@app.get("/debug/dependencies")
async def debug_dependencies():
    """Show which dependencies are using stubs vs real implementations."""
    import os
    
    return {
        "USE_STUB_METADATA": os.getenv("USE_STUB_METADATA", "1"),
        "USE_STUB_STORAGE": os.getenv("USE_STUB_STORAGE", "1"),
        "USE_STUB_HEALTH": os.getenv("USE_STUB_HEALTH", "1"),
        "USE_STUB_CONSENSUS": os.getenv("USE_STUB_CONSENSUS", "0"),  # Your real implementation
        "USE_STUB_CLOCK": os.getenv("USE_STUB_CLOCK", "1"),
        "message": "When teammates merge their code, set respective flags to 0"
    }


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run(app, host="0.0.0.0", port=port)