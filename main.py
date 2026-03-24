# main.py - Step 3
from fastapi import FastAPI, UploadFile, File, Response  # Added Response
import uvicorn
import asyncio
import httpx
import time
import os

from replication import ReplicationManager
from recovery import RecoveryManager
from fault_tolerance import FaultToleranceManager
from config import config
from storage import StorageManager
from registry import NodeRegistry
from utils import logger, generate_block_id
from utils import logger, generate_block_id, format_size

# Initialize components
storage = StorageManager()
registry = NodeRegistry(config.NODE_ID)
replication = ReplicationManager(config.NODE_ID, storage)
recovery = RecoveryManager(config.NODE_ID, storage)
fault_tolerance = FaultToleranceManager(config.NODE_ID, storage, registry, replication, recovery)

# Create FastAPI app
app = FastAPI(title=f"Node {config.NODE_ID}")
BLOCK_SIZE = 1024 * 1024  # 1MB blocks

#File upload endpoint - accepts a file, splits into blocks, and saves them
@app.post("/files/{filename}")
async def upload_file(filename: str, file: UploadFile = File(...)):
    """Upload a file - splits into blocks and replicates"""
    content = await file.read()
    blocks = []
    
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
        
        # Replicate to other nodes
        if target_nodes:
            await replication.replicate_block(
                block_id=block_id,
                data=block_data,
                target_nodes=target_nodes,
                version=1
            )
    
    # Save file manifest
    manifest = {
        "filename": filename,
        "total_size": len(content),
        "blocks": blocks,
        "created": time.time(),
        "replicated_to": target_nodes
    }
    await storage.save_metadata(f"manifest_{filename}", manifest)
    
    return {
        "status": "success",
        "filename": filename,
        "total_size": len(content),
        "blocks": len(blocks),
        "replicated_to": target_nodes
    }

@app.get("/files/{filename}")
async def download_file(filename: str):
    """Download a file - assembles from blocks"""
    # Load manifest
    manifest = await storage.get_metadata(f"manifest_{filename}")
    if not manifest:
        return {"error": "File not found"}
    
    # Read and combine all blocks
    all_data = b""
    for block_info in manifest["blocks"]:
        data = await storage.read_block(block_info["block_id"])
        if data:
            all_data += data
    
    # Return the file
    return Response(
        content=all_data, 
        media_type="application/octet-stream",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )

#Health check endpoint to verify the node is running
@app.get("/health")
async def health():
    return {"status": "ok", "node": config.NODE_ID}

#The heartbeat endpoint to receive heartbeats from other nodes and update the registry
@app.post("/heartbeat")
async def receive_heartbeat(data: dict):
    """Receive heartbeat from another node"""
    node_id = data.get("node_id")
    await registry.register_node(node_id)
    return {"status": "ok", "time": time.time()}

async def heartbeat_sender():
    """Background task: send heartbeats to all nodes every 2 seconds"""
    async with httpx.AsyncClient() as client:
        while True:
            for node_id, url in config.OTHER_NODES.items():
                try:
                    await client.post(
                        f"{url}/heartbeat",
                        json={"node_id": config.NODE_ID},
                        timeout=1.0
                    )
                    logger.debug(f"Heartbeat sent to {node_id}")
                except:
                    logger.warning(f"Heartbeat failed to {node_id}")
            await asyncio.sleep(2)

#the startup event to launch the heartbeat sender when the server starts
@app.on_event("startup")
async def startup():
    asyncio.create_task(heartbeat_sender())
    asyncio.create_task(recovery.start())
    asyncio.create_task(fault_tolerance.start())
    
@app.on_event("shutdown")
async def shutdown():
    """Clean shutdown"""
    logger.info(f"Shutting down node {config.NODE_ID}")
    await replication.stop()
    await recovery.stop()
    await fault_tolerance.stop()    

@app.get("/status")
async def status():
    """Comprehensive system status"""
    # Get system status from fault tolerance
    system_status = await fault_tolerance.get_system_status()
    
    # Get replication stats
    replication_stats = replication.get_stats()
    
    # Get checkpoint info
    checkpoint_info = recovery.get_checkpoint_info()
    
    # Get local storage info
    blocks = await storage.list_blocks()
    
    return {
        "node_id": config.NODE_ID,
        "system": system_status,
        "storage": {
            "block_count": len(blocks),
        },
        "replication": replication_stats,
        "checkpoint": checkpoint_info
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

@app.get("/fault/status")
async def fault_status():
    """Detailed fault tolerance status"""
    return await fault_tolerance.get_system_status()

#The main entry point to run the server
if __name__ == "__main__":
    uvicorn.run(app, host=config.HOST, port=config.PORT)