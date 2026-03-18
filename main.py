# main.py - Step 3
from fastapi import FastAPI, UploadFile, File, Response  # Added Response
import uvicorn
import asyncio
import httpx
import time
import os

from config import config
from storage import StorageManager
from registry import NodeRegistry
from utils import logger, generate_block_id

# Initialize components
storage = StorageManager()
registry = NodeRegistry(config.NODE_ID)

# Create FastAPI app
app = FastAPI(title=f"Node {config.NODE_ID}")
BLOCK_SIZE = 1024 * 1024  # 1MB blocks

#File upload endpoint - accepts a file, splits into blocks, and saves them
@app.post("/files/{filename}")
async def upload_file(filename: str, file: UploadFile = File(...)):
    """Upload a file - splits into blocks"""
    content = await file.read()
    blocks = []
    
    # Split into blocks
    for i in range(0, len(content), BLOCK_SIZE):
        block_data = content[i:i+BLOCK_SIZE]
        block_id = generate_block_id(filename, i // BLOCK_SIZE)
        
        # Save block
        await storage.write_block(block_id, block_data)
        
        blocks.append({
            "block_id": block_id,
            "offset": i,
            "size": len(block_data)
        })
    
    # Save file manifest
    manifest = {
        "filename": filename,
        "total_size": len(content),
        "blocks": blocks,
        "created": time.time()
    }
    await storage.save_metadata(f"manifest_{filename}", manifest)
    
    return manifest

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

#The main entry point to run the server
if __name__ == "__main__":
    uvicorn.run(app, host=config.HOST, port=config.PORT)