from registry import registry
import asyncio
import httpx
from fastapi import FastAPI, UploadFile, File
import uvicorn
from config import config
from storage import StorageManager
from utils import logger, generate_block_id

# Initialize components
storage = StorageManager()

# Create FastAPI app
app = FastAPI(title=f"Node {config.NODE_ID}")
BLOCK_SIZE = 1024 * 1024  # 1MB blocks

@app.post("/files/{filename}")
async def upload_file(filename: str, file: UploadFile = File(...)):
    content = await file.read()
    
    # Split into blocks
    blocks = []
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
  
  
@app.get("/health")
async def health():
    return {"status": "ok", "node": config.NODE_ID}

@app.get("/files/{filename}")
async def download_file(filename: str):
    # Simple version - just return first block
    # You'll make this smarter later
    return {"message": "Download not fully implemented yet"}

# Run server
if __name__ == "__main__":
    uvicorn.run(app, host=config.HOST, port=config.PORT)
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

# Add to startup
@app.on_event("startup")
async def startup():
    asyncio.create_task(heartbeat_sender())