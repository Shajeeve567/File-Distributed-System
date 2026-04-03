import asyncio
import httpx
from typing import Dict, List, Optional
from utils import logger

class ReplicationManager:
  """Handles replication of blocks to other nodes"""

  def __init__(self, node_id: str, storage):
    self.node_id = node_id
    self.storage = storage
    self.replication_queue = asyncio.Queue()
    self.replication_factor = 3
    self.client = httpx.AsyncClient(timeout=5.0)
    self.running = True
    
    # Statistics
    self.stats = {
        "total_replications": 0,
        "successful": 0,
        "failed": 0,
        "retries": 0
    }
    
    logger.info(f"ReplicationManager initialized for {node_id}")

  async def start(self):
    """Start background replication processor"""
    asyncio.create_task(self._process_queue())
    logger.info("Replication processor started")

  async def stop(self):
    """Stop replication manager"""
    self.running = False
    await self.client.aclose()

  async def replicate_block(self, block_id: str, data: bytes, target_nodes: List[str], version: int = 1):
    """Queue a block for replication to target nodes"""
    await self.replication_queue.put({
      "block_id": block_id,
      "data": data,
      "targets": target_nodes,
      "version": version,
      "attempts": 0
    })
    logger.info(f"Queued replication for {block_id} to {len(target_nodes)} nodes")

  async def _process_queue(self):
      """Background worker that processes replication queue"""
      while self.running:
        try:
          task = await self.replication_queue.get()
          
          # Increment attempts
          task["attempts"] += 1
          
          # Replicate to all targets
          results = await self._replicate_to_nodes(task)
          
          # Check results
          successful = [n for n, success in results.items() if success]
          failed = [n for n, success in results.items() if not success]
          
          self.stats["total_replications"] += 1
          self.stats["successful"] += len(successful)
          self.stats["failed"] += len(failed)
          
          # If some failed and attempts < 3, retry
          if failed and task["attempts"] < 3:
            logger.warning(f"Retrying replication for {task['block_id']} (attempt {task['attempts']+1})")
            await asyncio.sleep(2)  # Wait before retry
            await self.replication_queue.put(task)
            self.stats["retries"] += 1
          elif failed:
            logger.error(f"Replication failed for {task['block_id']} after 3 attempts")
                
        except Exception as e:
            logger.error(f"Error in replication processor: {e}")
            await asyncio.sleep(1)

  async def _replicate_to_nodes(self, task: Dict) -> Dict[str, bool]:
    """Send block to multiple nodes in parallel"""
    from config import config
    
    results = {}
    
    async def replicate_to_node(node_id: str, url: str):
      try:
        # Send replication request
        response = await self.client.post(
          f"{url}/replicate",
          json={
            "block_id": task["block_id"],
            "data": task["data"].hex(),
            "version": task["version"],
            "source_node": self.node_id,
            "timestamp": time.time()
          },
          timeout=5.0
        )
          
        if response.status_code == 200:
          return node_id, True
        else:
          return node_id, False
              
      except Exception as e:
        logger.warning(f"Replication to {node_id} failed: {e}")
        return node_id, False
    
    # Create tasks for all targets
    tasks = []
    for node_id in task["targets"]:
      url = config.ALL_NODES.get(node_id)
      if url:
        tasks.append(replicate_to_node(node_id, url))
    
    # Wait for all to complete
    task_results = await asyncio.gather(*tasks)
    
    for node_id, success in task_results:
      results[node_id] = success
    
    return results

  def get_stats(self) -> Dict:
    """Get replication statistics"""
    return self.stats.copy()