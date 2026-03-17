import asyncio
import httpx
import time
from typing import Dict, List
from utils import logger

class NodeRegistry:
  """Keeps track of all live nodes in the cluster"""
  
  def __init__(self, node_id: str):
    self.node_id = node_id
    self.live_nodes: Dict[str, float] = {}  # node_id -> last_seen timestamp
    self.lock = asyncio.Lock()
  
  async def register_node(self, node_id: str):
    """Record that a node is alive"""
    async with self.lock:
      self.live_nodes[node_id] = time.time()
      logger.info(f"Node {node_id} registered. Live nodes: {list(self.live_nodes.keys())}")
  
  async def get_live_nodes(self) -> List[str]:
    """Get list of nodes that heartbeated recently"""
    async with self.lock:
      now = time.time()
      # Remove nodes not seen in last 10 seconds
      dead = [n for n, t in self.live_nodes.items() if now - t > 10]
      for n in dead:
        del self.live_nodes[n]
      return list(self.live_nodes.keys())
  
  async def is_majority(self) -> bool:
    """Check if we have majority of nodes alive"""
    live = await self.get_live_nodes()
    total = 3  # total nodes in cluster
    return len(live) > total // 2

# Global registry instance
registry = NodeRegistry("node1")  # Will be updated per node