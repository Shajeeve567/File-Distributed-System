# registry.py - COMPLETE VERSION
import asyncio
import time
from enum import Enum
from typing import Dict, List, Optional
from utils import logger
from config import config

class NodeStatus(Enum):
    ACTIVE = "active"
    SUSPECTED = "suspected"
    FAILED = "failed"
    RECOVERING = "recovering"

class NodeRegistry:
    """Keeps track of all live nodes in the cluster"""
    
    def __init__(self, node_id: str):
        self.node_id = node_id
        self.live_nodes: Dict[str, float] = {}
        self.node_status: Dict[str, NodeStatus] = {}
        self.lock = asyncio.Lock()
        self.total_nodes = len(config.ALL_NODES)
        self.failure_timeout = 6  # seconds
        
        # Initialize status for all nodes
        for node in config.ALL_NODES.keys():
            self.node_status[node] = NodeStatus.ACTIVE if node == node_id else NodeStatus.ACTIVE
    
    async def register_node(self, node_id: str):
        """Record that a node is alive"""
        async with self.lock:
            self.live_nodes[node_id] = time.time()
            
            # If node was marked as failed, now it's recovering
            if self.node_status.get(node_id) == NodeStatus.FAILED:
                self.node_status[node_id] = NodeStatus.RECOVERING
                logger.info(f"Node {node_id} is RECOVERING")
            elif self.node_status.get(node_id) == NodeStatus.SUSPECTED:
                self.node_status[node_id] = NodeStatus.ACTIVE
                logger.info(f"Node {node_id} is back to ACTIVE")
            else:
                self.node_status[node_id] = NodeStatus.ACTIVE
            
            logger.info(f"Node {node_id} registered. Live nodes: {list(self.live_nodes.keys())}")
    
    async def get_live_nodes(self) -> List[str]:
        """Get list of nodes that heartbeated recently"""
        async with self.lock:
            now = time.time()
            # Remove nodes not seen in last failure_timeout seconds
            dead = [n for n, t in self.live_nodes.items() if now - t > self.failure_timeout]
            for n in dead:
                if self.node_status.get(n) != NodeStatus.FAILED:
                    self.node_status[n] = NodeStatus.FAILED
                    logger.warning(f"Node {n} marked as FAILED (no heartbeat for {self.failure_timeout}s)")
                del self.live_nodes[n]
            
            return list(self.live_nodes.keys())
    
    async def get_node_status(self, node_id: str) -> NodeStatus:
        """Get status of a specific node"""
        async with self.lock:
            return self.node_status.get(node_id, NodeStatus.ACTIVE)
    
    async def mark_failed(self, node_id: str):
        """Manually mark a node as failed"""
        async with self.lock:
            self.node_status[node_id] = NodeStatus.FAILED
            if node_id in self.live_nodes:
                del self.live_nodes[node_id]
            logger.info(f"Node {node_id} manually marked as FAILED")
    
    async def mark_recovering(self, node_id: str):
        """Mark a node as recovering"""
        async with self.lock:
            self.node_status[node_id] = NodeStatus.RECOVERING
            logger.info(f"Node {node_id} marked as RECOVERING")
    
    async def is_majority(self) -> bool:
        """Check if we have majority of nodes alive"""
        live = await self.get_live_nodes()
        if self.node_id not in live:
            live.append(self.node_id)
        return len(live) > self.total_nodes // 2
    
    async def get_active_nodes(self) -> List[str]:
        """Get all nodes that are ACTIVE"""
        async with self.lock:
            return [n for n, s in self.node_status.items() if s == NodeStatus.ACTIVE]