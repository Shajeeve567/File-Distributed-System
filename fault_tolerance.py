import asyncio
import time
from enum import Enum
from typing import Dict, List, Optional
from utils import logger

class NodeStatus(Enum):
    ACTIVE = "active"
    SUSPECTED = "suspected"
    FAILED = "failed"
    RECOVERING = "recovering"

class FaultToleranceManager:
    """
    Main fault tolerance coordinator
    Integrates heartbeat detection, replication, and recovery
    """
    
    def __init__(self, node_id: str, storage, registry, replication, recovery):
        self.node_id = node_id
        self.storage = storage
        self.registry = registry
        self.replication = replication
        self.recovery = recovery
        
        self.node_status: Dict[str, NodeStatus] = {}
        self.failure_timeout = 6  # seconds (3 missed heartbeats)
        self.heartbeat_interval = 2  # seconds
        
        self.running = True
        
        logger.info(f"FaultToleranceManager initialized for {node_id}")
    
    async def start(self):
        """Start fault tolerance monitoring"""
        asyncio.create_task(self._monitor_failures())
        logger.info("Fault tolerance monitoring started")
    
    async def stop(self):
        """Stop fault tolerance monitoring"""
        self.running = False
    
    async def _monitor_failures(self):
        """Monitor for node failures"""
        while self.running:
            await asyncio.sleep(1)
            
            # Get current live nodes from registry
            live_nodes = await self.registry.get_live_nodes()
            
            # Check each node
            for node_id in self._get_all_nodes():
                if node_id == self.node_id:
                    continue
                
                if node_id in live_nodes:
                    # Node is alive
                    if self.node_status.get(node_id) != NodeStatus.ACTIVE:
                        # Node was previously dead, now recovered
                        logger.info(f"✅ Node {node_id} is BACK ONLINE")
                        self.node_status[node_id] = NodeStatus.ACTIVE
                        
                        # Trigger recovery
                        file_map = await self._get_file_map()
                        await self.recovery.handle_node_recovery(node_id, file_map)
                else:
                    # Node not in live_nodes - might be failed
                    current_status = self.node_status.get(node_id, NodeStatus.ACTIVE)
                    
                    if current_status == NodeStatus.ACTIVE:
                        # First missed heartbeat - mark suspected
                        self.node_status[node_id] = NodeStatus.SUSPECTED
                        logger.warning(f"⚠️ Node {node_id} is SUSPECTED (missed heartbeat)")
                        
                    elif current_status == NodeStatus.SUSPECTED:
                        # Second missed heartbeat - mark failed
                        self.node_status[node_id] = NodeStatus.FAILED
                        logger.error(f"Node {node_id} has FAILED")
                        
                        # Handle failure
                        file_map = await self._get_file_map()
                        await self.recovery.handle_node_failure(node_id, file_map)
    
    def _get_all_nodes(self) -> List[str]:
        """Get list of all nodes in cluster"""
        from config import config
        return list(config.ALL_NODES.keys())
    
    async def _get_file_map(self) -> Dict[str, List[str]]:
        """
        Get mapping of files to which nodes have them
        In production, this would come from metadata service
        """
        file_map = {"_all_nodes": self._get_all_nodes()}
        
        # Get all files from local storage
        blocks = await self.storage.list_blocks()
        file_map["_total_blocks"] = len(blocks)
        
        # This is simplified - in production, you'd track file locations
        return file_map
    
    async def get_system_status(self) -> Dict:
        """Get current system status"""
        live_nodes = await self.registry.get_live_nodes()
        all_nodes = self._get_all_nodes()
        
        # Include self in live nodes if not present
        if self.node_id not in live_nodes:
            live_nodes.append(self.node_id)
        
        status = await self.recovery.system_status(live_nodes)
        
        return {
            "node_id": self.node_id,
            "status": status,
            "live_nodes": live_nodes,
            "failed_nodes": [n for n in all_nodes if n not in live_nodes and n != self.node_id],
            "suspected_nodes": [n for n, s in self.node_status.items() if s == NodeStatus.SUSPECTED],
            "recovering": self.recovery.is_recovering,
            "checkpoints": self.recovery.get_checkpoint_info()
        }
    
    async def is_majority(self) -> bool:
        """Check if we have majority of nodes"""
        live_nodes = await self.registry.get_live_nodes()
        if self.node_id not in live_nodes:
            live_nodes.append(self.node_id)
        return len(live_nodes) > len(self._get_all_nodes()) // 2