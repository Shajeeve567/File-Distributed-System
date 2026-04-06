import asyncio
import time
from enum import Enum
from typing import Dict, List, Optional
from shared.utils import logger

class NodeStatus(Enum):
    HEALTHY = "healthy"
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
        self.suspect_timeout = 2  # ~2 seconds (1 missed heartbeat)
        self.failure_timeout = 4  # ~4 seconds (3 missed heartbeats)
        self.heartbeat_interval = 1  # 1 second loop wait
        
        self.running = True
        self._start_time = time.time()
        
        logger.info(f"FaultToleranceManager initialized for {node_id}")
    
    # Add this method anywhere in the class
    def _get_start_time(self):
        """Get start time for uptime calculation"""
        return self._start_time
    
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
            await asyncio.sleep(self.heartbeat_interval)
            
            # Get current live nodes based purely on explicit timestamps
            now = time.time()
            # Direct lock-free access isn't perfect, but checking registry timestamps is better
            
            for node_id in self._get_all_nodes():
                if node_id == self.node_id:
                    continue
                
                last_seen_time = self.registry.live_nodes.get(node_id, 0)
                time_since_last_seen = now - last_seen_time
                current_status = self.node_status.get(node_id, NodeStatus.HEALTHY)
                
                # If recently seen within suspect timeout
                if time_since_last_seen <= self.suspect_timeout:
                    if current_status != NodeStatus.HEALTHY:
                        if current_status == NodeStatus.FAILED:
                            # Node was previously dead, now recovered
                            logger.info(f"[FAULT] Node {node_id} is BACK ONLINE and RECOVERING")
                            self.node_status[node_id] = NodeStatus.RECOVERING
                            
                            # Trigger asynchronous non-blocking recovery
                            asyncio.create_task(self._run_async_recovery(node_id))
                        elif current_status == NodeStatus.SUSPECTED:
                            self.node_status[node_id] = NodeStatus.HEALTHY
                            logger.info(f"[FAULT] Node {node_id} recovered from SUSPECTED to HEALTHY")
                
                # If missed exactly ~1 heartbeat boundaries
                elif self.suspect_timeout < time_since_last_seen <= self.failure_timeout:
                    if current_status == NodeStatus.HEALTHY:
                        self.node_status[node_id] = NodeStatus.SUSPECTED
                        logger.warning(f"[FAULT] Node {node_id} is SUSPECTED (missed heartbeat)")
                
                # If missed >= 3 consecutive bounds
                elif time_since_last_seen > self.failure_timeout:
                    if current_status == NodeStatus.SUSPECTED or current_status == NodeStatus.HEALTHY:
                        self.node_status[node_id] = NodeStatus.FAILED
                        logger.error(f"[FAULT] Node {node_id} has FAILED")
                        
                        file_map = await self._get_file_map()
                        asyncio.create_task(self.recovery.handle_node_failure(node_id, file_map))

    async def _run_async_recovery(self, node_id: str):
        """Asynchronous execution pipe for recovery to avoid blocking other systems"""
        file_map = await self._get_file_map()
        await self.recovery.handle_node_recovery(node_id, file_map)
        
        # Only mark HEALTHY after full sync completes safely
        self.node_status[node_id] = NodeStatus.HEALTHY
        logger.info(f"[FAULT] Node {node_id} fully synced and state is now HEALTHY")
    
    def _get_all_nodes(self) -> List[str]:
        """Get list of all nodes in cluster"""
        from shared.config import config
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