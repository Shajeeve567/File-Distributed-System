import asyncio
import httpx
import time
from typing import Dict, List, Optional
from shared.utils import logger

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
        self.running = True
        asyncio.create_task(self._process_queue())
        logger.info("Replication processor started")

    async def stop(self):
        """Stop replication manager"""
        self.running = False
        await self.client.aclose()

    async def replicate_block(self, block_id: str, data: bytes, target_nodes: List[str]):
        """Queue a binary block for replication"""
        await self.replication_queue.put({
            "type": "BLOCK",
            "block_id": block_id,
            "data": data,
            "targets": target_nodes,
            "attempts": 0
        })
        logger.info(f"💾 Queued block replication for {block_id} to {len(target_nodes)} nodes")

    async def replicate_metadata(self, filename: str, manifest: dict, target_nodes: List[str]):
        """Queue a metadata manifest for replication"""
        await self.replication_queue.put({
            "type": "METADATA",
            "filename": filename,
            "manifest": manifest,
            "targets": target_nodes,
            "attempts": 0
        })
        logger.info(f"📋 Queued metadata replication for {filename} to {len(target_nodes)} nodes")

    async def _process_queue(self):
        """Background worker that processes replication queue"""
        while self.running:
            try:
                task = await self.replication_queue.get()
                
                # Increment attempts
                task["attempts"] += 1
                
                # Replicate to all targets
                if task.get("type") == "METADATA":
                    results = await self._replicate_meta_to_nodes(task)
                else:
                    results = await self._replicate_to_nodes(task)
                
                # Check results
                successful = [n for n, success in results.items() if success]
                failed = [n for n, success in results.items() if not success]
                
                self.stats["total_replications"] += 1
                self.stats["successful"] += len(successful)
                self.stats["failed"] += len(failed)
                
                # Retry if some failed
                if failed and task["attempts"] < 3:
                    await asyncio.sleep(2)
                    await self.replication_queue.put(task)
                    self.stats["retries"] += 1
                
                self.replication_queue.task_done()
            except Exception as e:
                logger.error(f"Error in replication processor: {e}")
                await asyncio.sleep(1)

    async def _replicate_to_nodes(self, task: dict):
        """Helper to send binary blocks to specific nodes (JSON-Encoded for robustness)"""
        block_id = task["block_id"]
        data = task["data"]
        targets = task["targets"]
        results = {}
        from shared.config import config
        
        for peer_id in targets:
            peer_url = config.ALL_NODES.get(peer_id)
            if not peer_url: continue
            try:
                resp = await self.client.post(
                    f"{peer_url}/replicate",
                    json={
                        "block_id": block_id,
                        "data": data.hex(),
                        "source_node": config.NODE_ID
                    },
                    timeout=2.0
                )
                results[peer_id] = resp.status_code == 200
                if results[peer_id]:
                    logger.info(f"✨ Replicated block {block_id} to {peer_id}")
            except Exception as e:
                logger.error(f"Failed to replicate block {block_id} to {peer_id}: {e}")
                results[peer_id] = False
        return results

    async def _replicate_meta_to_nodes(self, task: dict):
        """Helper to send metadata manifests to specific nodes"""
        filename = task["filename"]
        manifest = task["manifest"]
        targets = task["targets"]
        results = {}
        from shared.config import config
        
        for peer_id in targets:
            peer_url = config.ALL_NODES.get(peer_id)
            if not peer_url: continue
            try:
                resp = await self.client.post(
                    f"{peer_url}/replicate_meta",
                    json={
                        "filename": filename,
                        "manifest": manifest
                    },
                    timeout=2.0
                )
                results[peer_id] = resp.status_code == 200
                if results[peer_id]:
                    logger.info(f"✨ Mirrored metadata {filename} to {peer_id}")
            except Exception as e:
                logger.error(f"Failed to replicate metadata {filename} to {peer_id}: {e}")
                results[peer_id] = False
        return results

    def get_stats(self) -> Dict:
        """Get replication statistics"""
        return self.stats.copy()