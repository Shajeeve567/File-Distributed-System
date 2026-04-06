import asyncio 
import logging
import httpx
from typing import List, Optional
from shared.models import LogEntry
from features.consensus.raft_node import RaftNode
from shared.config import config

logger = logging.getLogger(__name__)

class ReplicationManager: 
    """Manager class for log replication."""

    def __init__(self, raft_node: RaftNode):
        self.raft_node = raft_node
        self._running = True 
        self._task: Optional[asyncio.Task] = None 
        self._client: Optional[httpx.AsyncClient] = None

    async def start(self):
        """Start heartbeats."""
        self._running = True
        self._client = httpx.AsyncClient()
        self._task = asyncio.create_task(self._heartbeat_loop())


    async def stop(self):
        """Stop heartbeats."""
        self._running = False
        if self._client:
            await self._client.aclose()
        if self._task:
            self._task.cancel()

            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _heartbeat_loop(self):
        """Send periodic heartbeats."""
        while self._running:
            await asyncio.sleep(0.05)
            
            if self.raft_node.state != "leader":
                continue
            
            await self._send_heartbeats()

    async def _send_heartbeats(self):
        """Send heartbeats to all peers."""
        async with self.raft_node._lock:
            peers = self.raft_node.peer_ids.copy()

        tasks = [self._append_entries(peer, is_heartbeat=True) for peer in peers]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def replicate_log(self, entry: LogEntry) -> bool:
        """Replicate a log entry to followers."""
        logger.info(f"replicate_log: starting for entry {entry.index}")
        
        # Append locally
        if not await self.raft_node.append_entry(entry):
            logger.error("Failed to append entry locally")
            return False
        
        logger.info(f"Entry {entry.index} appended locally")
        
        # Send to followers
        await self._send_heartbeats()
        
        # Wait for commit with timeout
        timeout = 3.0
        start = asyncio.get_event_loop().time()
        
        while True:
            await asyncio.sleep(0.1)
            
            async with self.raft_node._lock:
                if entry.index <= self.raft_node.commit_index:
                    logger.info(f"Entry {entry.index} committed!")
                    return True
            
            if asyncio.get_event_loop().time() - start > timeout:
                logger.error(f"Timeout waiting for entry {entry.index} to commit")
                return False

    async def _append_entries(self, peer_id: str, is_heartbeat: bool = False) -> bool:
        """Send AppendEntries RPC to peer via HTTP."""
        peer_url = config.ALL_NODES.get(peer_id)
        if not peer_url:
            return False

        async with self.raft_node._lock:
            if self.raft_node.state != "leader":
                return False
            
            prev_log_index = self.raft_node.next_index.get(peer_id, 1) - 1
            prev_log_term = 0
            if prev_log_index > 0 and prev_log_index <= len(self.raft_node.log):
                prev_log_term = self.raft_node.log[prev_log_index - 1].term
            
            entries = []
            if not is_heartbeat and prev_log_index < len(self.raft_node.log):
                entries = [e.dict() for e in self.raft_node.log[prev_log_index:]]

            data = {
                "term": self.raft_node.current_term,
                "leader_id": self.raft_node.node_id,
                "prev_log_index": prev_log_index,
                "prev_log_term": prev_log_term,
                "entries": entries,
                "leader_commit": self.raft_node.commit_index
            }

        try:
            if not self._client:
                return False
                
            response = await self._client.post(
                f"{peer_url}/raft/append_entries",
                json=data,
                timeout=0.2
            )
            if response.status_code == 200:

                    result = response.json()
                    return await self._handle_append_response(peer_id, result.get("success", False))
        except Exception as e:
            logger.debug(f"Failed to send append_entries to {peer_id}: {e}")
        
        return False

    async def _handle_append_response(self, peer_id: str, success: bool) -> bool:
        """Handle AppendEntries response."""
        async with self.raft_node._lock:
            if self.raft_node.state != "leader":
                return False
            
            if success:
                self.raft_node.match_index[peer_id] = len(self.raft_node.log)
                self.raft_node.next_index[peer_id] = len(self.raft_node.log) + 1
                updated = await self.raft_node.update_commit_index()
                if updated:
                    # Force immediate heartbeat to propagate new commit_index
                    asyncio.create_task(self._send_heartbeats())
            else:
                old_next = self.raft_node.next_index.get(peer_id, 1)
                self.raft_node.next_index[peer_id] = max(1, old_next - 1)
            
            return success

    async def handle_append_entries(self, term: int, leader_id: str, 
                                    prev_log_index: int, prev_log_term: int, 
                                    entries: List[dict], leader_commit: int) -> dict:
        """Handle incoming AppendEntries RPC from leader."""
        async with self.raft_node._lock:
            if term > self.raft_node.current_term:
                await self.raft_node.become_follower(term, leader_id)
            
            success = False
            if term == self.raft_node.current_term:
                await self.raft_node.become_follower(term, leader_id)
                await self.raft_node.reset_election_timeout()
                
                if (prev_log_index == 0 or 
                    (prev_log_index <= len(self.raft_node.log) and 
                     self.raft_node.log[prev_log_index - 1].term == prev_log_term)):
                    
                    # Append new entries
                    if entries:
                        # Clear conflicting entries
                        self.raft_node.log = self.raft_node.log[:prev_log_index]
                        for entry_data in entries:
                            entry = LogEntry(**entry_data)
                            self.raft_node.log.append(entry)
                    
                    if leader_commit > self.raft_node.commit_index:
                        old_commit = self.raft_node.commit_index
                        self.raft_node.commit_index = min(leader_commit, len(self.raft_node.log))
                        if self.raft_node.commit_index > old_commit:
                            logger.info(f"[NODE {self.raft_node.node_id}] Commit index updated to {self.raft_node.commit_index}")
                            await self.raft_node._apply_committed_entries()
                    
                    success = True
            
            return {
                "term": self.raft_node.current_term, 
                "success": success, 
                "match_index": len(self.raft_node.log)
            }
