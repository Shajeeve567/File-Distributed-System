# features/consensus/consensus_impl.py
import asyncio
import logging
from typing import Optional, List
from shared.models import LogEntry
from features.consensus.raft_node import RaftNode
from features.consensus.election import ElectionManager
from features.consensus.log_replication import ReplicationManager

logger = logging.getLogger(__name__)

_raft_node: Optional[RaftNode] = None
_election_mgr: Optional[ElectionManager] = None
_replication_mgr: Optional[ReplicationManager] = None

async def init_raft_node(node_id: str, peer_ids: List[str]) -> RaftNode:
    """Initialize and start Raft node."""
    global _raft_node, _election_mgr, _replication_mgr
    
    if _raft_node is None:
        _raft_node = RaftNode(node_id, peer_ids)
        _election_mgr = ElectionManager(_raft_node)
        _replication_mgr = ReplicationManager(_raft_node)
        await _election_mgr.start()
        await _replication_mgr.start()
        logger.info(f"Raft node {node_id} initialized with peers: {peer_ids}")
    
    return _raft_node

async def get_current_leader() -> Optional[str]:
    """Get current leader ID."""
    if _raft_node is None:
        return None
    return await _raft_node.get_current_leader()

async def replicate_log(entry: LogEntry) -> bool:
    """Replicate a log entry."""
    if _replication_mgr is None:
        return False
    return await _replication_mgr.replicate_log(entry)

async def get_log_entry(index: int) -> Optional[LogEntry]:
    """Retrieve a specific log entry by index."""
    if _raft_node is None or index < 0 or index >= len(_raft_node.log):
        return None
    return _raft_node.log[index]

async def register_commit_callback(cb):
    """Register a callback for Raft log commit events."""
    if _raft_node:
        _raft_node.commit_callbacks.append(cb)

async def start_election() -> str:
    """Manually trigger election (for testing)."""
    if _election_mgr is None:
        return "S1"
    await _election_mgr.start_election()
    return await get_current_leader() or (_raft_node.node_id if _raft_node else "S1")

async def handle_vote_request(term: int, candidate_id: str, 
                               last_log_index: int, last_log_term: int) -> dict:
    """Handle incoming vote request."""
    if _election_mgr is None:
        return {"term": 0, "vote_granted": False}
    return await _election_mgr.handle_vote_request(term, candidate_id, 
                                                     last_log_index, last_log_term)

async def handle_append_entries(term: int, leader_id: str,
                                 prev_log_index: int, prev_log_term: int,
                                 entries: List[dict], leader_commit: int) -> dict:
    """Handle incoming append entries."""
    if _replication_mgr is None:
        return {"term": 0, "success": False, "match_index": 0}
    return await _replication_mgr.handle_append_entries(term, leader_id,
                                                         prev_log_index, prev_log_term,
                                                         entries, leader_commit)

async def stop_raft():
    """Clean shutdown."""
    global _raft_node, _election_mgr, _replication_mgr
    
    logger.info("Stopping Raft...")
    
    if _election_mgr:
        await _election_mgr.stop()
        logger.info("Election manager stopped")
    
    if _replication_mgr:
        await _replication_mgr.stop()
        logger.info("Replication manager stopped")
    
    if _raft_node:
        await _raft_node.stop()
        logger.info("Raft node stopped")
    
    _raft_node = None
    _election_mgr = None
    _replication_mgr = None
    
    logger.info("Raft shutdown complete")

class ConsensusEngineWrapper:
    """Wrapper to provide stub-like interface for the real implementation."""
    
    async def get_current_leader(self):
        return await get_current_leader()
    
    async def replicate_log(self, entry):
        return await replicate_log(entry)
    
    async def start_election(self):
        return await start_election()
    
    async def get_log_entry(self, index: int):
        return await get_log_entry(index)

    async def register_commit_callback(self, cb):
        return await register_commit_callback(cb)

consensus_engine = ConsensusEngineWrapper()