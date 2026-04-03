import asyncio
import time 
import random 
import logging 

from typing import Optional, List, Dict
from shared.models import LogEntry

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RaftNode: 

    def __init__(self, node_id: str, peer_ids: List[str]):

        self.node_id = node_id
        self.peer_ids = peer_ids 

        # persistent states
        self.current_term = 1
        self.voted_for: Optional[str] = None
        self.log: List[LogEntry] = [] 

        # volatile state 
        self.commit_index = 0
        self.last_applied = 0
        self.commit_callbacks = []
        
        # Lock for thread-safety 
        self.next_index: Dict[str, int] = {} 
        self.match_index: Dict[str, int] = {} 

        # node state
        self.state = "follower" # follower, candidate, leader 
        self.leader_id: Optional[str] = None 

        # timing (Randomized between 3 and 6 seconds to prevent split votes)
        # Seed with a combination of ID and time to avoid identical sequences in sub-processes
        random.seed(hash(node_id) + time.time())
        self.election_timeout = random.uniform(3000, 6000) / 1000
        
        # Initial jitter: Start with a random offset to ensure they don't timeout at the same time in Term 1
        self.last_heartbeat = asyncio.get_event_loop().time() - random.uniform(0, 2)

        self._lock = asyncio.Lock() 

    async def become_follower(self, term: int, leader_id: Optional[str] = None):
        # change into follower state 
        # Only reset voted_for if the term is strictly greater
        if term > self.current_term:
            self.voted_for = None
        
        self.state = "follower"
        self.current_term = max(self.current_term, term)
        self.leader_id = leader_id 
        self.last_heartbeat = asyncio.get_event_loop().time() 
        # Reset timeout to stable range
        self.election_timeout = random.uniform(3000, 6000) / 1000

        logger.info(f"Node {self.node_id} became FOLLOWER (term {self.current_term}, leader {leader_id})")
    
    async def become_candidate(self): 
        # change into candidate state 
        self.state = "candidate"
        self.current_term += 1
        self.voted_for = self.node_id
        self.last_heartbeat = asyncio.get_event_loop().time()
        # Reset timeout for new election attempt
        self.election_timeout = random.uniform(3000, 6000) / 1000

        logger.info(f"Node {self.node_id} became a CANDIDATE for term {self.current_term}")
        
    async def become_leader(self): 
        # become a leader
        self.state = "leader"
        self.leader_id = self.node_id
        
        last_log_index = len(self.log)
        for peer in self.peer_ids: 

            self.next_index[peer] = last_log_index + 1
            self.match_index[peer] = 0
        
        logger.info(f"Node {self.node_id} became LEADER for term {self.current_term}")
        
    # leader only operation
    async def append_entry(self, entry: LogEntry) -> bool:
        # append entry to log
        if self.state != "leader":
            return False
        
        entry.term = self.current_term
        entry.index = len(self.log) + 1
        self.log.append(entry)

        logger.info(f"Leader has appended the entry: {entry.index}")
        if not self.peer_ids:

            self.commit_index = len(self.log)
            logger.info(f"No peers, immediately committed up to index {self.commit_index}")

            await self._apply_committed_entries()
        return True 

    async def update_commit_index(self):
        # update index when majority replicated
        if self.state != "leader":
            return
        
        if not self.peer_ids:
            if len(self.log) > self.commit_index:
                self.commit_index = len(self.log)
                logger.info(f"✅ No peers, committed up to index {self.commit_index}")

        matches = list(self.match_index.values()) + [len(self.log)]
        matches.sort(reverse=True)
        majority_index = matches[len(matches) // 2]

        logger.info(f"🎯 Update commit: matches={matches}, majority_index={majority_index}, current_commit={self.commit_index}")

        if majority_index > self.commit_index:
            if majority_index > 0 and self.log[majority_index - 1].term == self.current_term:
                self.commit_index = majority_index

                logger.info(f"✅✅✅ COMMIT UPDATED to index {self.commit_index} ✅✅✅")
                logger.info(f"Leader committed up to index {self.commit_index}")

                await self._apply_committed_entries()
    
    async def _apply_committed_entries(self):
        # apply committed entries to state machine
        while self.last_applied < self.commit_index: 
            self.last_applied += 1
            entry = self.log[self.last_applied - 1]

            logger.info(f"📈 [NODE {self.node_id}] Applying committed entry {self.last_applied}: {entry.op}")

            # Execute callbacks
            for cb in self.commit_callbacks:
                try:
                    await cb(entry)
                except Exception as e:
                    logger.error(f"Error in Raft commit callback: {e}")

    async def get_current_leader(self) -> Optional[str]:
        # Assume caller holds lock
        if self.state == "leader":
            return self.node_id
        return self.leader_id
    
    async def should_start_election(self) -> bool:
        # Assume caller holds lock
        if self.state == "leader": 
            return False
        
        now = asyncio.get_event_loop().time()
        elapsed = now - self.last_heartbeat
        
        # Robust comparison including small grace period if needed
        return elapsed > self.election_timeout
    
    async def reset_election_timeout(self):
        # Assume caller holds lock
        # Consistent with monitor loop
        self.last_heartbeat = asyncio.get_event_loop().time()
        # 3.0s - 6.0s randomized range
        self.election_timeout = random.uniform(3000, 6000) / 1000




    async def stop(self):
        """Stop the Raft node."""
        logger.info(f"Raft node {self.node_id} stopping")
        # Add any cleanup here if needed