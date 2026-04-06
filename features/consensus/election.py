import asyncio
import logging
import httpx
import random
from typing import List, Optional
from features.consensus.raft_node import RaftNode
from shared.config import config

logger = logging.getLogger(__name__)

class ElectionManager:
    """Manager class for Raft elections."""

    def __init__(self, raft_node: RaftNode):
        self.raft_node = raft_node
        self._running = True
        self._task: Optional[asyncio.Task] = None
        self._election_in_progress = False
        self._client: Optional[httpx.AsyncClient] = None


    async def start(self):
        self._running = True
        self._client = httpx.AsyncClient()
        self._task = asyncio.create_task(self._monitor_loop())
        logger.info(f"Election monitor started for {self.raft_node.node_id}")

    
    async def stop(self):
        self._running = False
        if self._client:
            await self._client.aclose()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    
    async def _monitor_loop(self):
        while self._running:
            try:
                await asyncio.sleep(0.1) 
                
                if (
                    not self._election_in_progress and
                    self.raft_node.state != "leader" and
                    await self.raft_node.should_start_election()
                ):
                    logger.info(f"ELECTION TRIGGERED for {self.raft_node.node_id}. Applying random.uniform timeout...")
                    import random
                    # If multiple nodes timeout simultaneously, add slight randomization before sending votes
                    await asyncio.sleep(random.uniform(0.05, 0.15))
                    asyncio.create_task(self.start_election())
            except Exception as e:
                logger.error(f"Election loop error on {self.raft_node.node_id}: {e}", exc_info=True)
                await asyncio.sleep(1.0) # Backoff


    
    async def start_election(self):
        if self._election_in_progress:
            return
        
        self._election_in_progress = True

        try:
            await self.raft_node.become_candidate()
            current_term = self.raft_node.current_term

            logger.info(
                f"Node {self.raft_node.node_id} starting election for term {current_term}"
            )

            tasks = [self._request_vote(peer) for peer in self.raft_node.peer_ids]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            if self.raft_node.state != "candidate":
                return

            votes = 1  # self vote

            for result in results:
                if isinstance(result, dict):
                    if result.get("term", 0) > current_term:
                        await self.raft_node.become_follower(result["term"], None)
                        return
                    if result.get("vote_granted"):
                        votes += 1
                elif isinstance(result, Exception):
                    logger.error(f"Vote request failed: {result}")

            total_nodes = len(self.raft_node.peer_ids) + 1
            majority = total_nodes // 2 + 1

            logger.info(
                f"Node {self.raft_node.node_id} received {votes}/{majority} votes"
            )

            if votes >= majority and self.raft_node.state == "candidate":
                await self.raft_node.become_leader()
                await self.raft_node.reset_election_timeout()

                logger.info(
                    f"Node {self.raft_node.node_id} is now LEADER for term {self.raft_node.current_term}"
                )

                # Ensure heartbeat loop reacts to leader state
            else:
                logger.info(
                    f"Node {self.raft_node.node_id} failed to win election (votes={votes}/{majority})"
                )


        finally:
            self._election_in_progress = False

    async def _request_vote(self, peer_id: str):
        """Send RequestVote RPC to peer using HTTP."""
        peer_url = config.ALL_NODES.get(peer_id)
        if not peer_url or not self._client:
            return {"term": 0, "vote_granted": False}

        last_log_index = len(self.raft_node.log)
        last_log_term = self.raft_node.log[-1].term if self.raft_node.log else 0

        data = {
            "term": self.raft_node.current_term,
            "candidate_id": self.raft_node.node_id,
            "last_log_index": last_log_index,
            "last_log_term": last_log_term
        }

        try:
            response = await self._client.post(
                f"{peer_url}/raft/vote",
                json=data,
                timeout=0.2
            )
            if response.status_code == 200:
                return response.json()
        except Exception as e:
            logger.debug(f"Failed to request vote from {peer_id}: {e}")
        
        return {"term": 0, "vote_granted": False}


    async def handle_vote_request(
        self,
        term: int,
        candidate_id: str,
        last_log_index: int,
        last_log_term: int,
    ) -> dict:

        async with self.raft_node._lock:
            if term > self.raft_node.current_term:
                logger.info(f"Term higher than current ({term} > {self.raft_node.current_term}), stepping down.")
                await self.raft_node.become_follower(term, None)

            vote_granted = False
            reason = "Unknown"


            if term < self.raft_node.current_term:
                reason = f"Term out of date ({term} < {self.raft_node.current_term})"
            elif term == self.raft_node.current_term:
                if (
                    self.raft_node.voted_for is not None
                    and self.raft_node.voted_for != candidate_id
                ):
                    reason = f"Already voted for {self.raft_node.voted_for}"
                else:
                    last_log_term_self = (
                        self.raft_node.log[-1].term
                        if self.raft_node.log else 0
                    )
                    last_log_index_self = len(self.raft_node.log)

                    if (
                        last_log_term > last_log_term_self
                        or (
                            last_log_term == last_log_term_self
                            and last_log_index >= last_log_index_self
                        )
                    ):
                        vote_granted = True
                        self.raft_node.voted_for = candidate_id
                        await self.raft_node.reset_election_timeout()
                        logger.info(
                            f"{self.raft_node.node_id} voted for {candidate_id} (term {term})"
                        )
                    else:
                        reason = "Candidate log not up to date"

            if not vote_granted:
                logger.info(f"{self.raft_node.node_id} denied vote to {candidate_id}: {reason}")

            return {
                "term": self.raft_node.current_term,
                "vote_granted": vote_granted,
            }

