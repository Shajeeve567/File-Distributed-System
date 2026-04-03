import asyncio
import logging
from typing import List, Optional
from features.consensus.raft_node import RaftNode

logger = logging.getLogger(__name__)

class ElectionManager:
    """Manager class for Raft elections."""

    def __init__(self, raft_node: RaftNode):
        self.raft_node = raft_node
        self._running = True
        self._task: Optional[asyncio.Task] = None
        self._election_in_progress = False   # ✅ NEW

    async def start(self):
        self._running = True
        self._task = asyncio.create_task(self._monitor_loop())
        logger.info(f"Election monitor started for {self.raft_node.node_id}")
    
    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
    
    async def _monitor_loop(self):
        while self._running:
            await asyncio.sleep(0.01)

            if (
                not self._election_in_progress and
                self.raft_node.state != "leader" and
                await self.raft_node.should_start_election()
            ):
                logger.info(f"Election timeout triggered for {self.raft_node.node_id}")
                asyncio.create_task(self.start_election())  # ✅ don't block loop
    
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

            # ❗ Check if still candidate
            if self.raft_node.state != "candidate":
                return

            votes = 1  # self vote

            for result in results:
                if isinstance(result, dict):
                    # ✅ handle real responses
                    if result.get("term", 0) > current_term:
                        await self.raft_node.become_follower(result["term"], None)
                        return
                    if result.get("vote_granted"):
                        votes += 1

                elif result is True:
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

                # ✅ IMPORTANT: trigger heartbeats immediately
                if hasattr(self.raft_node, "replication_manager"):
                    await self.raft_node.replication_manager._send_heartbeats()

            else:
                await self.raft_node.become_follower(
                    self.raft_node.current_term, None
                )

        finally:
            self._election_in_progress = False

    async def _request_vote(self, peer_id: str):
        await asyncio.sleep(0.01)

        logger.debug(f"{self.raft_node.node_id} requesting vote from {peer_id}")

        # Simulated response (replace with HTTP later)
        return {
            "term": self.raft_node.current_term,
            "vote_granted": True
        }

    async def handle_vote_request(
        self,
        term: int,
        candidate_id: str,
        last_log_index: int,
        last_log_term: int,
    ) -> dict:

        async with self.raft_node._lock:

            if term > self.raft_node.current_term:
                await self.raft_node.become_follower(term, None)

            vote_granted = False

            if term == self.raft_node.current_term:
                if (
                    self.raft_node.voted_for is None
                    or self.raft_node.voted_for == candidate_id
                ):

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

            return {
                "term": self.raft_node.current_term,
                "vote_granted": vote_granted,
            }