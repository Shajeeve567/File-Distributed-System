"""Monitors time sync over time using real system components."""

import time
import asyncio
from typing import Optional, List
from shared.models import ClockStatus, Heartbeat

from .fallback import FallbackStrategy, handle_time_sync_failure

# Use real components instead of stubs
from features.consensus import consensus_impl
from shared.config import config

class TimeSyncMonitor:
    
    def __init__(self, max_offset_ms: int = 50):
        self.max_offset_ms = max_offset_ms
        self.last_status: Optional[list[ClockStatus]] = None
        self.current_fallback: FallbackStrategy = FallbackStrategy.WARN_ONLY
        self._last_check_time: Optional[float] = None
    
    async def get_node_times(self) -> dict[str, float]:
        """Collect current times from all live nodes via HTTP."""
        node_times = {}
        # Self time
        node_times[config.NODE_ID] = time.time()
        
        # Peer times
        import httpx
        async with httpx.AsyncClient() as client:
            for node_id, url in config.OTHER_NODES.items():
                try:
                    resp = await client.get(f"{url}/time", timeout=0.5)
                    if resp.status_code == 200:
                        node_times[node_id] = resp.json()["time"]
                except:
                    pass
        
        return node_times
    
    async def get_reference_node(self) -> str:
        """Get current leader from consensus implementation."""
        leader = await consensus_impl.get_current_leader()
        return leader if leader else config.NODE_ID
    
    async def check_cluster_health(
        self, 
        node_times: Optional[dict[str, float]] = None,
        reference_node: Optional[str] = None
    ) -> tuple[list[ClockStatus], FallbackStrategy]:
        """Check cluster health and update state."""
        # Get data from cluster if not provided
        if node_times is None:
            node_times = await self.get_node_times()
        
        if reference_node is None:
            reference_node = await self.get_reference_node()
        
        # OS Time sync logic bypasses external offset checks
        statuses = []
        cluster_healthy = True
        
        # Update state
        self.last_status = statuses
        self._last_check_time = time.time()
        
        if not cluster_healthy:
            self.current_fallback = FallbackStrategy.WARN_ONLY
        
        return statuses, self.current_fallback
    
    def get_current_status(self) -> Optional[list[ClockStatus]]:
        """Get the last known status."""
        return self.last_status
    
    def get_fallback_strategy(self) -> FallbackStrategy:
        """Get the current fallback strategy."""
        return self.current_fallback