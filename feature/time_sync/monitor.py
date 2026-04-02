"""Monitors time sync over time - uses stubs from Member 1 and Member 4."""

import time
from typing import Optional, List
from shared.models import ClockStatus, Heartbeat
from .clock_sync import compute_offsets,assess_cluster_health
from .fallback import FallbackStrategy,handle_time_sync_failure

# Import other members' stubs
from shared.stubs.node_health_stub import get_node_health_gateway
from shared.stubs.consensus_stub import get_consensus_engine


class TimeSyncMonitor:
    
    
    def __init__(self, max_offset_ms: int = 50):
        self.max_offset_ms = max_offset_ms
        self.last_status: Optional[list[ClockStatus]] = None
        self.current_fallback: FallbackStrategy = FallbackStrategy.WARN_ONLY
        self._last_check_time: Optional[float] = None
        
        # Get references to other members' stubs
        self.health_gateway = get_node_health_gateway()
        self.consensus_engine = get_consensus_engine()
    
    def get_heartbeat_timestamps(self, node_ids: Optional[List[str]] = None) -> dict[str, float]:
        
        if node_ids is None:
            node_ids = self.health_gateway.list_live_nodes()
        
        timestamps = {}
        for node_id in node_ids:
            heartbeat: Heartbeat = self.health_gateway.heartbeat(node_id)
            timestamps[node_id] = heartbeat.timestamp
        
        return timestamps
    
    def get_reference_node(self) -> str:
        
        leader = self.consensus_engine.get_current_leader()
        return leader if leader else "S1"  # Fallback to S1 if no leader
    
    def check_cluster_health(
        self, 
        node_times: Optional[dict[str, float]] = None,
        reference_node: Optional[str] = None
    ) -> tuple[list[ClockStatus], FallbackStrategy]:
        
        # Get data from stubs if not provided
        if node_times is None:
            node_times = self.get_heartbeat_timestamps()
        
        if reference_node is None:
            reference_node = self.get_reference_node()
        
        # Calculate offsets and assess health
        offsets = compute_offsets(node_times, reference_node)
        statuses, cluster_healthy = assess_cluster_health(offsets, self.max_offset_ms)
        
        # Update state
        self.last_status = statuses
        self._last_check_time = time.time()
        
        # Update fallback strategy if cluster is unhealthy
        if not cluster_healthy:
            self.current_fallback = handle_time_sync_failure(
                offsets, 
                self.max_offset_ms, 
                self.current_fallback
            )
        
        return statuses, self.current_fallback
    
    def get_current_status(self) -> Optional[list[ClockStatus]]:
        """Get the last known status. Other services call this."""
        return self.last_status
    
    def get_fallback_strategy(self) -> FallbackStrategy:
        """Get the current fallback strategy. Coordinator uses this."""
        return self.current_fallback