from typing import List, Dict
from shared.models import ClockStatus
from shared.interfaces import ClockMonitor

from .monitor import TimeSyncMonitor
from .clock_sync import compute_offsets


class RealClockMonitor(ClockMonitor):
    
    def __init__(self, max_offset_ms: int = 50):
        self.monitor = TimeSyncMonitor(max_offset_ms)
    
    def collect_offsets(self, node_ids: List[str]) -> Dict[str, int]:
        """Get time offsets for all nodes."""
        # Get timestamps from Member 1's stub
        node_times = self.monitor.get_heartbeat_timestamps(node_ids)
        
        # Get leader from Member 4's stub as reference
        reference_node = self.monitor.get_reference_node()
        
        # Calculate offsets
        offsets = compute_offsets(node_times, reference_node)
        
        return offsets
    
    def cluster_time_status(
        self, 
        node_ids: List[str], 
        max_offset_ms: int = 50
    ) -> List[ClockStatus]:
        """Get time sync status for each node."""
        offsets = self.collect_offsets(node_ids)
        
        statuses = []
        for node_id in node_ids:
            offset = offsets.get(node_id, 0)
            in_sync = abs(offset) <= max_offset_ms
            
            status = ClockStatus(
                node_id=node_id,
                offset_ms=offset,
                in_sync=in_sync
            )
            statuses.append(status)
        
        return statuses
    
    def get_fallback_strategy(self) -> str:
        """Get current fallback strategy as string."""
        return self.monitor.get_fallback_strategy().value


def get_clock_monitor():
    """Factory function - called by shared/dependencies.py when USE_STUB_CLOCK=0."""
    return RealClockMonitor()