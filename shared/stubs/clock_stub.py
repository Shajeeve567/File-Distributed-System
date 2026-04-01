from shared.models import ClockStatus 
from typing import Dict, List

# Simulated clock offsets (ms)
_OFFSETS_MS: Dict[str, int] = {
    "S1": 0,
    "S2": 4,   # 4ms ahead
    "S3": -3,  # 3ms behind
}

# Maximum allowed offset for "in sync" status
_MAX_OFFSET_MS: int = 50

class StubClockMonitor:

    def collect_offsets(self, node_ids: List[str]) -> Dict[str, int]:
        """Return simulated clock offsets for given nodes."""
        return {node_id: _OFFSETS_MS.get(node_id, 0) for node_id in node_ids}

    def cluster_time_status(self, node_ids: List[str]) -> List[ClockStatus]:
        """Return sync status for each node."""
        statuses = []
        for node_id in node_ids:
            offset = _OFFSETS_MS.get(node_id, 0)
            in_sync = abs(offset) <= _MAX_OFFSET_MS
            statuses.append(ClockStatus(
                node_id=node_id,
                offset_ms=offset,
                in_sync=in_sync
            ))
        return statuses


# Helper for tests to simulate clock drift
def set_node_offset(node_id: str, offset_ms: int):
    """Utility for tests to simulate clock skew."""
    _OFFSETS_MS[node_id] = offset_ms


def set_max_allowed_offset(max_offset: int):
    """Utility for tests to change sync threshold."""
    global _MAX_OFFSET_MS
    _MAX_OFFSET_MS = max_offset


# Export the instance that dependencies.py imports
clock_monitor = StubClockMonitor()