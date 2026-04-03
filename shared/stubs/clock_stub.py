from __future__ import annotations

from threading import Lock

from shared.models import ClockStatus


class StubClockMonitor:
    """
    Minimal time-sync stub.

    Not critical for your replication write path,
    but included so shared.dependencies is complete.
    """

    def __init__(self) -> None:
        self._lock = Lock()
        self._offsets_ms: dict[str, int] = {
            "S1": 0,
            "S2": 4,
            "S3": -3,
        }
        self._max_offset_ms = 50

    def collect_offsets(self, node_ids: list[str]) -> dict[str, int]:
        with self._lock:
            return {node_id: self._offsets_ms.get(node_id, 0) for node_id in node_ids}

    def cluster_time_status(self, node_ids: list[str]) -> list[ClockStatus]:
        with self._lock:
            statuses: list[ClockStatus] = []
            for node_id in node_ids:
                offset = self._offsets_ms.get(node_id, 0)
                statuses.append(
                    ClockStatus(
                        node_id=node_id,
                        offset_ms=offset,
                        in_sync=abs(offset) <= self._max_offset_ms,
                    )
                )
            return statuses

    # -------- test helpers --------

    def set_offset(self, node_id: str, offset_ms: int) -> None:
        with self._lock:
            self._offsets_ms[node_id] = offset_ms

    def set_max_offset(self, max_offset_ms: int) -> None:
        with self._lock:
            self._max_offset_ms = max_offset_ms


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