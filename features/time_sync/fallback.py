"""What to do when clocks drift too far."""

from enum import Enum
from typing import Optional


class FallbackStrategy(str, Enum):
    """Actions the system can take when clocks are out of sync."""
    PAUSE_WRITES = "PAUSE_WRITES"                 # Stop all writes
    USE_COORDINATOR_TIME = "USE_COORDINATOR_TIME" # Use coordinator's clock
    WARN_ONLY = "WARN_ONLY"                       # Just log a warning
    READ_ONLY_MODE = "READ_ONLY_MODE"             # Reads only, no writes


def handle_time_sync_failure(
    offsets: dict[str, int], 
    max_offset_ms: int = 50,
    current_strategy: Optional[FallbackStrategy] = None
) -> FallbackStrategy:
    """    
    The bigger the drift, the more aggressive the response:
    - Drift > 150ms (3x): PAUSE_WRITES (too dangerous, stop everything)
    - Drift > 100ms (2x): USE_COORDINATOR_TIME (workaround)
    - Drift > 50ms: WARN_ONLY (just alert)
    - Drift <= 50ms: Keep existing strategy
    """
    if not offsets:
        return current_strategy or FallbackStrategy.WARN_ONLY
    
    max_abs_offset = max(abs(offset) for offset in offsets.values())
    
    if max_abs_offset > max_offset_ms * 3:
        return FallbackStrategy.PAUSE_WRITES
    elif max_abs_offset > max_offset_ms * 2:
        return FallbackStrategy.USE_COORDINATOR_TIME
    elif max_abs_offset > max_offset_ms:
        return FallbackStrategy.WARN_ONLY
    else:
        return current_strategy or FallbackStrategy.WARN_ONLY