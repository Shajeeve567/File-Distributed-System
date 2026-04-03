from shared.models import ClockStatus

def compute_offsets(node_times: dict[str, float], reference_node: str = "S1") -> dict[str, int]:
    """
    Calculate time offset in milliseconds.
    
    IMPORTANT: Multiply by 1000 to convert seconds to milliseconds!
    """
    if reference_node not in node_times:
        raise ValueError(f"Reference node {reference_node} not found")
    
    reference_time = node_times[reference_node]
    offsets = {}

    for node_id, time in node_times.items():
        # FIXED: Multiply by 1000 to get milliseconds
        offset_ms = int((time - reference_time) * 1000)
        offsets[node_id] = offset_ms
        
    return offsets


def assess_cluster_health(
    offsets: dict[str, int],
    max_offset_ms: int = 50
) -> tuple[list[ClockStatus], bool]:
    """Assess health of each node based on clock offset."""
    statuses = []
    cluster_healthy = True

    for node_id, offset in offsets.items():
        in_sync = abs(offset) <= max_offset_ms

        status = ClockStatus(
            node_id=node_id,
            offset_ms=offset,
            in_sync=in_sync
        )
        statuses.append(status)

        if not in_sync:
            cluster_healthy = False
            
    return statuses, cluster_healthy