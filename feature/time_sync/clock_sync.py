from shared.models import ClockStatus

def compute_offsets(node_times: dict[str ,float], reference_node: str = "S1") -> dict[str, int]:
    """
    Calculate how many milliseconds each node's clock is off.
    
    Example: If S1 = 100.123, S2 = 100.127
    Then S2 is 4 milliseconds ahead of S1
    
    Returns: {"S1": 0, "S2": 4, "S3": -5}
    """

    if reference_node not in node_times:
        raise ValueError(f"Reference node {reference_node} not found")
    
    reference_time = node_times[reference_node]
    offsets = {}

    for node_id, time in node_times.items():
        # Convert seconds difference to milliseconds
        offsets_ms = int((time - reference_time))
        offsets[node_id] = offsets_ms
        
    return offsets

def assess_cluster_health(
    offsets: dict[str, int],
    max_offset_ms: int = 50
) -> tuple[list[ClockStatus] , bool]:
    """
    Classify each node based on how far its clock is off.
    
    Rules:
    - <= 50ms: IN_SYNC (safe)
    - 51-100ms: DRIFT_WARNING (trouble coming)
    - > 100ms: OUT_OF_SYNC (dangerous)
    
    Returns:
        (list of ClockStatus objects, whether cluster is healthy)
    """
    statuses = []
    cluster_healthy = True

    for node_id, offset in offsets.items():
        in_sync = abs(offset) <= max_offset_ms

        status = ClockStatus(
            node_id = node_id,
            offset_ms = offset,
            in_sync = in_sync
        )
        statuses.append(status)

        if not in_sync:
            cluster_healthy = False
    return statuses, cluster_healthy