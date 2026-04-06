def resolve_conflict(local_lamport: int, incoming_lamport: int, local_node_id: str, incoming_node_id: str) -> bool:
    """
    Deterministic Conflict Resolution:
    - Higher Lamport timestamp wins
    - If equal, lexicographically larger Node ID wins
    
    Guarantees deterministic convergence across all nodes without manual conflict resolution.
    Returns True if incoming data wins, False if local data wins.
    """
    if incoming_lamport > local_lamport:
        return True
    elif incoming_lamport == local_lamport:
        return incoming_node_id > local_node_id
    return False