def write_quorum_required(replication_factor: int = 3) -> int:
    """
    Basic majority rule.
    For 3 replicas -> 2 required.
    """
    return (replication_factor // 2) + 1


def is_write_successful(ack_count: int, required_acks: int) -> bool:
    return ack_count >= required_acks


def classify_write_state(ack_count: int, replication_factor: int) -> str:
    """
    Optional helper for reporting/demo.
    """
    if ack_count == replication_factor:
        return "FULLY_REPLICATED"
    if ack_count >= write_quorum_required(replication_factor):
        return "QUORUM_COMMITTED"
    return "FAILED"