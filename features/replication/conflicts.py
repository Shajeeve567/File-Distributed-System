def next_version_for_write(current_version: int) -> int:
    return current_version + 1


def is_stale_write(expected_version: int, current_version: int) -> bool:
    return expected_version < current_version


def resolve_conflict(current_version: int, incoming_version: int, incoming_ts: float) -> int:
    """
    Simple policy:
    - newer version wins
    - if equal version somehow appears, accept incoming as next version
    """
    if incoming_version > current_version:
        return incoming_version
    return current_version + 1