from __future__ import annotations

import time
from threading import Lock

from shared.models import Heartbeat


class StubNodeHealthGateway:
    """
    Liveness stub.

    Your replication layer uses this to:
    - filter valid live replica targets
    - simulate degraded states
    """

    def __init__(self) -> None:
        self._lock = Lock()
        self._live_nodes: dict[str, bool] = {
            "S1": True,
            "S2": True,
            "S3": True,
        }
        self._last_seen: dict[str, float] = {
            "S1": time.time(),
            "S2": time.time(),
            "S3": time.time(),
        }

    def heartbeat(self, node_id: str) -> Heartbeat:
        with self._lock:
            now = time.time()
            is_alive = self._live_nodes.get(node_id, False)
            if is_alive:
                self._last_seen[node_id] = now

            return Heartbeat(
                node_id=node_id,
                is_alive=is_alive,
                timestamp=self._last_seen.get(node_id, now),
            )

    def list_live_nodes(self) -> list[str]:
        with self._lock:
            return [node_id for node_id, alive in self._live_nodes.items() if alive]

    def is_node_alive(self, node_id: str) -> bool:
        with self._lock:
            return self._live_nodes.get(node_id, False)

    # -------- test helpers --------

    def set_node_alive(self, node_id: str, alive: bool) -> None:
        with self._lock:
            self._live_nodes[node_id] = alive
            if alive:
                self._last_seen[node_id] = time.time()

    def reset_all_alive(self) -> None:
        with self._lock:
            now = time.time()
            for node_id in list(self._live_nodes.keys()):
                self._live_nodes[node_id] = True
                self._last_seen[node_id] = now


node_health_gateway = StubNodeHealthGateway()