from __future__ import annotations

from threading import Lock
from typing import Optional

from shared.models import LogEntry


class StubConsensusEngine:
    """
    Minimal consensus stub so the rest of the system can run.

    Your replication module does not need deep consensus logic,
    but shared.dependencies can expose this cleanly.
    """

    def __init__(self) -> None:
        self._lock = Lock()
        self._current_leader: str = "COORDINATOR_1"
        self._term: int = 1
        self._log: list[LogEntry] = []

    def get_current_leader(self) -> Optional[str]:
        with self._lock:
            return self._current_leader

    def replicate_log(self, entry: LogEntry) -> bool:
        with self._lock:
            self._log.append(entry.model_copy(deep=True))
            return True

    def start_election(self) -> str:
        with self._lock:
            self._term += 1
            self._current_leader = "COORDINATOR_1"
            return self._current_leader

    # -------- test helpers --------

    def force_leader(self, leader_id: str) -> None:
        with self._lock:
            self._current_leader = leader_id

    def get_log(self) -> list[LogEntry]:
        with self._lock:
            return [entry.model_copy(deep=True) for entry in self._log]


consensus_engine = StubConsensusEngine()