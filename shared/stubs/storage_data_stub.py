from __future__ import annotations

from threading import Lock
from typing import Optional

from shared.models import StoredObject, WriteRequest


class StubStorageDataGateway:
    """
    In-memory storage stub for S1/S2/S3.

    Your replication layer can call:
    - store_on_node(...)
    - read_from_node(...)
    - delete_from_node(...)

    You can also inject failures per node for testing degraded writes.
    """

    def __init__(self) -> None:
        self._lock = Lock()
        self._storage: dict[str, dict[str, StoredObject]] = {
            "S1": {},
            "S2": {},
            "S3": {},
        }
        self._forced_store_failures: set[str] = set()
        self._forced_read_failures: set[str] = set()
        self._forced_delete_failures: set[str] = set()

    def store_on_node(self, node_id: str, req: WriteRequest) -> bool:
        with self._lock:
            if node_id not in self._storage:
                return False

            if node_id in self._forced_store_failures:
                return False

            self._storage[node_id][req.file_id] = StoredObject(
                file_id=req.file_id,
                filename=req.filename,
                content=req.content,
                client_ts=req.client_ts,
                version=None,
            )
            return True

    def read_from_node(self, node_id: str, file_id: str) -> Optional[str]:
        with self._lock:
            if node_id not in self._storage:
                return None

            if node_id in self._forced_read_failures:
                return None

            obj = self._storage[node_id].get(file_id)
            return obj.content if obj else None

    def delete_from_node(self, node_id: str, file_id: str) -> bool:
        with self._lock:
            if node_id not in self._storage:
                return False

            if node_id in self._forced_delete_failures:
                return False

            self._storage[node_id].pop(file_id, None)
            return True

    def list_node_files(self, node_id: str) -> list[str]:
        with self._lock:
            if node_id not in self._storage:
                return []
            return sorted(self._storage[node_id].keys())

    # -------- test helpers --------

    def clear_all(self) -> None:
        with self._lock:
            for node_store in self._storage.values():
                node_store.clear()

    def fail_store_on(self, node_id: str) -> None:
        with self._lock:
            self._forced_store_failures.add(node_id)

    def fail_read_on(self, node_id: str) -> None:
        with self._lock:
            self._forced_read_failures.add(node_id)

    def fail_delete_on(self, node_id: str) -> None:
        with self._lock:
            self._forced_delete_failures.add(node_id)

    def heal_store_on(self, node_id: str) -> None:
        with self._lock:
            self._forced_store_failures.discard(node_id)

    def heal_read_on(self, node_id: str) -> None:
        with self._lock:
            self._forced_read_failures.discard(node_id)

    def heal_delete_on(self, node_id: str) -> None:
        with self._lock:
            self._forced_delete_failures.discard(node_id)

    def get_raw_storage(self) -> dict[str, dict[str, StoredObject]]:
        with self._lock:
            return {
                node_id: {
                    file_id: obj.model_copy(deep=True)
                    for file_id, obj in node_store.items()
                }
                for node_id, node_store in self._storage.items()
            }


storage_data_gateway = StubStorageDataGateway()