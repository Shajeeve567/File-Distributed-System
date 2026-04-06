from __future__ import annotations

from threading import Lock
from typing import Optional

from shared.models import FileMetadata


class StubMetadataRepo:
    """
    In-memory metadata repo.

    This is enough for your replication layer to:
    - check current metadata/version
    - choose replicas
    - upsert metadata after successful replication
    """

    def __init__(self) -> None:
        self._lock = Lock()
        self._default_nodes = ["S1", "S2", "S3"]
        self._files: dict[str, FileMetadata] = {
            "demo-1": FileMetadata(
                file_id="demo-1",
                filename="demo.txt",
                version=1,
                primary_node="S1",
                replica_nodes=["S1", "S2", "S3"],
                status="READY",
            )
        }

    def get_file_metadata(self, file_id: str) -> Optional[FileMetadata]:
        with self._lock:
            meta = self._files.get(file_id)
            return meta.model_copy(deep=True) if meta else None

    def upsert_file_metadata(self, meta: FileMetadata) -> FileMetadata:
        with self._lock:
            self._files[meta.file_id] = meta.model_copy(deep=True)
            return self._files[meta.file_id].model_copy(deep=True)

    def choose_replica_nodes(self, file_id: str, replication_factor: int = 3) -> list[str]:
        """
        Minimal placement policy:
        - If metadata already exists, prefer its existing replicas first.
        - Fill remaining slots using default nodes.
        """
        with self._lock:
            existing = self._files.get(file_id)
            if existing:
                ordered = list(existing.replica_nodes)
                for node_id in self._default_nodes:
                    if node_id not in ordered:
                        ordered.append(node_id)
                return ordered[:replication_factor]

            return self._default_nodes[:replication_factor]

    def list_all_metadata(self) -> list[FileMetadata]:
        with self._lock:
            return [meta.model_copy(deep=True) for meta in self._files.values()]

    # -------- test helpers --------

    def clear(self) -> None:
        with self._lock:
            self._files.clear()

    def seed(self, entries: list[FileMetadata]) -> None:
        with self._lock:
            self._files = {entry.file_id: entry.model_copy(deep=True) for entry in entries}


metadata_repo = StubMetadataRepo()
from shared.models import FileMetadata
from typing import Optional, Dict

# In-memory storage 
_STUB_FILES: Dict[str, FileMetadata] = {
    "demo-1": FileMetadata(
        file_id="demo-1",
        filename="demo.txt",
        version=1,
        primary_node="S1",
        replica_nodes=["S1", "S2", "S3"],
        status="READY",
    )
}

class StubMetaDataRepo: 
    
    def get_file_metadata(self, file_id: str) -> Optional[FileMetadata]: 
        # return metadata for existing file, otherwise None
        return _STUB_FILES.get(file_id)
    
    def upsert_file_metadata(self, meta: FileMetadata) -> Optional[FileMetadata]:
        # store/update file metadata 
        _STUB_FILES[meta.file_id] = meta
        return meta
    
    def choose_replica_nodes(self, file_id: str, replication_factor: int = 3) -> list[str]:
        # returning 3 nodes for simulation testing
        return ["S1", "S2", "S3"][:replication_factor]
    
metadata_repo = StubMetaDataRepo() 
