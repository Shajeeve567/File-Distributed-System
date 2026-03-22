from typing import Optional
from shared.models import FileMetadata
from shared.interfaces import MetadataRepo

class InMemoryMetadataRepo(MetadataRepo):
    def __init__(self) -> None:
        self._files: dict[str, FileMetadata] = {}
        self._default_nodes = ["S1", "S2", "S3"]

    def get_file_metadata(self, file_id: str) -> Optional[FileMetadata]:
        return self._files.get(file_id)

    def upsert_file_metadata(self, meta: FileMetadata) -> FileMetadata:
        self._files[meta.file_id] = meta
        return meta

    def choose_replica_nodes(self, file_id: str, replication_factor: int = 3) -> list[str]:
        """
        For now, fixed placement policy.
        Later you can make this smarter.
        """
        return self._default_nodes[:replication_factor]


metadata_repo = InMemoryMetadataRepo()