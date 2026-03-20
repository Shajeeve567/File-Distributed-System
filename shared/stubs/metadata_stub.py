from shared.models import FileMetadata

class StubMetadataRepo:
    def get_file_metadata(self, file_id: str) -> FileMetadata | None:
        pass
    def upsert_file_metadata(self, meta: FileMetadata) -> FileMetadata:
        pass
    def choose_replica_nodes(self, file_id: str, replication_factor: int = 3) -> list[str]:
        pass

