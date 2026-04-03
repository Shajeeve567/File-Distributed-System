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