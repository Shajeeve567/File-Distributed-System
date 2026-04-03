from shared.models import WriteRequest
from typing import Optional, Dict

#In-memory storage
_STUB_STORAGE: Dict[str, Dict[str, str]] = {
    "S1": {},
    "S2": {}, 
    "S3": {}
}

class StubStorageDataGateway: 

    def store_on_node(self, node_id: str, req: WriteRequest) -> bool:

        # store content on correct node 
        if node_id not in _STUB_STORAGE:
            
            return False
        _STUB_STORAGE[node_id][req.file_id] = req.content
        return True
    
    def read_from_node(self, node_id: str, file_id: str) -> Optional[str]:

        # read content from correct node
        if node_id not in _STUB_STORAGE:

            return None
        return _STUB_STORAGE[node_id].get(file_id)
    
    def delete_from_node(self, node_id: str, file_id: str) -> bool:

        # delete file from correct node 
        if node_id not in _STUB_STORAGE:
            return False
        
        if file_id in _STUB_STORAGE[node_id]:
            del _STUB_STORAGE[node_id][file_id]
            return True
        return False
    
storage_data_gateway = StubStorageDataGateway()
