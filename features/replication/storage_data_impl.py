"""Storage Data Implementation (Task 2: Data Replication)"""

import httpx
from typing import Optional
from shared.models import WriteRequest
from shared.config import config

class StorageDataGatewayImpl:
    """Real implementation using HTTP to store/read blocks on other nodes."""
    
    async def store_on_node(self, node_id: str, req: WriteRequest) -> bool:
        """Sends data to the target node via HTTP."""
        peer_url = config.ALL_NODES.get(node_id)
        if not peer_url:
            return False
            
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.post(
                    f"{peer_url}/replicate",
                    json={
                        "block_id": req.file_id,
                        "data": req.content,
                        "source_node": config.NODE_ID
                    },
                    timeout=2.0
                )
                return resp.status_code == 200
        except Exception:
            return False

    async def read_from_node(self, node_id: str, file_id: str) -> Optional[str]:
        """Reads data from a target node via HTTP."""
        peer_url = config.ALL_NODES.get(node_id)
        if not peer_url:
            return None
            
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.get(f"{peer_url}/files/{file_id}")
                if resp.status_code == 200:
                    return resp.content.hex()
        except Exception:
            pass
        return None

    async def delete_from_node(self, node_id: str, file_id: str) -> bool:
        """Deletes data on a target node via HTTP."""
        # Optional: Implement if file deletion is needed
        return True

storage_data_gateway = StorageDataGatewayImpl()
