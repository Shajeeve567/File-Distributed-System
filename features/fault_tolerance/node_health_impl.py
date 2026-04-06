"""Node health implementation (Task 1: Fault Tolerance)"""

from shared.models import Heartbeat
import time
from shared.config import config

class NodeHealthGatewayImpl:
    """Real implementation of the node health gateway."""
    
    def __init__(self, registry):
        self.registry = registry

    async def heartbeat(self, node_id: str) -> Heartbeat:
        """Returns the heartbeat status from the registry."""
        live_nodes = await self.registry.get_live_nodes()
        is_alive = node_id in live_nodes or node_id == config.NODE_ID
        # In a real system, we might track the last seen timestamp
        return Heartbeat(
            node_id=node_id,
            is_alive=is_alive,
            timestamp=time.time()
        )

    async def list_live_nodes(self) -> list[str]:
        """Returns a list of currently active nodes."""
        return await self.registry.get_live_nodes()

    async def is_node_alive(self, node_id: str) -> bool:
        """Checks if a specific node is healthy."""
        live_nodes = await self.registry.get_live_nodes()
        return node_id in live_nodes or node_id == config.NODE_ID
