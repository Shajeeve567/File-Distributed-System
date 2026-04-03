from shared.models import Heartbeat
import time
from typing import Dict 

_LIVE_NODES: Dict[str, bool]  = {
    "S1": True, 
    "S2": True, 
    "S3": True 
}

_NODE_TIMES: Dict[str, float] = {
    "S1": 1000.000, 
    "S2": 1000.004, 
    "S3": 9999.998, 
}

class StubNodeHealthGateway: 

    def  heartbeat(self, node_id: str) -> Heartbeat:
        # return heartbeat status

        is_alive = _LIVE_NODES.get(node_id, False)
        timestamp = _NODE_TIMES.get(node_id, time.time() * 1000)

        return Heartbeat(
            node_id=node_id, 
            is_alive=is_alive,
            timestamp=timestamp
        )
    
    def list_live_nodes(self) -> list[str]: 
        return [node_id for node_id, alive in _LIVE_NODES.items() if alive]
    

# method for simulating clock scenarios
def set_node_failue(node_id: str, is_alive: bool = False): 
    # to make a node dead
    _LIVE_NODES[node_id] = is_alive

def set_node_timestamp(node_id: str, timestamp: float): 
    # to skew the clock
    _NODE_TIMES[node_id] = timestamp

node_health_gateway = StubNodeHealthGateway()
