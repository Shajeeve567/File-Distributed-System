from shared.models import LogEntry
from typing import Optional 

_CURRENT_LEADER: Optional[str] = "S1"
_CURRENT_TERM: int = 1 

class StubConsensusEngine: 

    def get_current_leader(self) -> Optional[str]:
        return _CURRENT_LEADER
    
    def replicate_log(self, entry: LogEntry) -> bool:
        return True
    
    def start_election(self) -> str:
        return "S1"
    
def set_current_leader(leader: Optional[str]):
    global _CURRENT_LEADER
    _CURRENT_LEADER = leader 

def set_current_term(term: int): 
    global _CURRENT_TERM 
    _CURRENT_TERM = term 

consensus_engine = StubConsensusEngine() 
