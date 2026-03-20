from typing import Literal, Optional

from pydantic import BaseModel, Field


class FileMetadata(BaseModel):
    file_id: str
    filename: str
    version: int
    primary_node: str
    replica_nodes: list[str] = Field(default_factory=list)
    status: Literal["READY", "DEGRADED", "RECOVERING"] = "READY"


class WriteRequest(BaseModel):
    file_id: str
    filename: str
    content: str
    client_ts: float


class WriteResult(BaseModel):
    success: bool
    version: int
    committed_nodes: list[str] = Field(default_factory=list)
    leader_id: str


class ReplicationResult(BaseModel):
    attempted_nodes: list[str] = Field(default_factory=list)
    succeeded_nodes: list[str] = Field(default_factory=list)
    failed_nodes: list[str] = Field(default_factory=list)
    ack_count: int = 0


class Heartbeat(BaseModel):
    node_id: str
    is_alive: bool
    timestamp: float


class ClockStatus(BaseModel):
    node_id: str
    offset_ms: int
    in_sync: bool


class LogEntry(BaseModel):
    term: int
    index: int
    op: str
    file_id: str
    payload: dict


class StoredObject(BaseModel):
    """
    Internal object used by the storage stub.
    Useful for debugging and future extension.
    """
    file_id: str
    filename: str
    content: str
    client_ts: float
    version: Optional[int] = None