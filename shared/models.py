from typing import Literal, Optional
from pydantic import BaseModel


class FileMetadata(BaseModel):
    file_id: str
    filename: str
    version: int
    primary_node: str
    replica_nodes: list[str]
    status: Literal["READY", "DEGRADED", "RECOVERING"]


class WriteRequest(BaseModel):
    file_id: str
    filename: str
    content: str
    client_ts: float


class WriteResult(BaseModel):
    success: bool
    version: int
    committed_nodes: list[str]
    leader_id: str


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


class ReplicationResult(BaseModel):
    attempted_nodes: list[str]
    succeeded_nodes: list[str]
    failed_nodes: list[str]
    ack_count: int