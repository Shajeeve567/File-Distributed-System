from shared.dependencies import metadata_repo, node_health_gateway, storage_data_gateway
from shared.models import FileMetadata, ReplicationResult, WriteRequest, WriteResult
from features.replication.conflicts import next_version_for_write
from features.replication.consistency import is_write_successful, write_quorum_required
from shared.config import config


def choose_valid_replica_nodes(file_id: str, requested_factor: int = None) -> list[str]:
    if requested_factor is None:
        requested_factor = config.REPLICATION_FACTOR
    candidate_nodes = metadata_repo.choose_replica_nodes(file_id, requested_factor)
    live_nodes = set(node_health_gateway.list_live_nodes())
    return [node_id for node_id in candidate_nodes if node_id in live_nodes]


def replicate_to_nodes(req: WriteRequest, node_ids: list[str], version: int) -> ReplicationResult:
    succeeded_nodes: list[str] = []
    failed_nodes: list[str] = []

    for node_id in node_ids:
        ok = storage_data_gateway.store_on_node(node_id, req)
        if ok:
            succeeded_nodes.append(node_id)
        else:
            failed_nodes.append(node_id)

    return ReplicationResult(
        attempted_nodes=node_ids,
        succeeded_nodes=succeeded_nodes,
        failed_nodes=failed_nodes,
        ack_count=len(succeeded_nodes),
    )


def handle_write(req: WriteRequest, leader_id: str = "COORDINATOR_1") -> WriteResult:
    current_meta = metadata_repo.get_file_metadata(req.file_id)
    current_version = current_meta.version if current_meta else 0
    new_version = next_version_for_write(current_version)

    replica_nodes = choose_valid_replica_nodes(req.file_id, requested_factor=3)
    if not replica_nodes:
        return WriteResult(
            success=False,
            version=current_version,
            committed_nodes=[],
            leader_id=leader_id,
        )

    replication_result = replicate_to_nodes(req, replica_nodes, version=new_version)
    required_acks = write_quorum_required(replication_factor=config.REPLICATION_FACTOR)
    success = is_write_successful(replication_result.ack_count, required_acks)

    if not success:
        return WriteResult(
            success=False,
            version=current_version,
            committed_nodes=replication_result.succeeded_nodes,
            leader_id=leader_id,
        )

    updated_meta = FileMetadata(
        file_id=req.file_id,
        filename=req.filename,
        version=new_version,
        primary_node=replication_result.succeeded_nodes[0],
        replica_nodes=replication_result.succeeded_nodes,
        status="READY" if len(replication_result.failed_nodes) == 0 else "DEGRADED",
    )
    metadata_repo.upsert_file_metadata(updated_meta)

    return WriteResult(
        success=True,
        version=new_version,
        committed_nodes=replication_result.succeeded_nodes,
        leader_id=leader_id,
    )