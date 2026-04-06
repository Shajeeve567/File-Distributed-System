import os

# Default to "0" (REAL implementation) instead of "1" (STUB)
if os.getenv("USE_STUB_METADATA", "0") == "1":
    from shared.stubs.metadata_stub import metadata_repo
else:
    from features.replication.metadata_impl import metadata_repo

if os.getenv("USE_STUB_STORAGE", "0") == "1":
    from shared.stubs.storage_data_stub import storage_data_gateway
else:
    from features.replication.storage_data_impl import storage_data_gateway

if os.getenv("USE_STUB_HEALTH", "0") == "1":
    from shared.stubs.node_health_stub import node_health_gateway
else:
    from features.fault_tolerance.node_health_impl import node_health_gateway

if os.getenv("USE_STUB_CONSENSUS", "0") == "1":
    from shared.stubs.consensus_stub import consensus_engine
else:
    from features.consensus.consensus_impl import consensus_engine

if os.getenv("USE_STUB_CLOCK", "0") == "1":
    from shared.stubs.clock_stub import clock_monitor
else:
    from features.time_sync.clock_impl import clock_monitor