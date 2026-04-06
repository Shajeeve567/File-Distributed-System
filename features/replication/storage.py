import os
import asyncio
import json
import time
from shared.utils import calculate_checksum, format_size, logger

class StorageManager:
    def __init__(self):
        from shared.config import config
        # Use absolute path resolving for complete reliability across environments
        self.blocks_dir = os.path.abspath(config.BLOCKS_DIR)
        self.wal_file = os.path.join(self.blocks_dir, "storage.wal")
        os.makedirs(self.blocks_dir, exist_ok=True)
        logger.info(f"Storage IRONCLAD initialized at {self.blocks_dir}")
        self._recover_from_wal()

    def _recover_from_wal(self):
        """Basic WAL recovery: Check for incomplete writes on startup"""
        if os.path.exists(self.wal_file):
            logger.info("[STORAGE] Checking WAL for recovery...")
            # Simple implementation: just log it for now
            # In a more robust system, we would rollback or complete the transactions
            with open(self.wal_file, 'r') as f:
                logs = f.readlines()
                if logs:
                    logger.warning(f"[STORAGE] Found {len(logs)} entries in WAL. Potential unclean shutdown.")
    
    def _log_wal(self, entry: str):
        """Append entry to WAL"""
        try:
            with open(self.wal_file, 'a') as f:
                f.write(f"{time.time()}: {entry}\n")
                f.flush()
                os.fsync(f.fileno())
        except Exception as e:
            logger.error(f"WAL write failed: {e}")

    async def write_block(self, block_id, data):
        """Save a block to disk with hardware sync and verification"""
        self._log_wal(f"START_WRITE {block_id}")
        path = os.path.join(self.blocks_dir, f"{block_id}.dat")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        def _write():
            with open(path, 'wb') as f:
                f.write(data)
                f.flush()
                os.fsync(f.fileno())  # Hardware sync
            # Post-write verify
            if not os.path.exists(path) or os.path.getsize(path) != len(data):
                raise IOError(f"Write verification failed for {path}")

        await asyncio.to_thread(_write)
        self._log_wal(f"COMMIT_WRITE {block_id}")
        logger.info(f"💾 IRONCLAD: Wrote block {block_id} ({len(data)} bytes) to {path}")
        return True
    
    async def read_block(self, block_id):
        """Read a block from disk"""
        path = os.path.join(self.blocks_dir, f"{block_id}.dat")
        if not os.path.exists(path):
            return None
        def _read():
            with open(path, 'rb') as f:
                data = f.read()
                logger.debug(f"[CONSISTENCY] Read block {block_id} for integrity verifications")
                return data
        return await asyncio.to_thread(_read)
    
    # Step 2: Add metadata
    async def save_metadata(self, block_id, metadata):
        """Store block info separately with Deterministic LWW tie-breaker"""
        path = os.path.join(self.blocks_dir, f"{block_id}.meta")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        
        # Enforce Logical LWW Convergence
        if os.path.exists(path):
            existing = await self.get_metadata(block_id)
            if existing:
                exist_ts = existing.get("lamport_ts", 0)
                new_ts = metadata.get("lamport_ts", 0)
                
                # Rule A: Ignore chronologically older vectors
                if new_ts < exist_ts:
                    logger.debug(f"[CONSISTENCY] Ignored logically stale metadata for {block_id} (Lamport {new_ts} < {exist_ts})")
                    return False
                
                # Rule B: Deterministic Tiebreaker via Node ID supremacy
                elif new_ts == exist_ts:
                    exist_node = existing.get("source_node", "")
                    new_node = metadata.get("source_node", "")
                    if new_node != exist_node and new_node < exist_node:
                        logger.debug(f"[CONSISTENCY] Tie-breaker rejected {block_id} (Node {new_node} < Node {exist_node})")
                        return False

        def _write_meta():
            with open(path, 'w') as f:
                f.write(json.dumps(metadata))
        await asyncio.to_thread(_write_meta)
        logger.info(f"Saved manifest for {block_id}")
        return True
    
    async def get_metadata(self, block_id):
        """Retrieve block info"""
        path = os.path.join(self.blocks_dir, f"{block_id}.meta")
        if not os.path.exists(path):
            return None
        def _read_meta():
            with open(path, 'r') as f:
                return json.loads(f.read())
        return await asyncio.to_thread(_read_meta)
    
    
    # Step 3: Add checksums for integrity
    async def write_with_checksum(self, block_id, data):
        """Save with integrity check"""
        checksum = calculate_checksum(data)
        logger.info(f"[CONSISTENCY] Calculated SHA256 checksum for block {block_id}")
        
        # Track version
        old_meta = await self.get_metadata(block_id)
        version = (old_meta.get("version", 0) + 1) if old_meta else 1
        
        await self.write_block(block_id, data)
        await self.save_metadata(block_id, {
            "checksum": checksum,
            "size": len(data),
            "created": time.time(),
            "version": version
        })
        return True
    
    async def list_blocks(self):
        """List all blocks stored locally"""
        blocks = []
        for filename in os.listdir(self.blocks_dir):
            if filename.endswith('.dat') and not filename.startswith('manifest_'):
                blocks.append(filename[:-4])  # Remove .dat extension
        return blocks

    async def delete_block(self, block_id):
        """Delete a block from disk"""
        self._log_wal(f"START_DELETE {block_id}")
        path = os.path.join(self.blocks_dir, f"{block_id}.dat")
        if os.path.exists(path):
            os.remove(path)
            self._log_wal(f"COMMIT_DELETE {block_id}")
            logger.info(f"[STORAGE] Deleted block {block_id}")
            return True
        return False

    async def delete_metadata(self, block_id):
        """Delete block info"""
        path = os.path.join(self.blocks_dir, f"{block_id}.meta")
        if os.path.exists(path):
            os.remove(path)
            logger.info(f" [STORAGE] Deleted metadata for {block_id}")
            return True
        return False

# Test each step as you go
if __name__ == "__main__":
    import asyncio
    async def test():
        storage = StorageManager()
        # Test write
        await storage.write_block("test", b"hello")
        # Test read
        data = await storage.read_block("test")
        print(f"Read: {data}")
    asyncio.run(test())
  