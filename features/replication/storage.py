import os
import asyncio
import json
from shared.utils import calculate_checksum, format_size, logger

class StorageManager:
    def __init__(self):
        from shared.config import config
        # Use absolute path resolving for complete reliability across environments
        self.blocks_dir = os.path.abspath(config.BLOCKS_DIR)
        os.makedirs(self.blocks_dir, exist_ok=True)
        logger.info(f"💎 Storage IRONCLAD initialized at {self.blocks_dir}")
    
    # Step 1: Basic write/read
    async def write_block(self, block_id, data):
        """Save a block to disk with hardware sync and verification"""
        path = os.path.join(self.blocks_dir, f"{block_id}.dat")
        def _write():
            with open(path, 'wb') as f:
                f.write(data)
                f.flush()
                os.fsync(f.fileno())  # Hardware sync
            # Post-write verify
            if not os.path.exists(path) or os.path.getsize(path) != len(data):
                raise IOError(f"Write verification failed for {path}")

        await asyncio.to_thread(_write)
        logger.info(f"💾 IRONCLAD: Wrote block {block_id} ({len(data)} bytes) to {path}")
        return True
    
    async def read_block(self, block_id):
        """Read a block from disk"""
        path = os.path.join(self.blocks_dir, f"{block_id}.dat")
        if not os.path.exists(path):
            return None
        def _read():
            with open(path, 'rb') as f:
                return f.read()
        return await asyncio.to_thread(_read)
    
    # Step 2: Add metadata
    async def save_metadata(self, block_id, metadata):
        """Store block info separately"""
        path = os.path.join(self.blocks_dir, f"{block_id}.meta")
        def _write_meta():
            with open(path, 'w') as f:
                f.write(json.dumps(metadata))
        await asyncio.to_thread(_write_meta)
        logger.info(f"📄 Saved manifest for {block_id}")
    
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
        await self.write_block(block_id, data)
        await self.save_metadata(block_id, {
            "checksum": checksum,
            "size": len(data),
            "created": time.time()
        })
        return True
    
    async def list_blocks(self):
        """List all blocks stored locally"""
        blocks = []
        for filename in os.listdir(self.blocks_dir):
            if filename.endswith('.dat') and not filename.startswith('manifest_'):
                blocks.append(filename[:-4])  # Remove .dat extension
        return blocks

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
  