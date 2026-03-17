# storage.py - Build this piece by piece
import os
import aiofiles
import json
from utils import calculate_checksum, format_size, logger

class StorageManager:
    def __init__(self):
        from config import config
        self.blocks_dir = config.BLOCKS_DIR
        logger.info(f"Storage initialized at {self.blocks_dir}")
    
    # Step 1: Basic write/read
    async def write_block(self, block_id, data):
        """Save a block to disk"""
        path = os.path.join(self.blocks_dir, f"{block_id}.dat")
        async with aiofiles.open(path, 'wb') as f:
            await f.write(data)
        logger.info(f"Wrote block {block_id}")
        return True
    
    async def read_block(self, block_id):
        """Read a block from disk"""
        path = os.path.join(self.blocks_dir, f"{block_id}.dat")
        if not os.path.exists(path):
            return None
        async with aiofiles.open(path, 'rb') as f:
            return await f.read()
    
    # Step 2: Add metadata
    async def save_metadata(self, block_id, metadata):
        """Store block info separately"""
        path = os.path.join(self.blocks_dir, f"{block_id}.meta")
        async with aiofiles.open(path, 'w') as f:
            await f.write(json.dumps(metadata))
    
    async def get_metadata(self, block_id):
        """Retrieve block info"""
        path = os.path.join(self.blocks_dir, f"{block_id}.meta")
        if not os.path.exists(path):
            return None
        async with aiofiles.open(path, 'r') as f:
            return json.loads(await f.read())
    
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