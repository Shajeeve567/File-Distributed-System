import hashlib
import uuid
import time
import logging
from datetime import datetime

# Setup logging first so you can see what's happening
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def generate_block_id(filename, part_number):
  """Create unique ID for each file block"""
  unique = f"{filename}_{part_number}_{uuid.uuid4().hex[:8]}"
  return hashlib.md5(unique.encode()).hexdigest()

def calculate_checksum(data):
  """Verify data integrity"""
  return hashlib.md5(data).hexdigest()

def get_timestamp():
  """Current time for versioning"""
  return time.time()

def format_size(size_bytes):
  """Convert bytes to human readable"""
  for unit in ['B', 'KB', 'MB', 'GB']:
    if size_bytes < 1024:
      return f"{size_bytes:.1f} {unit}"
    size_bytes /= 1024
  return f"{size_bytes:.1f} TB"

#Testing the utility functions
if __name__ == "__main__":
  print(generate_block_id("test.txt", 0))
  print(calculate_checksum(b"hello"))
  print(format_size(1024))