import os
import socket

class Config:
  #Node identity
  NODE_ID = os.getenv("NODE_ID", "node1")
  PORT = int(os.getenv("PORT", "8001"))
  HOST = "127.0.0.1"
  
  #Cluster configuration
  REPLICATION_FACTOR = 3
  
  #All nodes in cluster
  ALL_NODES = {
    "node1": "http://127.0.0.1:8001",
    "node2": "http://127.0.0.1:8002", 
    "node3": "http://127.0.0.1:8003"
  }
  
  #Other nodes exclusing itself
  OTHER_NODES = {k: v for k, v in ALL_NODES.items() if k != NODE_ID}
  
  #Data directories
  DATA_DIR = f"data/{NODE_ID}"
  BLOCKS_DIR = os.path.join(DATA_DIR, "blocks")
  
  os.makedirs(BLOCKS_DIR, exist_ok=True)

config = Config()

#Test with:
if __name__ == "__main__":
  print(f"Starting {config.NODE_ID}")
  print(f"Other nodes: {config.OTHER_NODES}")