import os

class Config:
    def __init__(self):
        # Anchor all paths to the Absolute Root of the project
        # This resolves to D:\Computer Science\year 2\2nd semester\DS\assignment\FaultTolerance
        self.BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.HOST = "127.0.0.1"
        self.REPLICATION_FACTOR = 3
        self.ALL_NODES = {
            "node1": "http://127.0.0.1:8001",
            "node2": "http://127.0.0.1:8002",
            "node3": "http://127.0.0.1:8003"
        }

    @property
    def NODE_ID(self):
        return os.environ.get("NODE_ID", "node1")

    @property
    def PORT(self):
        return int(os.environ.get("PORT", 8001))

    @property
    def DATA_DIR(self):
        return os.path.join(self.BASE_DIR, "data", self.NODE_ID)

    @property
    def BLOCKS_DIR(self):
        return os.path.join(self.DATA_DIR, "blocks")

    @property
    def OTHER_NODES(self):
        return {k: v for k, v in self.ALL_NODES.items() if k != self.NODE_ID}

# Single instance for the application
config = Config()