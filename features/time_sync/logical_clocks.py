import threading

"""
Physical time is provided by OS-level NTP synchronization and is used only for logging/debugging. Logical correctness is ensured using Lamport clocks, which operate independently of physical time.
"""

class LamportClock:
    """Lamport Logical Clock implementation to maintain causality of events."""
    def __init__(self):
        self.time = 0
        self._lock = threading.Lock()

    def get_time(self):
        with self._lock:
            return self.time

    def tick(self):
        """Increment the clock before a local event or sending a message."""
        with self._lock:
            self.time += 1
            return self.time

    def update(self, received_time):
        """Update clock upon receiving a message."""
        with self._lock:
            self.time = max(self.time, received_time) + 1
            return self.time


class VectorClock:
    """Vector Clock implementation to track causality and detect concurrent events."""
    def __init__(self, node_id, initial_nodes=None):
        self.node_id = node_id
        if initial_nodes is None:
            initial_nodes = [node_id]
        self.vector = {node: 0 for node in initial_nodes}
        self._lock = threading.Lock()

    def get_vector(self):
        with self._lock:
            return self.vector.copy()

    def add_node(self, node_id):
        with self._lock:
            if node_id not in self.vector:
                self.vector[node_id] = 0

    def tick(self):
        """Increment the local counter before a local event or sending a message."""
        with self._lock:
            if self.node_id not in self.vector:
                self.vector[self.node_id] = 0
            self.vector[self.node_id] += 1
            return self.vector.copy()

    def update(self, received_vector):
        """Update clock upon receiving a message with a remote vector."""
        with self._lock:
            if self.node_id not in self.vector:
                self.vector[self.node_id] = 0
            self.vector[self.node_id] += 1
            
            for node, time in received_vector.items():
                if node not in self.vector:
                    self.vector[node] = time
                else:
                    self.vector[node] = max(self.vector[node], time)
            return self.vector.copy()

    def compare(self, vector1, vector2):
        """
        Compare two vector clocks.
        Returns:
            1 if vector1 > vector2 (v1 happens after v2)
            -1 if vector1 < vector2 (v1 happens before v2)
            0 if vector1 == vector2
            None if they are concurrent
        """
        all_nodes = set(vector1.keys()).union(set(vector2.keys()))
        v1_greater = False
        v2_greater = False

        for node in all_nodes:
            val1 = vector1.get(node, 0)
            val2 = vector2.get(node, 0)

            if val1 > val2:
                v1_greater = True
            elif val2 > val1:
                v2_greater = True

        if v1_greater and not v2_greater:
            return 1
        elif v2_greater and not v1_greater:
            return -1
        elif not v1_greater and not v2_greater:
            return 0
        else:
            return None  # Concurrent
