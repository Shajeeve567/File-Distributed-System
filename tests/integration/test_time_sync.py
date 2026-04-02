"""Test your time sync module using stubs from other members."""

import pytest
from shared.models import ClockStatus
from feature.time_sync.clock_sync import compute_offsets, assess_cluster_health
from feature.time_sync.fallback import handle_time_sync_failure, FallbackStrategy
from feature.time_sync.monitor import TimeSyncMonitor


class TestTimeSync:
    """Test all time sync functionality."""
    
    def test_compute_offsets_basic(self):
        """Test that offset calculation works."""
        node_times = {"S1": 100.123, "S2": 100.127, "S3": 100.118}
        offsets = compute_offsets(node_times, "S1")
        
        assert offsets["S1"] == 0
        assert offsets["S2"] == 4   # 4ms ahead
        assert offsets["S3"] == -5  # 5ms behind
    
    def test_assess_health_creates_clockstatus(self):
        """Test that we get proper ClockStatus objects."""
        offsets = {"S1": 0, "S2": 60, "S3": -5}
        statuses, healthy = assess_cluster_health(offsets, max_offset_ms=50)
        
        assert len(statuses) == 3
        assert all(isinstance(s, ClockStatus) for s in statuses)
        
        # S2 should be out of sync
        s2 = next(s for s in statuses if s.node_id == "S2")
        assert s2.in_sync is False
        assert s2.offset_ms == 60
        
        assert healthy is False
    
    def test_monitor_uses_stubs(self):
        """Test that monitor automatically gets data from stubs."""
        monitor = TimeSyncMonitor(max_offset_ms=50)
        
        # This should work without any other code
        statuses, strategy = monitor.check_cluster_health()
        
        # Should get all 3 nodes from stub
        assert len(statuses) == 3
        
        # Stubs return all nodes alive, so all should be in sync
        assert all(s.in_sync for s in statuses)
    
    def test_monitor_detects_drift(self):
        """Test that drift is detected correctly."""
        monitor = TimeSyncMonitor(max_offset_ms=50)
        
        # Simulate S2 having 120ms drift
        node_times = {"S1": 100.000, "S2": 100.120, "S3": 100.010}
        
        statuses, strategy = monitor.check_cluster_health(node_times, "S1")
        
        # S2 should be out of sync
        s2 = next(s for s in statuses if s.node_id == "S2")
        assert s2.in_sync is False
        assert s2.offset_ms == 120
        
        # Strategy should be USE_COORDINATOR_TIME (moderate drift)
        assert strategy == FallbackStrategy.USE_COORDINATOR_TIME
    
    def test_fallback_levels(self):
        """Test different fallback levels."""
        # Minor drift (60ms) -> WARN_ONLY
        offsets = {"S1": 0, "S2": 60}
        assert handle_time_sync_failure(offsets, 50) == FallbackStrategy.WARN_ONLY
        
        # Moderate drift (120ms) -> USE_COORDINATOR_TIME
        offsets = {"S1": 0, "S2": 120}
        assert handle_time_sync_failure(offsets, 50) == FallbackStrategy.USE_COORDINATOR_TIME
        
        # Severe drift (200ms) -> PAUSE_WRITES
        offsets = {"S1": 0, "S2": 200}
        assert handle_time_sync_failure(offsets, 50) == FallbackStrategy.PAUSE_WRITES
    
    def test_clock_impl_returns_statuses(self):
        """Test the main interface."""
        from feature.time_sync.clock_impl import RealClockMonitor
        
        monitor = RealClockMonitor()
        statuses = monitor.cluster_time_status(["S1", "S2", "S3"])
        
        assert len(statuses) == 3
        assert all(isinstance(s, ClockStatus) for s in statuses)