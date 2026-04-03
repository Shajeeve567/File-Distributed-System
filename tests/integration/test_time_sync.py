"""Test your time sync module."""

import pytest
from shared.models import ClockStatus
from features.time_sync.clock_sync import compute_offsets, assess_cluster_health
from features.time_sync.fallback import handle_time_sync_failure, FallbackStrategy
from features.time_sync.monitor import TimeSyncMonitor


class TestTimeSync:
    
    def test_compute_offsets_correct_milliseconds(self):
        """VERIFY THE BUG IS FIXED: Should return milliseconds, not seconds."""
        node_times = {
            "S1": 100.123,
            "S2": 100.127,  # 4ms ahead
            "S3": 100.118,  # 5ms behind
        }
        
        offsets = compute_offsets(node_times, "S1")
        
        # FIXED: Should be 4 and -5, NOT 0.004 and -0.005
        assert offsets["S1"] == 0
        assert offsets["S2"] == 4   # (0.127 - 0.123) * 1000 = 4
        assert offsets["S3"] == -5  # (0.118 - 0.123) * 1000 = -5
    
    def test_assess_health_with_drift(self):
        """Test health assessment with actual drift values."""
        # S2 is 60ms ahead (out of sync if threshold is 50)
        offsets = {"S1": 0, "S2": 60, "S3": 10}
        
        statuses, healthy = assess_cluster_health(offsets, max_offset_ms=50)
        
        assert healthy is False
        
        s2 = next(s for s in statuses if s.node_id == "S2")
        assert s2.in_sync is False
        assert s2.offset_ms == 60
    
    def test_fallback_levels(self):
        """Test different fallback levels."""
        # 60ms drift -> WARN_ONLY
        offsets = {"S1": 0, "S2": 60}
        assert handle_time_sync_failure(offsets, 50) == FallbackStrategy.WARN_ONLY
        
        # 120ms drift -> USE_COORDINATOR_TIME
        offsets = {"S1": 0, "S2": 120}
        assert handle_time_sync_failure(offsets, 50) == FallbackStrategy.USE_COORDINATOR_TIME
        
        # 200ms drift -> PAUSE_WRITES
        offsets = {"S1": 0, "S2": 200}
        assert handle_time_sync_failure(offsets, 50) == FallbackStrategy.PAUSE_WRITES
    
    def test_monitor_imports_correctly(self):
        """Test that all imports work."""
        monitor = TimeSyncMonitor(max_offset_ms=50)
        assert monitor.max_offset_ms == 50
        assert monitor.current_fallback == FallbackStrategy.WARN_ONLY


# Run with: pytest tests/integration/test_time_sync.py -v