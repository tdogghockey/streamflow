#!/usr/bin/env python3
"""
Tests for the self-occupation discount in the provider live-busy path.

Background: a provider-side "full" verdict (active_cons >= max_connections)
on a single-connection account may be caused by our own in-flight ffmpeg
probes (concurrent channel checks overlap). Such verdicts must not trigger
instant-skip/defer — the normal slot-acquire path waits out our own drain.
Only genuine occupancy (busy verdict with zero of our probes in flight)
keeps the instant-skip behavior.
"""

import sys
import os
import threading
import unittest

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from apps.stream.concurrent_stream_limiter import (
    AccountStreamLimiter,
    own_in_flight_probes,
)


def _bare_limiter(checking):
    """Real limiter object without running __init__ (pattern used in repo tests)."""
    lim = object.__new__(AccountStreamLimiter)
    lim.lock = threading.Lock()
    lim.account_checking_counts = dict(checking)
    return lim


class TestGetCheckingCount(unittest.TestCase):
    def test_returns_count_for_known_account(self):
        lim = _bare_limiter({5: 2})
        self.assertEqual(lim.get_checking_count(5), 2)

    def test_zero_for_unknown_account(self):
        lim = _bare_limiter({5: 2})
        self.assertEqual(lim.get_checking_count(6), 0)

    def test_zero_for_invalid_account(self):
        lim = _bare_limiter({5: 2})
        self.assertEqual(lim.get_checking_count(None), 0)

    def test_fail_soft_without_attrs(self):
        lim = object.__new__(AccountStreamLimiter)
        self.assertEqual(lim.get_checking_count(5), 0)


class TestOwnInFlightProbes(unittest.TestCase):
    def test_returns_limiter_count(self):
        class Stub:
            def get_checking_count(self, account_id):
                return 3 if account_id == 7 else 0

        self.assertEqual(own_in_flight_probes(Stub(), 7), 3)
        self.assertEqual(own_in_flight_probes(Stub(), 8), 0)

    def test_none_limiter_is_zero(self):
        self.assertEqual(own_in_flight_probes(None, 7), 0)

    def test_missing_getter_is_zero(self):
        self.assertEqual(own_in_flight_probes(object(), 7), 0)

    def test_throwing_getter_is_zero(self):
        class Stub:
            def get_checking_count(self, account_id):
                raise RuntimeError("boom")

        self.assertEqual(own_in_flight_probes(Stub(), 7), 0)


if __name__ == '__main__':
    unittest.main()
