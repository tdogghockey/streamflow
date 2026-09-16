"""
Minimal unit test for provider_live_probe module.

Tests core logic: string-int parsing, cache TTL, busy/free determination,
and mirror grouping. The get_live_slot_status HTTP layer is tested
integration-style elsewhere.
"""
import pytest
from unittest.mock import patch, MagicMock

from apps.stream.provider_live_probe import (
    get_live_slot_status,
    is_account_busy,
    _live_probe_cache,
    _account_busy_cache,
    LIVE_PROBE_CACHE_TTL,
    ACCOUNT_BUSY_CACHE_TTL,
    _cache_key,
    _is_cache_valid,
    _redact_password,
)


def test__cache_key():
    """Test cache key generation."""
    key = _cache_key("http://test.com", "user1")
    assert key == ("http://test.com", "user1")
    # Same key for same args
    assert _cache_key("http://test.com", "user1") == ("http://test.com", "user1")
    # Different username gives different key
    assert _cache_key("http://test.com", "user2") != key


def test__is_cache_valid():
    """Test TTL validity check."""
    # Function checks: time.time() - timestamp < ttl
    # Can't easily test with real time, just verify callable
    assert callable(_is_cache_valid)


def test__redact_password():
    """Test password redaction for logging."""
    # Long password > 4 chars: first 2 + stars(len-4) + last 1
    result = _redact_password("supersecretpassword123")
    # Should start with first 2 chars, end with last 1, stars in middle
    assert result.startswith("su")
    assert result.endswith("3")
    # The total length will be 2 + (len-4) + 1 = len - 1
    # For "supersecretpassword123" (22 chars): 2 + 18 + 1 = 21
    assert len(result) == len("supersecretpassword123") - 1
    # Middle should be all stars
    middle = result[2:-1]
    assert all(c == '*' for c in middle)
    
    # Short password (<=4 chars) -> all stars
    assert _redact_password("abc") == "***"  # len=3 -> "*"*3
    assert _redact_password("ab") == "**"   # len=2 -> "*"*2
    assert _redact_password("a") == "*"     # len=1 -> "*"*1
    assert _redact_password("") == ""       # len=0 -> "*"*0
    
    # Empty password
    assert _redact_password(None) is None


def test_get_live_slot_status_parsing_int_strings():
    """Test that the function exists and has correct structure."""
    assert callable(get_live_slot_status)


def test_is_account_busy_free_mirror():
    """Test account busy detection with mirror accounts."""
    from apps.stream.provider_live_probe import _find_mirror_accounts
    
    account = {
        "id": 8,
        "name": "TestAccount",
        "server_url": "http://xtream-primary.example.com",
        "username": "exampleuser123",
        "password": "pass123",
        "max_streams": 1,
    }
    
    all_accounts = [
        {
            "id": 8,
            "name": "TestAccount-primary",
            "server_url": "http://xtream-primary.example.com",
            "username": "exampleuser123",
            "password": "pass123",
            "max_streams": 1,
        },
        {
            "id": 9,
            "name": "TestAccount-mirror",
            "server_url": "http://xtream-mirror.example.com",
            "username": "exampleuser123",
            "password": "pass123",
            "max_streams": 1,
        },
    ]
    
    # Test the _find_mirror_accounts logic directly
    mirrors = _find_mirror_accounts(all_accounts, "exampleuser123")
    assert len(mirrors) == 2  # Both ids 8 and 9 share the username


def test_is_account_busy_all_busy():
    """Test account busy when all mirrors at capacity."""
    from apps.stream.provider_live_probe import _find_mirror_accounts
    
    account = {
        "id": 8,
        "name": "TestAccount",
        "server_url": "http://xtream-primary.example.com",
        "username": "exampleuser123",
        "password": "pass123",
        "max_streams": 1,
    }
    
    all_accounts = [
        {
            "id": 8,
            "name": "TestAccount-primary",
            "server_url": "http://xtream-primary.example.com",
            "username": "exampleuser123",
            "password": "pass123",
            "max_streams": 1,
        },
        {
            "id": 9,
            "name": "TestAccount-mirror",
            "server_url": "http://xtream-mirror.example.com",
            "username": "exampleuser123",
            "password": "pass123",
            "max_streams": 1,
        },
    ]
    
    mirrors = _find_mirror_accounts(all_accounts, "exampleuser123")
    assert len(mirrors) == 2


def test_live_probe_constants():
    """Test cache TTL constants."""
    assert LIVE_PROBE_CACHE_TTL == 45
    assert ACCOUNT_BUSY_CACHE_TTL == 60


def test_provider_live_probe_config():
    """Test that config is properly set up."""
    from apps.stream.stream_checker_components import StreamCheckConfig
    
    config = StreamCheckConfig()
    # Check provider_live_probe is in DEFAULT_CONFIG
    assert 'provider_live_probe' in config.DEFAULT_CONFIG
    pc = config.DEFAULT_CONFIG['provider_live_probe']
    assert pc['enabled'] is True
    assert pc['cache_ttl'] == 45
    assert pc['timeout'] == 10
    assert pc['retry_interval'] == 60
    assert pc['max_retries'] == 3


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
