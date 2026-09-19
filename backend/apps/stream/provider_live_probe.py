#!/usr/bin/env python3
"""
Provider Live Probe - Checks live stream slot status via Xtream player_api.php

Provides real-time capacity checking for Xtream/StreamFlow accounts with
max_streams protection. Uses player_api.php (not HEAD/GET to stream URLs)
to avoid consuming slots.

Key design points:
- GET {server_url}/player_api.php with 10s timeout - does NOT consume a slot
- Short TTL cache 45s keyed by (server_url, username) to avoid rate-limit
- On error returns {unknown:true} and fail-open (allow existing limiter to decide)
- Helper probes all mirror URLs for a logical account (shared username grouping)
- Fail-open: if probe fails, existing AccountStreamLimiter decides
"""

import logging
import time
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# TTL cache: key -> (timestamp, result)
_live_probe_cache: Dict[Tuple[str, str], Tuple[float, Dict[str, Any]]] = {}

# Per-account busy cache: key -> (timestamp, is_busy)
_account_busy_cache: Dict[str, Tuple[float, bool]] = {}
# Typing placeholder - actual writes are dynamic due to cache key variance

# Cache TTL in seconds
LIVE_PROBE_CACHE_TTL = 45
ACCOUNT_BUSY_CACHE_TTL = 60


def _cache_key(server_url: str, username: str) -> Tuple[str, str]:
    """Create cache key from server_url and username."""
    return (server_url, username)


def _redact_password(password: Optional[str]) -> Optional[str]:
    """Redact password for logging - never log raw credentials."""
    if password is None:
        return None
    if len(password) <= 4:
        return "*" * len(password)
    return password[:2] + "*" * (len(password) - 4) + password[-1:]


def _is_cache_valid(timestamp: float, ttl: float) -> bool:
    """Check if cache entry is still valid based on TTL."""
    return time.time() - timestamp < ttl


def get_live_slot_status(server_url: str, username: str, password: str) -> Dict[str, Any]:
    """
    Get live slot status for an Xtream account via player_api.php.

    Does NOT consume a slot. Uses GET {server_url}/player_api.php with 10s timeout.

    Args:
        server_url: Base URL of the Xtream server (e.g. http://xtream.example.com)
        username: User's login username
        password: User's login password

    Returns:
        Dict with keys:
            - active_cons: int - current active connections
            - max_connections: int - maximum allowed connections
            - auth: str - auth token/status
            - status: str - "ok" or "unknown"
            - unknown: bool - True if probe failed (caller should fail-open)

        On error: returns {active_cons: 0, max_connections: 0, auth: "", status: "unknown", unknown: True}
                 - this is fail-open, allowing the existing limiter to decide
    """
    import requests

    cache_key = _cache_key(server_url, username)
    now = time.time()

    # Check cache
    if cache_key in _live_probe_cache:
        cache_ts, cache_result = _live_probe_cache[cache_key]
        if _is_cache_valid(cache_ts, LIVE_PROBE_CACHE_TTL):
            logger.debug(
                f"Live probe cache hit for ({server_url}, {username})"
            )
            return cache_result

    url = f"{server_url}/player_api.php"
    params = {
        "username": username,
        "password": password,
    }
    # Browser/player-style UA: some provider CDNs (Cloudflare) return 520 to
    # the default python-requests UA, which would force fail-closed defers.
    headers = {"User-Agent": "VLC/3.0.14"}

    # Sanitize for logging - never log raw password
    log_params = params.copy()
    if "password" in log_params:
        log_params["password"] = _redact_password(log_params["password"])

    logger.debug(
        f"Probing live slot at {url} with params {log_params}"
    )

    try:
        response = requests.get(url, params=params, timeout=10, headers=headers)
        response.raise_for_status()
        data = response.json()

        # Parse string ints safely - player_api returns strings
        active_cons = int(data.get("active_cons", "0") or 0)
        max_connections = int(data.get("max_connections", "0") or 0)
        auth = data.get("auth", "")
        status = data.get("status", "unknown")

        result = {
            "active_cons": active_cons,
            "max_connections": max_connections,
            "auth": auth,
            "status": status,
        }

        # Cache the result
        _live_probe_cache[cache_key] = (now, result)

        return result

    except Exception as e:
        logger.warning(
            f"Live probe failed for ({server_url}, {username}): "
            f"{type(e).__name__}: {e}"
        )

        # Return unknown result - fail-open (allow existing limiter to decide)
        # Brief cache on error to avoid rate-limiting cascade
        error_result = {
            "active_cons": 0,
            "max_connections": 0,
            "auth": "",
            "status": "unknown",
            "unknown": True,
        }
        _live_probe_cache[cache_key] = (now, error_result)

        return error_result


def _find_mirror_accounts(
    all_accounts: List[Dict[str, Any]], username: str
) -> List[Dict[str, Any]]:
    """
    Find all M3U accounts that share the same username (mirror accounts).

    Accounts with the same logical username but different server_urls/ids
    are mirrors of each other. For example, two accounts sharing one
    username but different server_urls/ids with max_streams=1.

    Args:
        all_accounts: List of all M3U account dicts from UDI
        username: The username to group by

    Returns:
        List of account dicts that share this username (including the original)
    """
    if not username:
        return []

    username_lower = str(username).lower().strip()
    mirrors = []

    for account in all_accounts:
        acc_username = str(account.get("username") or "").lower().strip()
        if acc_username == username_lower:
            mirrors.append(account)

    return mirrors


def is_account_busy(
    m3u_account: Dict[str, Any],
    all_accounts: Optional[List[Dict[str, Any]]] = None,
) -> bool:
    """
    Check if an M3U account is busy by probing all mirror URLs.

    For a logical account (one shared username across mirror URLs),
    probe all mirror URLs. If any mirror reports
    active_cons < max_connections -> free (not busy).
    If all report busy -> busy.

    Gets creds from UDI account dict if it contains password,
    else via Dispatcharr API using existing api_utils pattern;
    do NOT hardcode creds/URLs.

    Args:
        m3u_account: Dictionary with account info (id, name, server_url,
                     username, password, max_streams)
        all_accounts: Optional list of all M3U accounts (from UDI).
                     If not provided, will try to fetch via UDI.

    Returns:
        True if all mirrors are busy (no free slots).
        False if any mirror has a free slot.
    """
    server_url = m3u_account.get("server_url")
    username = m3u_account.get("username")
    account_password = m3u_account.get("password")
    max_streams = m3u_account.get("max_streams", 1)

    if not server_url or not username:
        logger.warning(
            "Cannot probe account: missing server_url or username, "
            "assuming busy (fail-open)"
        )
        return True

    # Determine credentials: use account password if available,
    # else fetch via Dispatcharr API
    if account_password:
        creds_password = account_password
    else:
        # Fetch credentials via UDI or APIUtils
        creds_password = None
        try:
            from apps.udi import get_udi_manager

            udi = get_udi_manager()
            if udi is not None:
                accounts = udi.get_m3u_accounts()
                for acc in accounts or []:
                    acc_username = str(acc.get("username") or "").lower().strip()
                    if acc_username == str(username).lower().strip():
                        creds_password = acc.get("password")
                        break
        except Exception as e:
            logger.warning(
                f"Failed to fetch credentials from UDI: {e}"
            )

        if creds_password is None:
            # Fallback: try APIUtils
            try:
                from apps.core.api_utils import get_m3u_accounts as api_get_accounts
                accounts = api_get_accounts()
                for acc in accounts or []:
                    acc_username = str(acc.get("username") or "").lower().strip()
                    if acc_username == str(username).lower().strip():
                        creds_password = acc.get("password")
                        break
            except Exception as e2:
                logger.warning(
                    f"Failed to fetch credentials via API: {e2}"
                )

        if creds_password is None:
            logger.warning(
                f"Cannot find password for username {username}, "
                "assuming busy (fail-open)"
            )
            return True

    # Find all mirror accounts sharing this username
    if all_accounts is None:
        try:
            from apps.udi import get_udi_manager
            udi = get_udi_manager()
            all_accounts = udi.get_m3u_accounts() or []
        except Exception:
            all_accounts = []

    mirror_accounts = _find_mirror_accounts(all_accounts, username)
    # Ensure the original account is included
    account_ids = {str(a.get("id")) for a in mirror_accounts}
    if str(m3u_account.get("id")) not in account_ids:
        mirror_accounts.append(m3u_account)

    # Probe each mirror URL
    free_found = False

    for mirror_account in mirror_accounts:
        mirror_url = mirror_account.get("server_url") or server_url
        mirror_username = mirror_account.get("username") or username

        # Probe the mirror's live slot status
        status = get_live_slot_status(mirror_url, mirror_username, creds_password)

        if status.get("unknown"):
            # Probe failed - assume this mirror busy, continue checking others
            # If all fail, result is busy
            logger.debug(
                f"Live probe failed for mirror {mirror_url}, "
                "assuming busy"
            )
            continue

        active_cons = status.get("active_cons", 0)
        max_conns = status.get("max_connections", max_streams)

        # Parse string ints safely (in case they came as strings from cache/JSON)
        try:
            active_cons = int(active_cons)
        except (TypeError, ValueError):
            active_cons = 0
        try:
            max_conns = int(max_conns)
        except (TypeError, ValueError):
            max_conns = max_streams

        logger.debug(
            f"Mirror {mirror_url}: "
            f"active_cons={active_cons}, max_connections={max_conns}"
        )

        if active_cons < max_conns:
            # This mirror has a free slot
            free_found = True
            logger.info(
                f"Mirror {mirror_url} has free slot "
                f"(active_cons={active_cons} < max_connections={max_conns})"
            )
            break
        else:
            logger.debug(
                f"Mirror {mirror_url} is busy "
                f"(active_cons={active_cons} >= max_connections={max_conns})"
            )

    result = not free_found  # True = busy, False = has free slot

    # Cache the busy status briefly
    cache_key = f"busy:{server_url}:{username}"
    _account_busy_cache[cache_key] = (time.time(), result)

    return result