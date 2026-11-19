"""API-key validation and AES-256-GCM encrypted pagination cursors."""
from __future__ import annotations

import base64
import hmac
import json
import logging
import os
import secrets
import time
from typing import Any

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

LOGGER = logging.getLogger(__name__)

# Binds ciphertext to this service — prevents ciphertexts from being
# replayed against a different application sharing the same key.
AAD = b"databricks-delta-api:v1"

MAX_CURSOR_TOKEN_LEN = 16_384  # protects against oversized payload attacks


class InvalidCursor(ValueError):
    """Raised when a cursor token cannot be decoded or authenticated."""


class ExpiredCursor(InvalidCursor):
    """Raised when a cursor token has exceeded its TTL."""


def api_key_is_valid(presented: str | None) -> bool:
    """Return True if the presented key matches the active or previous API key.

    Uses constant-time comparison (hmac.compare_digest) to prevent timing
    attacks. Both API_KEY_ACTIVE and API_KEY_PREVIOUS are always evaluated
    so the response time does not reveal which keys are configured.

    Raises RuntimeError if API_KEY_ACTIVE is not set.
    """
    if not presented:
        return False

    active = os.getenv("API_KEY_ACTIVE", "").strip()
    previous = os.getenv("API_KEY_PREVIOUS", "").strip()

    if not active:
        raise RuntimeError("API_KEY_ACTIVE is not configured")

    supplied = presented.encode("utf-8")
    matched = hmac.compare_digest(supplied, active.encode("utf-8"))
    if previous:
        matched = hmac.compare_digest(supplied, previous.encode("utf-8")) or matched

    return matched


def _decode_key(setting_name: str) -> bytes | None:
    """Decode a URL-safe Base64 AES-256 key from an environment variable.

    Returns None when the variable is empty. Raises RuntimeError if the
    value is present but not a valid 32-byte Base64 string.
    """
    raw = os.getenv(setting_name, "").strip()
    if not raw:
        return None
    try:
        key = base64.urlsafe_b64decode(raw + "=" * (-len(raw) % 4))
    except Exception as exc:
        raise RuntimeError(f"{setting_name} must be valid URL-safe Base64") from exc
    if len(key) != 32:
        raise RuntimeError(f"{setting_name} must decode to exactly 32 bytes (AES-256)")
    return key


def _b64url(data: bytes) -> str:
    """Encode bytes to URL-safe Base64 without padding."""
    return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")


def _unb64url(value: str) -> bytes:
    """Decode URL-safe Base64 with auto-padding."""
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


def encode_cursor(state: dict[str, Any]) -> str:
    """Encrypt cursor state with AES-256-GCM and return a versioned token.

    A fresh 96-bit nonce is generated per token. The nonce is prepended
    to the ciphertext before Base64 encoding. The v1. prefix allows future
    format changes without breaking existing clients.
    """
    key = _decode_key("CURSOR_ENCRYPTION_KEY_ACTIVE")
    if key is None:
        raise RuntimeError("CURSOR_ENCRYPTION_KEY_ACTIVE is not configured")

    nonce = secrets.token_bytes(12)
    plaintext = json.dumps(state, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    ciphertext = AESGCM(key).encrypt(nonce, plaintext, AAD)
    return "v1." + _b64url(nonce + ciphertext)


def decode_cursor(token: str) -> dict[str, Any]:
    """Decrypt and validate a cursor token.

    Tries both CURSOR_ENCRYPTION_KEY_ACTIVE and CURSOR_ENCRYPTION_KEY_PREVIOUS
    so cursors issued before a key rotation remain usable until they expire.

    Raises:
        ExpiredCursor: cursor TTL has passed.
        InvalidCursor: token is malformed, tampered, or decryption failed.
    """
    if not token or len(token) > MAX_CURSOR_TOKEN_LEN or not token.startswith("v1."):
        raise InvalidCursor("Malformed cursor")

    try:
        encrypted = _unb64url(token[3:])
        if len(encrypted) < 28:  # 12-byte nonce + 16-byte GCM tag minimum
            raise InvalidCursor("Malformed cursor")
        nonce, ciphertext = encrypted[:12], encrypted[12:]
    except InvalidCursor:
        raise
    except Exception as exc:
        raise InvalidCursor("Malformed cursor") from exc

    keys = [
        _decode_key("CURSOR_ENCRYPTION_KEY_ACTIVE"),
        _decode_key("CURSOR_ENCRYPTION_KEY_PREVIOUS"),
    ]
    plaintext = None
    for key in keys:
        if key is None:
            continue
        try:
            plaintext = AESGCM(key).decrypt(nonce, ciphertext, AAD)
            break
        except InvalidTag:
            continue

    if plaintext is None:
        raise InvalidCursor("Cursor authentication failed")

    try:
        state = json.loads(plaintext.decode("utf-8"))
    except Exception as exc:
        raise InvalidCursor("Malformed cursor payload") from exc

    if not isinstance(state, dict):
        raise InvalidCursor("Malformed cursor payload")

    expires_at = state.get("expires_at", 0)
    try:
        expires_at_int = int(expires_at)
    except (TypeError, ValueError) as exc:
        raise InvalidCursor("Malformed cursor expiration") from exc
    if expires_at_int < int(time.time()):
        raise ExpiredCursor("Cursor expired")

    return state


def new_cursor_state(
    *,
    table_fqn: str,
    version: int,
    data_as_of: str,
    key_columns: list[str],
    key_types: list[str],
    last_keys: list[str],
    page: int,
) -> dict[str, Any]:
    """Build the cursor state dict that will be encrypted into a next_cursor token.

    TTL is read from CURSOR_TTL_SECONDS (default 86 400 s / 24 h).
    Valid range: 60 s – 604 800 s (1 minute – 7 days).
    """
    ttl = int(os.getenv("CURSOR_TTL_SECONDS", "86400"))
    if ttl < 60 or ttl > 604800:
        raise RuntimeError("CURSOR_TTL_SECONDS must be between 60 and 604800")
    return {
        "table_fqn": table_fqn,
        "version": int(version),
        "data_as_of": data_as_of,
        "key_columns": key_columns,
        "key_types": key_types,
        "last_keys": last_keys,
        "page": int(page),
        "expires_at": int(time.time()) + ttl,
    }
