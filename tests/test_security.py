"""Comprehensive unit tests for the security module.

Tests cover:
- API key validation (active, previous, missing, wrong)
- Cursor encryption round-trip
- Cursor tamper detection
- Cursor expiration
- Cursor key rotation
- Edge cases
"""
import base64
import time

import pytest

from security import (
    ExpiredCursor,
    InvalidCursor,
    api_key_is_valid,
    decode_cursor,
    encode_cursor,
    new_cursor_state,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=False)
def setup_keys(monkeypatch):
    """Configure test encryption and API keys."""
    key = base64.urlsafe_b64encode(b"a" * 32).decode("ascii").rstrip("=")
    monkeypatch.setenv("CURSOR_ENCRYPTION_KEY_ACTIVE", key)
    monkeypatch.delenv("CURSOR_ENCRYPTION_KEY_PREVIOUS", raising=False)
    monkeypatch.setenv("API_KEY_ACTIVE", "test-active-key")
    monkeypatch.setenv("API_KEY_PREVIOUS", "test-old-key")
    monkeypatch.setenv("CURSOR_TTL_SECONDS", "86400")
    return key


# ---------------------------------------------------------------------------
# API key validation tests
# ---------------------------------------------------------------------------

class TestApiKeyValidation:
    def test_accepts_active_key(self, monkeypatch, setup_keys):
        assert api_key_is_valid("test-active-key") is True

    def test_accepts_previous_key(self, monkeypatch, setup_keys):
        assert api_key_is_valid("test-old-key") is True

    def test_rejects_wrong_key(self, monkeypatch, setup_keys):
        assert api_key_is_valid("wrong-key") is False

    def test_rejects_none(self, monkeypatch, setup_keys):
        assert api_key_is_valid(None) is False

    def test_rejects_empty_string(self, monkeypatch, setup_keys):
        assert api_key_is_valid("") is False

    def test_raises_when_active_not_configured(self, monkeypatch):
        monkeypatch.delenv("API_KEY_ACTIVE", raising=False)
        with pytest.raises(RuntimeError, match="API_KEY_ACTIVE"):
            api_key_is_valid("any-key")

    def test_works_without_previous_key(self, monkeypatch, setup_keys):
        monkeypatch.setenv("API_KEY_PREVIOUS", "")
        assert api_key_is_valid("test-active-key") is True
        assert api_key_is_valid("test-old-key") is False

    def test_key_comparison_is_exact(self, monkeypatch, setup_keys):
        # Must not match partial keys
        assert api_key_is_valid("test-active-ke") is False
        assert api_key_is_valid("test-active-keyX") is False


# ---------------------------------------------------------------------------
# Cursor encryption tests
# ---------------------------------------------------------------------------

class TestCursorEncryption:
    def test_round_trip(self, monkeypatch, setup_keys):
        state = {
            "table_fqn": "cat.sch.tbl",
            "version": 42,
            "expires_at": int(time.time()) + 60,
        }
        token = encode_cursor(state)
        decoded = decode_cursor(token)
        assert decoded == state

    def test_cursor_contents_are_encrypted(self, monkeypatch, setup_keys):
        state = {
            "table_fqn": "my_catalog.my_schema.my_table",
            "version": 99,
            "expires_at": int(time.time()) + 60,
        }
        token = encode_cursor(state)
        # The table name must not appear in the token
        assert "my_catalog" not in token
        assert "my_schema" not in token
        assert "my_table" not in token

    def test_token_starts_with_version_prefix(self, monkeypatch, setup_keys):
        token = encode_cursor({"expires_at": int(time.time()) + 60})
        assert token.startswith("v1.")

    def test_rejects_tampering(self, monkeypatch, setup_keys):
        token = encode_cursor({"expires_at": int(time.time()) + 60})
        # Flip a character in the ciphertext portion
        chars = list(token)
        idx = len(token) // 2
        chars[idx] = "A" if chars[idx] != "A" else "B"
        tampered = "".join(chars)
        with pytest.raises(InvalidCursor):
            decode_cursor(tampered)

    def test_rejects_expired_cursor(self, monkeypatch, setup_keys):
        token = encode_cursor({"expires_at": int(time.time()) - 1})
        with pytest.raises(ExpiredCursor):
            decode_cursor(token)

    def test_rejects_empty_token(self, monkeypatch, setup_keys):
        with pytest.raises(InvalidCursor):
            decode_cursor("")

    def test_rejects_none_token(self, monkeypatch, setup_keys):
        with pytest.raises(InvalidCursor):
            decode_cursor(None)

    def test_rejects_wrong_prefix(self, monkeypatch, setup_keys):
        with pytest.raises(InvalidCursor):
            decode_cursor("v2.some_data")

    def test_rejects_oversized_token(self, monkeypatch, setup_keys):
        with pytest.raises(InvalidCursor):
            decode_cursor("v1." + "A" * 20000)

    def test_rejects_too_short_ciphertext(self, monkeypatch, setup_keys):
        with pytest.raises(InvalidCursor):
            decode_cursor("v1.AAAA")

    def test_raises_when_encryption_key_not_configured(self, monkeypatch):
        monkeypatch.delenv("CURSOR_ENCRYPTION_KEY_ACTIVE", raising=False)
        with pytest.raises(RuntimeError, match="CURSOR_ENCRYPTION_KEY_ACTIVE"):
            encode_cursor({"expires_at": int(time.time()) + 60})


class TestCursorKeyRotation:
    def test_cursor_encrypted_with_old_key_decrypts_with_previous(self, monkeypatch):
        """Simulate key rotation: encrypt with old key, decrypt with new active + old previous."""
        old_key = base64.urlsafe_b64encode(b"b" * 32).decode("ascii").rstrip("=")
        new_key = base64.urlsafe_b64encode(b"c" * 32).decode("ascii").rstrip("=")

        # Encrypt with old key (as if it was the active key before rotation)
        monkeypatch.setenv("CURSOR_ENCRYPTION_KEY_ACTIVE", old_key)
        monkeypatch.delenv("CURSOR_ENCRYPTION_KEY_PREVIOUS", raising=False)
        state = {"table_fqn": "c.s.t", "version": 1, "expires_at": int(time.time()) + 60}
        token = encode_cursor(state)

        # Now rotate: new key is active, old key is previous
        monkeypatch.setenv("CURSOR_ENCRYPTION_KEY_ACTIVE", new_key)
        monkeypatch.setenv("CURSOR_ENCRYPTION_KEY_PREVIOUS", old_key)
        decoded = decode_cursor(token)
        assert decoded == state

    def test_cursor_encrypted_with_revoked_key_fails(self, monkeypatch):
        """After key rotation completes and old key is removed, old cursors fail."""
        old_key = base64.urlsafe_b64encode(b"d" * 32).decode("ascii").rstrip("=")
        new_key = base64.urlsafe_b64encode(b"e" * 32).decode("ascii").rstrip("=")

        monkeypatch.setenv("CURSOR_ENCRYPTION_KEY_ACTIVE", old_key)
        monkeypatch.delenv("CURSOR_ENCRYPTION_KEY_PREVIOUS", raising=False)
        token = encode_cursor({"expires_at": int(time.time()) + 60})

        # Fully rotated: old key is gone
        monkeypatch.setenv("CURSOR_ENCRYPTION_KEY_ACTIVE", new_key)
        monkeypatch.delenv("CURSOR_ENCRYPTION_KEY_PREVIOUS", raising=False)
        with pytest.raises(InvalidCursor):
            decode_cursor(token)


# ---------------------------------------------------------------------------
# Cursor state builder tests
# ---------------------------------------------------------------------------

class TestNewCursorState:
    def test_builds_valid_state(self, monkeypatch, setup_keys):
        state = new_cursor_state(
            table_fqn="cat.sch.tbl",
            version=5,
            data_as_of="2025-01-01T00:00:00Z",
            key_columns=["id"],
            key_types=["BIGINT"],
            last_keys=["100"],
            page=1,
        )
        assert state["table_fqn"] == "cat.sch.tbl"
        assert state["version"] == 5
        assert state["page"] == 1
        assert "expires_at" in state
        assert state["expires_at"] > int(time.time())

    def test_rejects_too_small_ttl(self, monkeypatch, setup_keys):
        monkeypatch.setenv("CURSOR_TTL_SECONDS", "10")
        with pytest.raises(RuntimeError, match="CURSOR_TTL_SECONDS"):
            new_cursor_state(
                table_fqn="c.s.t", version=1, data_as_of="x",
                key_columns=["id"], key_types=["INT"], last_keys=["1"], page=1,
            )

    def test_rejects_too_large_ttl(self, monkeypatch, setup_keys):
        monkeypatch.setenv("CURSOR_TTL_SECONDS", "9999999")
        with pytest.raises(RuntimeError, match="CURSOR_TTL_SECONDS"):
            new_cursor_state(
                table_fqn="c.s.t", version=1, data_as_of="x",
                key_columns=["id"], key_types=["INT"], last_keys=["1"], page=1,
            )
