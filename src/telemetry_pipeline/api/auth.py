"""API-key authentication for the report endpoints (ADR 0007).

The key is read from ``TELEMETRY_API_KEY`` and presented in the ``X-API-Key``
header. Comparison is constant-time (``hmac.compare_digest``) so response
timing cannot reveal the key. Failures never echo the presented key.
"""

from __future__ import annotations

import hmac

from fastapi import HTTPException, Request, status

API_KEY_HEADER = "X-API-Key"


def require_api_key(request: Request) -> None:
    settings = request.app.state.settings
    expected = settings.api_key
    if expected is None:
        # Only reachable when api_allow_anonymous=true; create_app enforces that.
        return
    expected_bytes = expected.get_secret_value().encode()
    presented = request.headers.get(API_KEY_HEADER, "").encode()
    # ``compare_digest(b"", b"")`` is True, so a blank configured key would admit every
    # request that omits the header. Settings already map a blank key to None; this
    # keeps the dependency fail-closed on its own (ADR 0011). Real keys compare verbatim.
    if not expected_bytes.strip() or not hmac.compare_digest(presented, expected_bytes):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing or invalid API key",
            headers={"WWW-Authenticate": API_KEY_HEADER},
        )
