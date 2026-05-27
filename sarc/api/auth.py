"""Shared HTTP Basic Auth gate used by all `sarc.api` routers.

Activated only when both `DASH_BASIC_AUTH_USER` and `DASH_BASIC_AUTH_PASSWORD`
env vars are set. Otherwise this is a no-op so local dev / tests / podman
still work without credentials.
"""

import os
import secrets

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials

_BASIC_AUTH_USER = os.environ.get("DASH_BASIC_AUTH_USER")
_BASIC_AUTH_PASSWORD = os.environ.get("DASH_BASIC_AUTH_PASSWORD")
_basic_security = HTTPBasic(auto_error=False)


def require_basic_auth(
    credentials: HTTPBasicCredentials | None = Depends(_basic_security),
) -> None:
    """Pop the browser's login dialog when DASH_BASIC_AUTH_* env vars are set.

    `secrets.compare_digest` is constant-time so timing attacks can't be used
    to leak the expected user/password byte by byte.
    """
    if not _BASIC_AUTH_USER or not _BASIC_AUTH_PASSWORD:
        return
    bad = (
        credentials is None
        or not secrets.compare_digest(credentials.username, _BASIC_AUTH_USER)
        or not secrets.compare_digest(credentials.password, _BASIC_AUTH_PASSWORD)
    )
    if bad:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required",
            headers={"WWW-Authenticate": "Basic"},
        )
