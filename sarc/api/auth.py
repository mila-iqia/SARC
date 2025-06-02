import logging
from typing import Annotated

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer
from google.auth.transport import requests
from google.oauth2 import id_token

from sarc.client.users.api import User
from sarc.client.users.api import get_user as _get_user

logger = logging.getLogger(__name__)

auth_scheme = HTTPBearer()


# The client needs to request the "profile" and "email" scopes at least for
# authentication to succeed.
def get_email(token: Annotated[str, Depends(auth_scheme)]) -> str:
    try:
        # Note that we do not validate the client id of the token because there
        # will be multiple applications that will call this service and we don't
        # want to have to constantly update the SARC config to account for that.
        idinfo = id_token.verify_oauth2_token(
            id_token=token, request=requests.Request()
        )

        # The google OpenID Connect authentication flow only works for mila for
        # now.  We may revisit this if we add support for other institutes.
        if idinfo.get("hd", None) != "mila.quebec":
            raise ValueError("Token not from authorized domain")
        if "email" not in idinfo:
            raise ValueError("Token doesn't have the proper scopes")
        if not idinfo.get("email_verified", False):
            raise ValueError("Email is not verified")
        return idinfo["email"]
    except ValueError as e:
        logger.error(f"Authentication error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token",
            headers={"WWW-Authenticate": "Bearer"},
        )


async def get_user(mila_email: Annotated[str, Depends(get_email)]) -> User:
    user = _get_user(mila_cluster_username=mila_email)
    if user is not None:
        if user.mila.active:
            return user
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Unknown or inactive user",
        headers={"WWW-Authenticate": "Bearer"},
    )


async def get_permissions(user: Annotated[User, Depends(get_user)]):
    # TODO
    pass
