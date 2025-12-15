import logging

from easy_oauth import OAuthManager

from sarc.config import config

logger = logging.getLogger(__name__)


def get_oauth() -> OAuthManager | None:
    conf = config("scraping")

    if conf.auth is not None:
        return OAuthManager(
            server_metadata_url=conf.auth.metadata_url,
            secret_key=conf.auth.secret_key,
            client_id=conf.auth.client_id,
            client_secret=conf.auth.client_secret,
            client_kwargs={"scope": "openid email", "prompt": "select_account"},
            prefix="/0",
        )
    else:
        return None
