import pytest

from sarc.client.users.api import get_user, get_users


@pytest.mark.usefixtures("read_only_db_with_users", "client_mode", "tzlocal_is_mtl")
def test_get_users(file_regression):
    users = get_users()
    file_regression.check(
        f"Found {len(users)} users(s):\n"
        + "\n".join(
            [user.model_dump_json(exclude={"id": True}, indent=4) for user in users]
        )
    )


@pytest.mark.usefixtures("read_only_db_with_users", "client_mode", "tzlocal_is_mtl")
def test_get_user(file_regression):
    user = get_user(mila_email_username="bonhomme@mila.quebec")
    assert user is not None
    file_regression.check(
        "Found user:\n" + user.model_dump_json(exclude={"id": True}, indent=4)
    )
