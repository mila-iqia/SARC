from dataclasses import dataclass

import pytest

email_map = {
    "admin": "admin@admin.admin",
    "user": "smithj@mila.quebec",
    "not_in_db": "unknown-user@mila.quebec",
    "guest": None,
}


@dataclass
class Endpoint:
    endpoint: str
    statuses: dict[str, int]


@dataclass
class Q:
    id: str
    endpoint: str
    user: str
    status: int = 200


def queries(**endpoints):
    expanded = [
        Q(
            id=f"{eid}.{profile}",
            endpoint=endpoint.endpoint,
            user=email_map[profile],
            status=status,
        )
        for eid, endpoint in endpoints.items()
        for profile, status in endpoint.statuses.items()
    ]
    return pytest.mark.parametrize("query", expanded, ids=[q.id for q in expanded])


# Endpoints using `requestor` + `get_query()` (raises 501 for non-admin):
#   admin=200, user=501, not_in_db=403, guest=401
#
# Endpoints using only `dependencies=[Depends(requestor)]` (no admin check in handler):
#   admin=200/404, user=200/404, not_in_db=403, guest=401
#
# Endpoints using only `dependencies=[Depends(require_admin)]`
#   admin=200/404, user=403, not_in_db=403, guest=401
@pytest.mark.usefixtures("read_only_db_with_users")
@queries(
    cluster_list=Endpoint(
        "/v0/cluster/list", {"admin": 200, "user": 200, "not_in_db": 403, "guest": 401}
    ),
    gpu_rgu=Endpoint(
        "/v0/gpu/rgu", {"admin": 200, "user": 200, "not_in_db": 403, "guest": 401}
    ),
    job_query=Endpoint(
        "/v0/job/query", {"admin": 200, "user": 501, "not_in_db": 403, "guest": 401}
    ),
    job_list=Endpoint(
        "/v0/job/list", {"admin": 200, "user": 501, "not_in_db": 403, "guest": 401}
    ),
    job_count=Endpoint(
        "/v0/job/count", {"admin": 200, "user": 501, "not_in_db": 403, "guest": 401}
    ),
    job_id=Endpoint(
        # Use a valid-format but nonexistent ObjectId to distinguish auth failures
        # (401/403) from successful auth + data-not-found (404).
        "/v0/job/id/000000000000000000000001",
        {
            "admin": 404,
            "user": 404,  # smithj is in DB, passes requestor, but job not found
            "not_in_db": 403,
            "guest": 401,
        },
    ),
    user_query=Endpoint(
        "/v0/user/query", {"admin": 200, "user": 501, "not_in_db": 403, "guest": 401}
    ),
    user_list=Endpoint(
        "/v0/user/list", {"admin": 200, "user": 501, "not_in_db": 403, "guest": 401}
    ),
    user_id=Endpoint(
        # Valid UUID format, nonexistent user.
        "/v0/user/id/00000000-0000-4000-8000-000000000001",
        {
            "admin": 404,
            "user": 404,  # smithj is in DB, passes requestor, but user not found
            "not_in_db": 403,
            "guest": 401,
        },
    ),
    user_email=Endpoint(
        "/v0/user/email/jsmith@example.com",
        {"admin": 200, "user": 403, "not_in_db": 403, "guest": 401},
    ),
)
def test_auth(app, query):
    u = app.client(query.user)
    u.get(query.endpoint, expect_status=query.status)
