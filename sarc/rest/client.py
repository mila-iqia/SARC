"""
Python client for SARC REST API.

Contains a SarcApiClient class for direct calls to REST API,
and high-level client functions using REST API as backend,
with almost same signatures as MongoDB counterparts:
- count_jobs
- get_job
- get_jobs
- get_rgus
- get_users
- load_job_series

To use high-level functions, consider setting `api` section
in SARC client config file, then calling your script
in client mode with your config file.

Example:
    sarc-client.yaml
        api:
            url: http://api.sarc.com:1234
    myscript.py
        >>> from sarc.rest.client import count_jobs
        >>> print(count_jobs())
    run:
        SARC_MODE=client SARC_CONFIG=sarc-client.yaml uv run myscript.py
"""

from datetime import datetime, time
from typing import Any, Callable, Iterable

import httpx
from pydantic import UUID4
from pydantic_mongo import PydanticObjectId

from sarc.client.job import SlurmJob, SlurmState
from sarc.client.series import AbstractJobSeriesFactory
from sarc.config import UTC, ConfigurationError, config
from sarc.core.models.api import SlurmJobList, UserList
from sarc.core.models.users import MemberType, UserData
from sarc.traces import trace_decorator


class SarcApiClient:
    """
    Python client for SARC REST API.

     If initialized without parameters, will look for
     section `api` from SARC config file to get API URL.
    """

    def __init__(
        self,
        remote_url: str | None = None,
        oauth2_token: str | None = None,
        timeout: int | None = None,
        session: httpx.Client | None = None,
        per_page: int | None = None,
    ) -> None:
        """
        Initialize.

        :param remote_url: API URL
        :param oauth2_token: Authentification token obtained from google.  Will do an interactive prompt if not specified
        :param timeout: requests timeout, in seconds. Default: 120.
        :param session: internal httpx client to use. Default: httpx module.
        :param per_page: default page size for paginated endpoints. Default: 100.
        """

        api_cfg = config().api

        if remote_url is None:
            if api_cfg.url is None:
                raise ConfigurationError(
                    "Remote URL not configured for REST API. "
                    "Either pass URL to SarcApiClient object, "
                    "or set `api` section in SARC config file"
                )
            remote_url = api_cfg.url

        if timeout is None:
            timeout = api_cfg.timeout

        if per_page is None:
            per_page = api_cfg.per_page

        assert oauth2_token is not None

        # Ensure no trailing slash for consistency
        self.remote_url = remote_url.rstrip("/")
        self.timeout = timeout
        self.session = session or httpx
        self.per_page = per_page
        self.oauth2_token = oauth2_token

    def _get(self, endpoint: str, params: dict | None = None) -> httpx.Response:
        """Helper to perform a GET request."""
        # Clean up params: remove None values and convert non-primitive types if needed
        cleaned_params: dict[str, Any] = {}
        if params:
            for k, v in params.items():
                if v is not None:
                    if isinstance(v, datetime):
                        cleaned_params[k] = v.isoformat()
                    elif isinstance(v, list) and not v:
                        # Force empty parameter.
                        # This is to handle particular case of
                        # empty list passed to `job_id`.
                        cleaned_params[k] = ""
                    else:
                        cleaned_params[k] = v

        url = f"{self.remote_url}{endpoint}"
        response = self.session.get(
            url,
            params=cleaned_params,
            timeout=self.timeout,
            headers={"Authorization": f"Bearer: {self.oauth2_token}"},
        )
        response.raise_for_status()
        return response

    # --- Job Endpoints ---

    def job_query(
        self,
        cluster: str | None = None,
        job_id: int | list[int] | None = None,
        job_state: SlurmState | None = None,
        username: str | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> list[str]:
        """
        Query jobs.
        Return a list of job internal object IDs,
        which can be passed to job/id/{id}
        """
        params = {
            "cluster": cluster,
            "job_id": job_id,
            "job_state": job_state.value if job_state else None,
            "username": username,
            "start": start,
            "end": end,
        }
        response = self._get("/v0/job/query", params=params)
        # The API returns list[PydanticObjectId], which serializes to list[str] in JSON
        return response.json()

    def job_list(
        self,
        cluster: str | None = None,
        job_id: int | list[int] | None = None,
        job_state: SlurmState | None = None,
        username: str | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        page: int = 1,
        per_page: int | None = None,
    ) -> SlurmJobList:
        """
        Query jobs with pagination.
        Return a SlurmJobList result, containing
        a paginated list of SlurmJob objects.
        """
        params = {
            "cluster": cluster,
            "job_id": job_id,
            "job_state": job_state.value if job_state else None,
            "username": username,
            "start": start,
            "end": end,
            "page": page,
            "per_page": per_page if per_page is not None else self.per_page,
        }
        response = self._get("/v0/job/list", params=params)
        return SlurmJobList.model_validate(response.json())

    def job_count(
        self,
        cluster: str | None = None,
        job_id: int | list[int] | None = None,
        job_state: SlurmState | None = None,
        username: str | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> int:
        """Count jobs."""
        params = {
            "cluster": cluster,
            "job_id": job_id,
            "job_state": job_state.value if job_state else None,
            "username": username,
            "start": start,
            "end": end,
        }
        response = self._get("/v0/job/count", params=params)
        return response.json()

    def job_by_id(self, oid: str | PydanticObjectId) -> SlurmJob:
        """
        Get a specific job by its internal Object ID.
        """
        response = self._get(f"/v0/job/id/{oid}")
        return SlurmJob.model_validate(response.json())

    # --- Cluster Endpoints ---

    def cluster_list(self) -> list[str]:
        """
        Return the names of available clusters.
        """
        response = self._get("/v0/cluster/list")
        return response.json()

    # --- GPU Endpoints ---

    def gpu_rgu(self) -> dict[str, float]:
        """
        Return the mapping GPU->RGU.
        """
        response = self._get("/v0/gpu/rgu")
        return response.json()

    # --- User Endpoints ---

    def user_query(
        self,
        display_name: str | None = None,
        email: str | None = None,
        member_type: MemberType | None = None,
        member_start: datetime | None = None,
        member_end: datetime | None = None,
        supervisor: UUID4 | None = None,
        supervisor_start: datetime | None = None,
        supervisor_end: datetime | None = None,
        co_supervisor: UUID4 | None = None,
        co_supervisor_start: datetime | None = None,
        co_supervisor_end: datetime | None = None,
    ) -> list[UUID4]:
        """
        Search users. Return list of user UUIDs.
        """
        params = {
            "display_name": display_name,
            "email": email,
            "member_type": member_type.value if member_type else None,
            "member_start": member_start,
            "member_end": member_end,
            "supervisor": str(supervisor) if supervisor else None,
            "supervisor_start": supervisor_start,
            "supervisor_end": supervisor_end,
            "co_supervisor": str(co_supervisor) if co_supervisor else None,
            "co_supervisor_start": co_supervisor_start,
            "co_supervisor_end": co_supervisor_end,
        }
        response = self._get("/v0/user/query", params=params)
        # The API returns list[UUID4], JSON decodes as list[str], we convert back to UUID objects
        return [UUID4(uuid_str) for uuid_str in response.json()]

    def user_list(
        self,
        display_name: str | None = None,
        email: str | None = None,
        member_type: MemberType | None = None,
        member_start: datetime | None = None,
        member_end: datetime | None = None,
        supervisor: UUID4 | None = None,
        supervisor_start: datetime | None = None,
        supervisor_end: datetime | None = None,
        co_supervisor: UUID4 | None = None,
        co_supervisor_start: datetime | None = None,
        co_supervisor_end: datetime | None = None,
        page: int = 1,
        per_page: int | None = None,
    ) -> UserList:
        """
        List users with details and pagination.
        """
        params = {
            "display_name": display_name,
            "email": email,
            "member_type": member_type.value if member_type else None,
            "member_start": member_start,
            "member_end": member_end,
            "supervisor": str(supervisor) if supervisor else None,
            "supervisor_start": supervisor_start,
            "supervisor_end": supervisor_end,
            "co_supervisor": str(co_supervisor) if co_supervisor else None,
            "co_supervisor_start": co_supervisor_start,
            "co_supervisor_end": co_supervisor_end,
            "page": page,
            "per_page": per_page if per_page is not None else self.per_page,
        }
        response = self._get("/v0/user/list", params=params)
        return UserList.model_validate(response.json())

    def user_by_id(self, uuid: UUID4 | str) -> UserData:
        """
        Get user with given UUID.
        """
        response = self._get(f"/v0/user/id/{uuid}")
        return UserData.model_validate(response.json())

    def user_by_email(self, email: str) -> UserData:
        """
        Get user with given email.
        """
        response = self._get(f"/v0/user/email/{email}")
        return UserData.model_validate(response.json())


def _parse_common_args(
    job_id: int | list[int] | None = None,
    job_state: str | SlurmState | None = None,
    start: str | datetime | None = None,
    end: str | datetime | None = None,
) -> tuple[list[int] | None, SlurmState | None, datetime | None, datetime | None]:
    """
    Helper to parse arguments common to job functions.
    Used in high-level functions below.
    """
    if isinstance(job_id, int):
        job_id = [job_id]

    if isinstance(job_state, str):
        job_state = SlurmState(job_state)

    if isinstance(start, str):
        start = datetime.combine(
            datetime.strptime(start, "%Y-%m-%d"), time.min
        ).replace(tzinfo=UTC)

    if isinstance(end, str):
        end = datetime.combine(datetime.strptime(end, "%Y-%m-%d"), time.min).replace(
            tzinfo=UTC
        )

    return job_id, job_state, start, end


def count_jobs(
    *,
    cluster: str | None = None,
    job_id: int | list[int] | None = None,
    job_state: str | SlurmState | None = None,
    user: str | None = None,
    start: str | datetime | None = None,
    end: str | datetime | None = None,
) -> int:
    """
    Count jobs matching the criteria using the REST API.

    Same signature as in `sarc.client.job.count_jobs`,
    except parameter `query_options` which is specific to MongoDB.
    """
    job_id, job_state, start, end = _parse_common_args(job_id, job_state, start, end)

    try:
        return SarcApiClient().job_count(
            cluster=cluster,
            job_id=job_id,  # type: ignore
            job_state=job_state,
            username=user,
            start=start,
            end=end,
        )
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code != 404:
            raise exc

    return 0


def get_job(**kwargs) -> SlurmJob | None:
    """
    Get a single job that matches the query, or None if nothing is found.

    Same signature as `sarc.client.job.get_job` (except query_options).
    """
    # Extract arguments expected by list_jobs
    cluster = kwargs.get("cluster")
    user = kwargs.get("user")

    job_id, job_state, start, end = _parse_common_args(
        kwargs.get("job_id"),
        kwargs.get("job_state"),
        kwargs.get("start"),
        kwargs.get("end"),
    )

    try:
        # We fetch page 1 with page size 1.
        # NB: the API sorts by submit_time desc by default, which
        # returns the most recent version.
        job_list_resp = SarcApiClient().job_list(
            cluster=cluster,
            job_id=job_id,  # type: ignore
            job_state=job_state,
            username=user,
            start=start,
            end=end,
            page=1,
            per_page=1,
        )

        if job_list_resp.jobs:
            return job_list_resp.jobs[0]

    except httpx.HTTPStatusError as exc:
        if exc.response.status_code != 404:
            raise exc

    return None


def get_jobs(
    *,
    cluster: str | None = None,
    job_id: int | list[int] | None = None,
    job_state: str | SlurmState | None = None,
    user: str | None = None,
    start: str | datetime | None = None,
    end: str | datetime | None = None,
) -> Iterable[SlurmJob]:
    """
    Get jobs matching the criteria using the REST API.
    Fetches all results by iterating over pages.

    Same signature as in `sarc.client.job.get_jobs`,
    except parameter `query_options` which is specific to MongoDB,
    """
    # Use a single httpx client for all calls.
    with httpx.Client() as session:
        client = SarcApiClient(session=session)

        per_page = client.per_page

        job_id, job_state, start, end = _parse_common_args(
            job_id, job_state, start, end
        )

        nb_all_jobs = 0
        page = 1

        try:
            while True:
                job_list_resp = client.job_list(
                    cluster=cluster,
                    job_id=job_id,  # type: ignore
                    job_state=job_state,  # type: ignore
                    username=user,
                    start=start,
                    end=end,
                    page=page,
                    per_page=per_page,
                )

                for job in job_list_resp.jobs:
                    yield job

                nb_all_jobs += len(job_list_resp.jobs)

                # Check if we have fetched everything
                if nb_all_jobs >= job_list_resp.total:
                    break

                # Or if the current page returned fewer than per_page (last page)
                if len(job_list_resp.jobs) < job_list_resp.per_page:
                    break

                page += 1

        except httpx.HTTPStatusError as exc:
            if exc.response.status_code != 404:
                raise exc


def get_rgus(rgu_version: str = "1.0") -> dict[str, float]:
    """
    Return GPU->RGU mapping.

    Note: rgu_version argument is ignored as the API does not currently support it.
    """
    if rgu_version != "1.0":
        raise NotImplementedError("rgu_version != 1.0 is not yet supported by REST API")
    return SarcApiClient().gpu_rgu()


def get_users(
    display_name: str | None = None,
    email: str | None = None,
    member_type: MemberType | None = None,
    member_start: datetime | None = None,
    member_end: datetime | None = None,
    supervisor: UUID4 | None = None,
    supervisor_start: datetime | None = None,
    supervisor_end: datetime | None = None,
    co_supervisor: UUID4 | None = None,
    co_supervisor_start: datetime | None = None,
    co_supervisor_end: datetime | None = None,
) -> list[UserData]:
    """
    Get users matching the criteria using the REST API.
    Fetches all results by iterating over pages.

    **NB**: Not same signature as sarc.users.db.get_users,
    which waits for a MongoDB `query` dict.
    """
    with httpx.Client() as session:
        client = SarcApiClient(session=session)

        per_page = client.per_page

        all_users = []
        page = 1

        while True:
            user_list_resp = client.user_list(
                display_name=display_name,
                email=email,
                member_type=member_type,
                member_start=member_start,
                member_end=member_end,
                supervisor=supervisor,
                supervisor_start=supervisor_start,
                supervisor_end=supervisor_end,
                co_supervisor=co_supervisor,
                co_supervisor_start=co_supervisor_start,
                co_supervisor_end=co_supervisor_end,
                page=page,
                per_page=per_page,
            )

            all_users.extend(user_list_resp.users)

            if len(all_users) >= user_list_resp.total:
                break

            if len(user_list_resp.users) < user_list_resp.per_page:
                break

            page += 1

        return all_users


class RestJobSeriesFactory(AbstractJobSeriesFactory):
    """
    Implementation of JobSeriesFactory for REST API.
    Allow to implement load_job_series for REST API below.
    """

    def _get_desc(self) -> str:
        return super()._get_desc() + " (REST)"

    def count_jobs(self, *args, **kwargs) -> int:
        return count_jobs(*args, **kwargs)

    def get_jobs(self, *args, **kwargs) -> Iterable[SlurmJob]:
        return get_jobs(*args, **kwargs)

    def get_users(self) -> list[UserData]:
        return get_users()


@trace_decorator()
def load_job_series(
    *,
    fields: None | list[str] | dict[str, str] = None,
    clip_time: bool = False,
    callback: None | Callable = None,
    **jobs_args,
) -> Any:  # Returns DataFrame, Any to avoid import
    """
    Query jobs using the REST API and return them in a DataFrame.

    See sarc.client.series.JobSeriesFactory.load_job_series for details.
    """
    factory = RestJobSeriesFactory()
    try:
        return factory.load_job_series(
            fields=fields, clip_time=clip_time, callback=callback, **jobs_args
        )
    except httpx.HTTPStatusError as exc:
        resp = exc.response.json()
        raise RuntimeError(resp) from exc
