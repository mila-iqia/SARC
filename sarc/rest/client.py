from __future__ import annotations

import os
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Literal

import httpx
from serieux import deserialize
from serieux.features.encrypt import EncryptionKey, Secret
from serieux.formats import FileSource

from sarc.models.api import JobSeriesList, SlurmJobList, UserList
from sarc.models.cluster import SlurmCluster
from sarc.models.job import SlurmJob, SlurmState
from sarc.models.series import JobSeries
from sarc.models.support import GpuRgu
from sarc.models.user import MemberType, User


@dataclass(kw_only=True)
class SarcClient:
    # Base URL for SARC's API
    base_url: str = "https://sarc.mila.quebec/v0"

    # API token
    token: Secret[str] | None = None

    # Connection timeout in seconds
    timeout: int = 120

    # Optional httpx.Client instance to use for requests
    session: httpx.Client | None = None

    # How many results to get in one go
    block_size: int = 100

    # Extra fields to fetch on jobs
    job_extra_fields: list[Literal["cluster_name", "sarc_user", "statistics"]] = field(
        default_factory=list
    )

    @classmethod
    def load(cls, config_file: str | Path | None = None):
        """Load a client from a configuration file.

        * No argument: load from sarc.client in the main configuration file
        * load("file.yaml") => load from that file
        * load("pyproject.toml:sarc") => load from that file's sarc field
        """
        if config_file is None:
            from . import default_client

            return default_client._obj()  # ty:ignore[unresolved-attribute]
        else:
            if isinstance(config_file, str):
                config_file = deserialize(FileSource, config_file)
            return deserialize(
                cls,
                config_file,
                EncryptionKey(os.getenv("SERIEUX_PASSWORD")),  # ty:ignore[too-many-positional-arguments]
            )

    def _extra_fields(self, extra_fields: list[str] | None) -> list[str] | None:
        merged = list({*self.job_extra_fields, *(extra_fields or [])})
        return merged or None

    def _get(self, path: str, params: list[tuple[str, Any]] | None = None) -> Any:
        headers = {}
        if self.token is not None:
            headers["Authorization"] = f"Bearer {self.token}"
        url = self.base_url.rstrip("/") + "/" + path.lstrip("/")
        getter = self.session.get if self.session is not None else httpx.get
        response = getter(url, params=params, headers=headers, timeout=self.timeout)
        response.raise_for_status()
        return response.json()

    def _job_params(
        self,
        cluster_name: str | None = None,
        job_id: list[int] | None = None,
        job_state: SlurmState | str | None = None,
        email: str | None = None,
        sarc_user_id: int | None = None,
        cluster_user: str | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        extra_fields: list[str] | None = None,
    ) -> list[tuple[str, Any]]:
        params: list[tuple[str, Any]] = []
        if cluster_name is not None:
            params.append(("cluster_name", cluster_name))
        if job_id is not None:
            if len(job_id) == 0:
                params.append(("job_id", ""))
            else:
                params.extend(("job_id", jid) for jid in job_id)
        if job_state is not None:
            params.append(
                (
                    "job_state",
                    job_state if isinstance(job_state, str) else job_state.value,
                )
            )
        if email is not None:
            params.append(("email", email))
        if sarc_user_id is not None:
            params.append(("sarc_user_id", sarc_user_id))
        if cluster_user is not None:
            params.append(("cluster_user", cluster_user))
        if start is not None:
            params.append(("start", start.isoformat()))
        if end is not None:
            params.append(("end", end.isoformat()))
        if extra_fields:
            params.append(("extra_fields", ",".join(extra_fields)))
        return params

    def get_jobs(
        self,
        *,
        cluster_name: str | None = None,
        job_id: list[int] | None = None,
        job_state: SlurmState | str | None = None,
        email: str | None = None,
        sarc_user_id: int | None = None,
        cluster_user: str | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        extra_fields: list[str] | None = None,
    ) -> Iterator[SlurmJob]:
        base_params = self._job_params(
            cluster_name=cluster_name,
            job_id=job_id,
            job_state=job_state,
            email=email,
            sarc_user_id=sarc_user_id,
            cluster_user=cluster_user,
            start=start,
            end=end,
            extra_fields=self._extra_fields(extra_fields),
        )
        cursor = None
        while cursor is not False:
            params = base_params + [("limit", self.block_size)]
            if cursor is not None:
                params.append(("cursor", cursor))
            page = SlurmJobList.model_validate(self._get("/job/query", params))
            yield from page.results
            cursor = page.cursor

    def count_jobs(
        self,
        *,
        cluster_name: str | None = None,
        job_id: list[int] | None = None,
        job_state: SlurmState | str | None = None,
        email: str | None = None,
        sarc_user_id: int | None = None,
        cluster_user: str | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> int:
        return self._get(
            "/job/count",
            self._job_params(
                cluster_name=cluster_name,
                job_id=job_id,
                job_state=job_state,
                email=email,
                sarc_user_id=sarc_user_id,
                cluster_user=cluster_user,
                start=start,
                end=end,
            ),
        )

    def get_job(self, id: int, extra_fields: list[str] | None = None) -> SlurmJob:
        merged = self._extra_fields(extra_fields)
        params = [("extra_fields", ",".join(merged))] if merged else None
        return SlurmJob.model_validate(self._get(f"/job/id/{id}", params))

    def _user_params(
        self,
        display_name: str | None = None,
        email: str | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        member_type: MemberType | str | None = None,
        supervisor: int | None = None,
    ) -> list[tuple[str, Any]]:
        params: list[tuple[str, Any]] = []
        if display_name is not None:
            params.append(("display_name", display_name))
        if email is not None:
            params.append(("email", email))
        if start is not None:
            params.append(("start", start.isoformat()))
        if end is not None:
            params.append(("end", end.isoformat()))
        if member_type is not None:
            params.append(
                (
                    "member_type",
                    member_type if isinstance(member_type, str) else member_type.value,
                )
            )
        if supervisor is not None:
            params.append(("supervisor", supervisor))
        return params

    def get_users(
        self,
        *,
        display_name: str | None = None,
        email: str | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        member_type: MemberType | str | None = None,
        supervisor: int | None = None,
    ) -> Iterator[User]:
        base_params = self._user_params(
            display_name=display_name,
            email=email,
            start=start,
            end=end,
            member_type=member_type,
            supervisor=supervisor,
        )
        cursor = None
        while cursor is not False:
            params = base_params + [("limit", self.block_size)]
            if cursor is not None:
                params.append(("cursor", cursor))
            page = UserList.model_validate(self._get("/user/query", params))
            yield from page.results
            cursor = page.cursor

    def get_user_by_id(self, id: int) -> User:
        return User.model_validate(self._get(f"/user/id/{id}"))

    def get_user_by_email(self, email: str) -> User:
        return User.model_validate(self._get(f"/user/email/{email}"))

    def get_clusters(self) -> list[SlurmCluster]:
        return [SlurmCluster.model_validate(c) for c in self._get("/cluster/list")]

    def get_rgus(self) -> list[GpuRgu]:
        return [GpuRgu.model_validate(ret) for ret in self._get("/gpu/rgu")]

    def get_job_series(
        self,
        *,
        cluster_name: str | None = None,
        job_id: list[int] | None = None,
        job_state: SlurmState | str | None = None,
        email: str | None = None,
        sarc_user_id: int | None = None,
        cluster_user: str | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        extra_fields: list[str] | None = None,
    ) -> Iterator[JobSeries]:
        base_params = self._job_params(
            cluster_name=cluster_name,
            job_id=job_id,
            job_state=job_state,
            email=email,
            sarc_user_id=sarc_user_id,
            cluster_user=cluster_user,
            start=start,
            end=end,
            extra_fields=extra_fields,
        )
        cursor = None
        while cursor is not False:
            params = base_params + [("limit", self.block_size)]
            if cursor is not None:
                params.append(("cursor", cursor))
            page = JobSeriesList.model_validate(self._get("/job/series", params))
            yield from page.results
            cursor = page.cursor
