from datetime import datetime

import pymongo
import pytest

from sarc.config import config
from sarc.jobs import get_jobs
from sarc.jobs.job import jobs_collection


def drop_job():
    coll = jobs_collection().get_collection()
    coll.drop()


def write_to_job():
    coll = jobs_collection().get_collection()
    coll.update_one(
        {"job_id": "123", "cluster_name": "mila", "submit_time": datetime.utcnow()},
        {
            "$set": {
                "job_id": "123",
                "cluster_name": "mila",
                "submit_time": datetime.utcnow(),
            },
        },
        upsert=True,
    )


def read_job():
    jobs = list(get_jobs(**{"job_state": "COMPLETED"}))
    assert len(jobs) == 1


@pytest.mark.usefixtures("read_setup")
def test_read_only_user_cannot_write(freeport):
    assert (
        config().mongo.connection_string
        == f"mongodb://user_name:user_pass@localhost:{freeport}/sarc"
    )

    with pytest.raises(pymongo.errors.OperationFailure) as exc_info:
        write_to_job()

    assert "not authorized on sarc to execute command" in str(exc_info.value)


@pytest.mark.usefixtures("read_setup")
def test_read_only_user_cannot_read_secrets(freeport):
    assert (
        config().mongo.connection_string
        == f"mongodb://user_name:user_pass@localhost:{freeport}/sarc"
    )

    with pytest.raises(pymongo.errors.OperationFailure) as exc_info:
        for doc in config().mongo.database_instance.secrets.find({}):
            print(doc)

    assert "not authorized on sarc to execute command" in str(exc_info.value)


@pytest.mark.usefixtures("read_setup")
def test_read_only_user_can_read(freeport):
    assert (
        config().mongo.connection_string
        == f"mongodb://user_name:user_pass@localhost:{freeport}/sarc"
    )

    read_job()


@pytest.mark.usefixtures("write_setup")
def test_write_user_can_read(freeport):
    assert (
        config().mongo.connection_string
        == f"mongodb://write_name:write_pass@localhost:{freeport}/sarc"
    )

    read_job()


@pytest.mark.usefixtures("write_setup")
def test_write_user_can_write(freeport):
    assert (
        config().mongo.connection_string
        == f"mongodb://write_name:write_pass@localhost:{freeport}/sarc"
    )

    write_to_job()


@pytest.mark.usefixtures("write_setup")
def test_write_user_can_delete(freeport):
    assert (
        config().mongo.connection_string
        == f"mongodb://write_name:write_pass@localhost:{freeport}/sarc"
    )

    drop_job()
