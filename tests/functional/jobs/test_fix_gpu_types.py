"""
Functional tests for the GPU-name harmonization fix (sarc.scraping.gpu_fixes).

These tests exercise ``fix_gpu_types`` end-to-end against a real database
session (``jobless_read_write_db``: clusters + users seeded, no jobs).

They rely on the ``raisin_no_prometheus`` cluster of ``tests/sarc-test.yaml``,
where the GPU alias ``asupergpu`` harmonizes differently depending on the node:
- on ``cn-c018``        -> "Nec Plus Plus ULTRA GPU 2000"
- on ``cn-c[019-030]``  -> "Nec Plus ULTRA GPU 2000"

A job spanning ``cn-c018`` + ``cn-c019`` therefore yields two distinct
harmonized names: the corner case ``fix_gpu_types`` is meant to handle.

Note: these harmonized names are *not* IGUANE/RAWDATA names, so they are absent
from GpuRguDB after ``insert_rgu``. Each test seeds GpuRguDB explicitly with the
RGU values it needs.
"""

import logging
from datetime import datetime

import pytest
from sqlmodel import select

from sarc.config import UTC
from sarc.db.cluster import SlurmClusterDB
from sarc.db.job import SlurmJobDB
from sarc.db.support import GpuRguDB
from sarc.db.users import UserDB
from sarc.models.job import SlurmState
from sarc.scraping.gpu_fixes import (
    HarmonizedNameNotInRguError,
    fix_gpu_types,
    get_gpu_jobs_without_harmonized_gpu_types,
)

CLUSTER = "raisin_no_prometheus"
# asupergpu harmonized names, per node, on that cluster:
GPU_C018 = "Nec Plus Plus ULTRA GPU 2000"
GPU_C019 = "Nec Plus ULTRA GPU 2000"

_TS = datetime(2023, 2, 14, tzinfo=UTC)


def _cluster_and_user(sess):
    cluster = sess.exec(
        select(SlurmClusterDB).where(SlurmClusterDB.name == CLUSTER)
    ).one()
    user = sess.exec(select(UserDB)).first()
    return cluster, user


def _add_rgu(sess, name, rgu, drac_rgu=None):
    sess.add(
        GpuRguDB(name=name, rgu=rgu, drac_rgu=rgu if drac_rgu is None else drac_rgu)
    )


def _count_rgu(sess):
    return len(sess.exec(select(GpuRguDB)).all())


def _add_gpu_job(
    sess, cluster, user, *, gpu_type, nodes, job_id, harmonized_gpu_type=None
):
    job = SlurmJobDB(
        cluster_id=cluster.id,
        sarc_user_id=user.id,
        account="acct",
        job_id=job_id,
        name="job",
        cluster_user="user",
        group="group",
        job_state=SlurmState.COMPLETED,
        partition="long",
        nodes=nodes,
        work_dir="/wd",
        submit_line=None,
        elapsed_time=60.0,
        submit_time=_TS,
        start_time=_TS,
        end_time=_TS,
        allocated_gres_gpu=1,
        allocated_gpu_type=gpu_type,
        harmonized_gpu_type=harmonized_gpu_type,
        requested_gres_gpu=1,
    )
    sess.add(job)
    return job


def test_single_harmonized_name(jobless_read_write_db):
    """One node -> one harmonized name: the job is rewritten, GpuRguDB unchanged."""
    sess = jobless_read_write_db
    cluster, user = _cluster_and_user(sess)
    _add_rgu(sess, GPU_C018, 10.0)
    job = _add_gpu_job(
        sess, cluster, user, gpu_type="asupergpu", nodes=["cn-c018"], job_id=1
    )
    sess.commit()
    jid = job.id
    nb_rgu_before = _count_rgu(sess)

    fix_gpu_types(sess)

    assert sess.get(SlurmJobDB, jid).harmonized_gpu_type == GPU_C018
    # A single existing harmonized name must not add any GpuRguDB row.
    assert _count_rgu(sess) == nb_rgu_before


def test_compound_name_when_same_rgu(jobless_read_write_db):
    """Many names with same RGU -> compound name + new GpuRguDB row."""
    sess = jobless_read_write_db
    cluster, user = _cluster_and_user(sess)
    _add_rgu(sess, GPU_C018, 4.8)
    _add_rgu(sess, GPU_C019, 4.8)
    job = _add_gpu_job(
        sess,
        cluster,
        user,
        gpu_type="asupergpu",
        nodes=["cn-c018", "cn-c019"],
        job_id=1,
    )
    sess.commit()
    jid = job.id

    fix_gpu_types(sess)

    expected = ", ".join(sorted([GPU_C018, GPU_C019]))
    assert sess.get(SlurmJobDB, jid).harmonized_gpu_type == expected
    # The compound name must have been registered in GpuRguDB with the shared RGU.
    row = sess.get(GpuRguDB, expected)
    assert row is not None
    assert row.rgu == 4.8
    assert row.drac_rgu == 4.8


def test_many_matchings_when_different_rgu(jobless_read_write_db, caplog):
    """Many names with different RGU -> job left untouched, warning logged."""
    sess = jobless_read_write_db
    cluster, user = _cluster_and_user(sess)
    _add_rgu(sess, GPU_C018, 4.8)
    _add_rgu(sess, GPU_C019, 9.6)
    job = _add_gpu_job(
        sess,
        cluster,
        user,
        gpu_type="asupergpu",
        nodes=["cn-c018", "cn-c019"],
        job_id=1,
    )
    sess.commit()
    jid = job.id
    nb_rgu_before = _count_rgu(sess)

    with caplog.at_level(logging.WARNING, logger="sarc.scraping.gpu_fixes"):
        fix_gpu_types(sess)

    # Job is not modified, and no compound name is created.
    assert sess.get(SlurmJobDB, jid).harmonized_gpu_type is None
    assert _count_rgu(sess) == nb_rgu_before
    assert "many harmonized names with different RGU values" in caplog.text


def test_no_matching_gpu_name(jobless_read_write_db, caplog):
    """A GPU alias that harmonizes to nothing -> job untouched, warning logged."""
    sess = jobless_read_write_db
    cluster, user = _cluster_and_user(sess)
    job = _add_gpu_job(
        sess, cluster, user, gpu_type="nonexistent_gpu", nodes=["cn-c018"], job_id=1
    )
    sess.commit()
    jid = job.id

    with caplog.at_level(logging.WARNING, logger="sarc.scraping.gpu_fixes"):
        fix_gpu_types(sess)

    assert sess.get(SlurmJobDB, jid).harmonized_gpu_type is None
    assert "cannot be harmonized" in caplog.text


def test_fix_gpu_types_is_idempotent(jobless_read_write_db, caplog):
    """Running the fix twice leaves the database untouched on the second pass."""
    sess = jobless_read_write_db
    cluster, user = _cluster_and_user(sess)
    _add_rgu(sess, GPU_C018, 10.0)
    job = _add_gpu_job(
        sess, cluster, user, gpu_type="asupergpu", nodes=["cn-c018"], job_id=1
    )
    sess.commit()
    jid = job.id

    fix_gpu_types(sess)
    assert sess.get(SlurmJobDB, jid).harmonized_gpu_type == GPU_C018
    nb_rgu_after_first = _count_rgu(sess)

    # Second pass: nothing left to harmonize.
    caplog.clear()
    with caplog.at_level(logging.WARNING, logger="sarc.scraping.gpu_fixes"):
        fix_gpu_types(sess)

    assert len(get_gpu_jobs_without_harmonized_gpu_types(sess)) == 0
    assert sess.get(SlurmJobDB, jid).harmonized_gpu_type == GPU_C018
    assert _count_rgu(sess) == nb_rgu_after_first
    assert "jobs with no harmonized names" not in caplog.text


def test_query_ignores_cpu_and_already_harmonized_jobs(jobless_read_write_db):
    """The candidate query skips CPU jobs (no GPU) and already-harmonized jobs."""
    sess = jobless_read_write_db
    cluster, user = _cluster_and_user(sess)
    # CPU job: allocated_gpu_type is None.
    _add_gpu_job(sess, cluster, user, gpu_type=None, nodes=["cn-c018"], job_id=1)
    # Already-harmonized job: "A100-SXM4-80GB" is an IGUANE name already in GpuRguDB.
    _add_gpu_job(
        sess,
        cluster,
        user,
        gpu_type="A100-SXM4-80GB",
        harmonized_gpu_type="A100-SXM4-80GB",
        nodes=["cn-c018"],
        job_id=2,
    )
    # Unharmonized GPU job: the only expected candidate.
    _add_gpu_job(sess, cluster, user, gpu_type="asupergpu", nodes=["cn-c018"], job_id=3)
    sess.commit()

    candidates = get_gpu_jobs_without_harmonized_gpu_types(sess)
    assert [job.job_id for job in candidates] == [3]


def test_single_name_absent_from_rgu_db_raises(jobless_read_write_db):
    """A single harmonized name missing from GpuRguDB is a config/insert_rgu bug.

    It signals either a mis-populated GpuRguDB or a bad gpus_per_nodes config,
    so fix_gpu_types must raise rather than silently write a non-RGU name.
    """
    sess = jobless_read_write_db
    cluster, user = _cluster_and_user(sess)
    # GPU_C018 is intentionally NOT seeded into GpuRguDB.
    _add_gpu_job(sess, cluster, user, gpu_type="asupergpu", nodes=["cn-c018"], job_id=1)
    sess.commit()

    with pytest.raises(HarmonizedNameNotInRguError):
        fix_gpu_types(sess)


def test_multinode_name_absent_from_rgu_db_raises(jobless_read_write_db):
    """In the multi-name branch, any name missing from GpuRguDB must raise too."""
    sess = jobless_read_write_db
    cluster, user = _cluster_and_user(sess)
    # Two distinct harmonized names are produced, but only one is seeded:
    # GPU_C019 (cn-c019) is missing from GpuRguDB.
    _add_rgu(sess, GPU_C018, 4.8)
    _add_gpu_job(
        sess,
        cluster,
        user,
        gpu_type="asupergpu",
        nodes=["cn-c018", "cn-c019"],
        job_id=1,
    )
    sess.commit()

    with pytest.raises(HarmonizedNameNotInRguError):
        fix_gpu_types(sess)
