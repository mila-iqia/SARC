from typing import Sequence

import sqlmodel
from sqlmodel import Session

from sarc.config import config
from sarc.db.job import SlurmJobDB
from sarc.db.support import GpuRguDB


# @register
def patch_gpu_types(sess: Session):
    pass


def get_jobs_without_harmonized_gpu_types(sess: Session) -> Sequence[SlurmJobDB]:
    query = (
        sqlmodel.select(SlurmJobDB)
        .outerjoin(GpuRguDB, SlurmJobDB.allocated_gpu_type == GpuRguDB.name)
        .where(
            sqlmodel.col(SlurmJobDB.allocated_gpu_type).is_not(None),
            sqlmodel.col(GpuRguDB.name).is_(None),
        )
    )
    return sess.exec(query).all()


def main():
    cfg = config()
    with cfg.db.session() as sess:
        jobs = get_jobs_without_harmonized_gpu_types(sess)
        print(len(jobs))
        for job in jobs:
            print(job.allocated_gpu_type)


if __name__ == '__main__':
    main()
