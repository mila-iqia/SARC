from datetime import timedelta, datetime, UTC

import sqlalchemy
from sqlmodel import func, case, col, Session, select

from sarc.db.job import SlurmJobDB


class SqlSymbols:
    # Effective start_time: fall back to submit_time when the job never started.
    eff_start = func.coalesce(SlurmJobDB.start_time, SlurmJobDB.submit_time)

    # Effective end_time: end_time if recorded; else `start_time + elapsed_time` for jobs that started
    # (still running or transitioning); else submit_time, so jobs that never ran
    # (e.g. PENDING) collapse to a point at submission.
    eff_end = case(
        (col(SlurmJobDB.end_time).is_not(None), SlurmJobDB.end_time),
        (
            col(SlurmJobDB.start_time).is_not(None),
            SlurmJobDB.start_time
            + SlurmJobDB.elapsed_time
            * sqlalchemy.literal(timedelta(seconds=1), type_=sqlalchemy.Interval),
        ),
        else_=SlurmJobDB.submit_time,
    )

    @classmethod
    def convert_job_time_interval_to_sql_bounds(
        cls, sess: Session, time_interval: timedelta | None
    ):
        eff_start = cls.eff_start
        eff_end = cls.eff_end

        # Determine [start, end] bounds for frame iteration.
        # We compute clip elapsed time if start and end are available,
        # so that minimum_runtime is compared to job running time in given interval.
        if time_interval is None:
            start, end = sess.exec(
                select(func.min(eff_start), func.max(eff_end))
            ).one_or_none()
            clipped_elapsed_time = SlurmJobDB.elapsed_time * sqlalchemy.literal(
                timedelta(seconds=1), type_=sqlalchemy.Interval
            )
        else:
            end = datetime.now(tz=UTC)
            start = end - time_interval
            clipped_elapsed_time = func.least(eff_end, end) - func.greatest(
                eff_start, start
            )

        return start, end, clipped_elapsed_time
