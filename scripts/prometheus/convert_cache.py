import sys
from collections.abc import Generator
from datetime import datetime, timedelta
from pathlib import Path

from sarc.cache import Cache
from sarc.client.job import _jobs_collection


def usage():
    print("Usage:")
    print(f"  {sys.argv[0]} SOURCE_DIR TARGET_DIR")


if len(sys.argv) != 3:
    usage()
    sys.exit(1)

source = Path(sys.argv[1])
target = Path(sys.argv[2])

assert source.exists()
assert source.is_dir()


def walk_cache(p: Path) -> Generator[Path]:
    for year in sorted(p.iterdir()):
        for month in sorted(year.iterdir()):
            yield from sorted(month.iterdir())


DATE_FORMAT = "%Y-%m-%dT%Hh%Mm%Ss"
almost_one_day = timedelta(days=1) - timedelta(microseconds=1)
cache = Cache("prometheus")
collection = _jobs_collection()

for day in walk_cache(source):
    month = day.parent
    year = month.parent
    timepoint = (
        datetime(year=int(year.name), month=int(month.name), day=int(day.name))
        + almost_one_day
    )
    with cache.create_entry(at_time=timepoint) as ce:
        for f in day.iterdir():
            print(f.name)
            cluster_name, job_id_str, start_to_end, metrics, *_ = f.name.split(".")
            if (
                metrics != "cu+f16g+f32g+f64g+mus+pwg+sog+ug+ugm"
            ):  # TODO: check if ok with older entries
                continue
            job_id = int(job_id_str)
            start_str, _ = start_to_end.split("_to_")
            start = datetime.strptime(start_str, DATE_FORMAT)
            # This searches for a job that has the same cluster and id as the
            # cache entry and has the submit time that is closest, but still
            # lower than the start time. This is because we need the submit
            # time, but it wasn't stored in the original cache format so we make
            # a guesstimate using the start time.
            entry = collection.to_model(
                collection.get_collection()
                .find(
                    {
                        "cluster_name": cluster_name,
                        "job_id": job_id,
                        "submit_time": {"$lte": {start}},
                    }
                )
                .sort("submit_time", -1)
                .limit(1)
                .next()
            )
            # This is just to make sure that the mongo query above is not broken
            assert entry.cluster_name == cluster_name
            assert entry.job_id == job_id
            assert entry.submit_time <= start
            ce.add_value(
                f"{entry.cluster_name}${entry.job_id}${entry.submit_time.isoformat(timespec='seconds')}",
                f.read_bytes(),
            )
