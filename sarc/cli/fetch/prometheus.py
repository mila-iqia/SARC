import logging
from dataclasses import dataclass
from datetime import datetime

from simple_parsing import field

from sarc.config import config
from sarc.jobs.prometheus_scraping import fetch_prometheus
from sarc.traces import using_trace

logger = logging.getLogger(__name__)


@dataclass
class FetchPrometheus:
    cluster_names: list[str] = field(alias=["-c"], default_factory=list)
    after: str | None = field(
        default=None, help="Only fetch data for jobs after this date"
    )
    max_jobs: int | None = field(
        type=int, default=None, help="Max number of jobs  to fetch"
    )

    def execute(self) -> int:
        after = None
        if self.after is not None:
            after = datetime.fromisoformat(self.after)

        cfg = config("scraping")
        clusters_configs = cfg.clusters

        for cluster_name in self.cluster_names:
            cluster = clusters_configs[cluster_name]
            if not cluster.prometheus_url:
                logger.error(
                    f"No prometheus URL for cluster: {cluster_name}, cannot get Prometheus metrics."
                )
                continue
            try:
                with using_trace(
                    "FetchPrometheus", "fetch_prometheus_metrics", exception_types=()
                ) as span:
                    span.set_attribute("cluster_name", cluster_name)
                    logger.info(
                        f"Acquire Prometheus metrics on {cluster_name} for jobs after {after}"
                    )
                    with config().db.session() as sess:
                        fetch_prometheus(sess, cluster, after, self.max_jobs)

            except Exception as e:
                logger.error(
                    f"Error while acquiring Prometheus metrics on {cluster_name}: "
                    f"{type(e).__name__}: {e} ; skipping cluster."
                )
                # Continue to next cluster.
                continue
        return 0
