from dataclasses import dataclass
from simple_parsing import field


@dataclass
class DbPrometheusBackup:
    skip_cache: bool = field(
        type=bool,
        default=False,
        help="If True, skip jobs which already have prometheus cached data in <sarc-cache>/prometheus",
    )

    def execute(self) -> int:
        print("skip cache?", self.skip_cache)
        return 0
