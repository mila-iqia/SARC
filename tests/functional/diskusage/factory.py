from __future__ import annotations

from datetime import datetime

from sarc.config import UTC


def create_diskusages():
    diskusages = []
    for cluster_name in ["botw", "totk"]:
        for timestamp in [
            datetime(2023, 2, 14, 0, 0, 0, tzinfo=UTC),
            datetime(2021, 12, 1, 0, 0, 0, tzinfo=UTC),
        ]:
            diskusages.append(
                {
                    "cluster_name": cluster_name,
                    "timestamp": timestamp,
                    "groups": [
                        {
                            "group_name": "gerudo",
                            "users": [
                                {"user": "urbosa", "nbr_files": 2, "size": 0},
                                {"user": "riju", "nbr_files": 50, "size": 14484777205},
                                {"user": "mipha", "nbr_files": 2, "size": 0},
                            ],
                        },
                        {
                            "group_name": "piaf",
                            "users": [
                                {
                                    "user": "revali",
                                    "nbr_files": 47085,
                                    "size": 4509715660,
                                },
                            ],
                        },
                    ],
                }
            )
    return diskusages
