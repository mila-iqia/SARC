import pytest

from sarc.config import config


@pytest.fixture
def init_empty_db():
    db = config().mongo.instance
    db.allocations.drop()
    yield db


@pytest.fixture
def init_db_with_allocations(init_empty_db):
    db = init_empty_db
    db.allocations.insert_many(
        [
            {
                "start": "2017-04-01T00:00:00",
                "end": "2018-04-01T00:00:00",
                "timestamp": "2023-02-01T00:00:00",
                "cluster_name": "fromage",
                "resource_name": "fromage-gpu",
                "group_name": "rrg-bonhomme-ad",
                "resources": {
                    "compute": {
                        "cpu_year": None,
                        "gpu_year": 100,
                        "vcpu_year": None,
                        "vgpu_year": None,
                    },
                    "storage": {
                        "project_size": None,
                        "project_inodes": None,
                        "nearline": None,
                    },
                },
            },
            {
                "start": "2017-04-01T00:00:00",
                "end": "2018-04-01T00:00:00",
                "timestamp": "2023-02-01T00:00:00",
                "cluster_name": "fromage",
                "resource_name": "fromage-storage",
                "group_name": "rrg-bonhomme-ad",
                "resources": {
                    "compute": {
                        "cpu_year": None,
                        "gpu_year": None,
                        "vcpu_year": None,
                        "vgpu_year": None,
                    },
                    "storage": {
                        "project_size": "50TB",
                        "project_inodes": "5e6",
                        "nearline": "15TB",
                    },
                },
            },
            {
                "start": "2018-04-01T00:00:00",
                "end": "2019-04-01T00:00:00",
                "timestamp": "2023-02-01T00:00:00",
                "cluster_name": "fromage",
                "resource_name": "fromage-gpu",
                "group_name": "rrg-bonhomme-ad",
                "resources": {
                    "compute": {
                        "cpu_year": None,
                        "gpu_year": 100,
                        "vcpu_year": None,
                        "vgpu_year": None,
                    },
                    "storage": {
                        "project_size": None,
                        "project_inodes": None,
                        "nearline": None,
                    },
                },
            },
            {
                "start": "2018-04-01T00:00:00",
                "end": "2019-04-01T00:00:00",
                "timestamp": "2023-02-01T00:00:00",
                "cluster_name": "fromage",
                "resource_name": "fromage-storage",
                "group_name": "rrg-bonhomme-ad",
                "resources": {
                    "compute": {
                        "cpu_year": None,
                        "gpu_year": None,
                        "vcpu_year": None,
                        "vgpu_year": None,
                    },
                    "storage": {
                        "project_size": "70TB",
                        "project_inodes": None,
                        "nearline": None,
                    },
                },
            },
            {
                "start": "2019-04-01T00:00:00",
                "end": "2020-04-01T00:00:00",
                "timestamp": "2023-02-01T00:00:00",
                "cluster_name": "patate",
                "resource_name": "patate-gpu",
                "group_name": "rrg-bonhomme-ad",
                "resources": {
                    "compute": {
                        "cpu_year": None,
                        "gpu_year": 190,
                        "vcpu_year": None,
                        "vgpu_year": None,
                    },
                    "storage": {
                        "project_size": None,
                        "project_inodes": None,
                        "nearline": None,
                    },
                },
            },
            {
                "start": "2019-04-01T00:00:00",
                "end": "2020-04-01T00:00:00",
                "timestamp": "2023-02-01T00:00:00",
                "cluster_name": "patate",
                "resource_name": "patate-storage",
                "group_name": "rrg-bonhomme-ad",
                "resources": {
                    "compute": {
                        "cpu_year": None,
                        "gpu_year": None,
                        "vcpu_year": None,
                        "vgpu_year": None,
                    },
                    "storage": {
                        "project_size": "90TB",
                        "project_inodes": "5e6",
                        "nearline": "90TB",
                    },
                },
            },
            {
                "start": "2020-04-01T00:00:00",
                "end": "2021-04-01T00:00:00",
                "timestamp": "2023-02-01T00:00:00",
                "cluster_name": "fromage",
                "resource_name": "fromage-gpu",
                "group_name": "rrg-bonhomme-ad",
                "resources": {
                    "compute": {
                        "cpu_year": None,
                        "gpu_year": 130,
                        "vcpu_year": None,
                        "vgpu_year": None,
                    },
                    "storage": {
                        "project_size": None,
                        "project_inodes": None,
                        "nearline": None,
                    },
                },
            },
            {
                "start": "2020-04-01T00:00:00",
                "end": "2021-04-01T00:00:00",
                "timestamp": "2023-02-01T00:00:00",
                "cluster_name": "fromage",
                "resource_name": "fromage-storage",
                "group_name": "rrg-bonhomme-ad",
                "resources": {
                    "compute": {
                        "cpu_year": None,
                        "gpu_year": None,
                        "vcpu_year": None,
                        "vgpu_year": None,
                    },
                    "storage": {
                        "project_size": "30TB",
                        "project_inodes": "5e6",
                        "nearline": None,
                    },
                },
            },
            {
                "start": "2020-04-01T00:00:00",
                "end": "2021-04-01T00:00:00",
                "timestamp": "2023-02-01T00:00:00",
                "cluster_name": "patate",
                "resource_name": "patate-compute",
                "group_name": "rrg-bonhomme-ad",
                "resources": {
                    "compute": {
                        "cpu_year": 219,
                        "gpu_year": None,
                        "vcpu_year": None,
                        "vgpu_year": None,
                    },
                    "storage": {
                        "project_size": None,
                        "project_inodes": None,
                        "nearline": None,
                    },
                },
            },
            {
                "start": "2020-04-01T00:00:00",
                "end": "2021-04-01T00:00:00",
                "timestamp": "2023-02-01T00:00:00",
                "cluster_name": "patate",
                "resource_name": "patate-gpu",
                "group_name": "rrg-bonhomme-ad",
                "resources": {
                    "compute": {
                        "cpu_year": None,
                        "gpu_year": 200,
                        "vcpu_year": None,
                        "vgpu_year": None,
                    },
                    "storage": {
                        "project_size": None,
                        "project_inodes": None,
                        "nearline": None,
                    },
                },
            },
            {
                "start": "2020-04-01T00:00:00",
                "end": "2021-04-01T00:00:00",
                "timestamp": "2023-02-01T00:00:00",
                "cluster_name": "patate",
                "resource_name": "patate-storage",
                "group_name": "rrg-bonhomme-ad",
                "resources": {
                    "compute": {
                        "cpu_year": None,
                        "gpu_year": None,
                        "vcpu_year": None,
                        "vgpu_year": None,
                    },
                    "storage": {
                        "project_size": "70TB",
                        "project_inodes": "5e6",
                        "nearline": "80TB",
                    },
                },
            },
        ]
    )
