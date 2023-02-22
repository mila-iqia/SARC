from datetime import datetime

import pytest

from sarc.config import config


@pytest.fixture
def init_empty_db():
    db = config().mongo.instance
    db.allocations.drop()
    yield db


@pytest.fixture
def db_allocations():
    return [
        {
            "start": datetime(year=2017, month=4, day=1),
            "end": datetime(year=2018, month=4, day=1),
            "timestamp": datetime(year=2023, month=2, day=1),
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
            "start": datetime(year=2017, month=4, day=1),
            "end": datetime(year=2018, month=4, day=1),
            "timestamp": datetime(year=2023, month=2, day=1),
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
            "start": datetime(year=2018, month=4, day=1),
            "end": datetime(year=2019, month=4, day=1),
            "timestamp": datetime(year=2023, month=2, day=1),
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
            "start": datetime(year=2018, month=4, day=1),
            "end": datetime(year=2019, month=4, day=1),
            "timestamp": datetime(year=2023, month=2, day=1),
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
            "start": datetime(year=2019, month=4, day=1),
            "end": datetime(year=2020, month=4, day=1),
            "timestamp": datetime(year=2023, month=2, day=1),
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
            "start": datetime(year=2019, month=4, day=1),
            "end": datetime(year=2020, month=4, day=1),
            "timestamp": datetime(year=2023, month=2, day=1),
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
            "start": datetime(year=2020, month=4, day=1),
            "end": datetime(year=2021, month=4, day=1),
            "timestamp": datetime(year=2023, month=2, day=1),
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
            "start": datetime(year=2020, month=4, day=1),
            "end": datetime(year=2021, month=4, day=1),
            "timestamp": datetime(year=2023, month=2, day=1),
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
            "start": datetime(year=2020, month=4, day=1),
            "end": datetime(year=2021, month=4, day=1),
            "timestamp": datetime(year=2023, month=2, day=1),
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
            "start": datetime(year=2020, month=4, day=1),
            "end": datetime(year=2021, month=4, day=1),
            "timestamp": datetime(year=2023, month=2, day=1),
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
            "start": datetime(year=2020, month=4, day=1),
            "end": datetime(year=2021, month=4, day=1),
            "timestamp": datetime(year=2023, month=2, day=1),
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


@pytest.fixture
def init_db_with_allocations(init_empty_db, db_allocations):
    db = init_empty_db
    db.allocations.insert_many(db_allocations)
