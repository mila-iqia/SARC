from sarc.config import ClusterConfig, BaseModel, config
from pydantic_mongo import AbstractRepository, ObjectIdField


class DiskUsageSize(BaseModel):
    value: float
    unit: str


class DiskUsageUser(BaseModel):
    username: str
    nbr_files: int
    size: DiskUsageSize


class DiskUsageGroup(BaseModel):
    group_name: str
    users: list[DiskUsageUser]


class DiskUsage(BaseModel):
    """
    Disk usage on a given cluster
    """

    # # Database ID
    id: ObjectIdField = None

    cluster_name: str
    groups: list[DiskUsageGroup]


class ClusterDiskUsageRepository(AbstractRepository[DiskUsage]):
    class Meta:
        collection_name = "diskusage"

    def add(self, disk_usage: DiskUsage):
        document = self.to_document(disk_usage)
        query_attrs = ["cluster_name"]  # only key for the moment
        query = {key: document[key] for key in query_attrs}
        return self.get_collection().update_one(query, {"$set": document}, upsert=True)


def get_diskusage_collection():
    db = config().mongo.instance
    return ClusterDiskUsageRepository(database=db)


def get_diskusages(cluster_name: str | list[str]) -> list[DiskUsage]:
    collection = get_diskusage_collection()

    query = {}
    if isinstance(cluster_name, str):
        query["cluster_name"] = cluster_name
    else:
        query["cluster_name"] = {"$in": cluster_name}

    return list(collection.find_by(query))
