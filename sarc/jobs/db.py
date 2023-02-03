from pydantic_mongo import AbstractRepository
from tqdm import tqdm

from ..config import config
from .sacct import SAcctScraper, SlurmJob


class SlurmJobRepository(AbstractRepository[SlurmJob]):
    class Meta:
        collection_name = "jobs"

    def save(self, model):
        """Save a SlurmJob into the database.

        Note: This overrides AbstractRepository's save function to do an upsert when
        the id is provided.
        """
        document = self.to_document(model)
        if model.id:
            mongo_id = document.pop("_id")
            return self.get_collection().update_one(
                {"_id": mongo_id}, {"$set": document}, upsert=True
            )
        result = self.get_collection().insert_one(document)
        model.id = result.inserted_id
        return result


def sacct_mongodb_import(cluster, day) -> None:
    """Fetch sacct data and store it in MongoDB.

    Arguments:
        cluster: The cluster on which to fetch the data.
        day: The day for which to fetch the data. The time does not matter.
    """
    collection = jobs_collection()
    scraper = SAcctScraper(cluster, day)
    print("Getting the sacct data...")
    scraper.get_raw()
    print(f"Saving into mongodb collection '{collection.Meta.collection_name}'...")
    for entry in tqdm(scraper):
        collection.save(entry)
    print(f"Saved {len(scraper)} entries.")


def jobs_collection():
    """Return the jobs collection in the current MongoDB."""
    db = config().mongo.instance
    return SlurmJobRepository(database=db)
