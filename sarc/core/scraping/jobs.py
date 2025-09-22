from datetime import datetime
import logging
from pydantic import BaseModel
from typing import Type

from sarc.core.models.jobs import Job

# Logging
logger = logging.getLogger(__name__)


class JobScraper[T](BaseModel):
    """
    Plugin-agnostic class to retrieve jobs from a data source
    """
    name: str
    config_type: Type[T]

    # TODO: Is it needed to add the with_cache decorator here?
    def get_raw(self, day: datetime) -> dict:
        """
        Fetch the raw job data
        """
        logger.info(f"The function get_raw has not been implemented for JobScraper {self.name}")
        

