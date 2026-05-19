from datetime import date

from pydantic import BaseModel


class SlurmCluster(BaseModel):
    id: int | None = None

    name: str
    domain: str
    start_date: date
    billing_is_gpu: bool = False
