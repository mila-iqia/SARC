from sqlmodel import Field, SQLModel


class SlurmCluster(SQLModel):
    """Hold data for a Slurm cluster."""

    # Database ID
    id: int | None = Field(default=None, primary_key=True)

    cluster_name: str
    start_date: str | None = None
    end_time_sacct: str | None = None
    end_time_prometheus: str | None = None
    billing_is_gpu: bool = False
