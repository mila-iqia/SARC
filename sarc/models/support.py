from pydantic import BaseModel


class GpuRgu(BaseModel):
    name: str
    rgu: float
    drac_rgu: float
