from sqlmodel import Session

from sarc.patch import declare_patch


@declare_patch
def patch_gpu_types(sess: Session):
    """
    Patch jobs to harmonize GPU names.

    Make sure each GPU name in SlurmJobDB table is a standard (harmonized) name
    (either IGUANE name or "<iguane> : <mig>" name) also present in GpuRguDB table.

    Won't modify job if GPU name cannot be harmonized. This may still happen,
    for e.g. if a job requested a GPU that doesn't exist on cluster.
    """
