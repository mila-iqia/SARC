# RGU

SARC can compute RGU cost per job using functions from sarc `client` module:
1) `load_job_series`: to get jobs in a Pandas dataframe, easier to manipulate.
2) `update_job_series_rgu`: to add columns with RGU information in jobs dataframe.
  This function adds two main columns:
  - `allocated.gpu_type_rgu`: RGU cost per GPU (RGU/GPU ratio)
  - `allocated.gres_rgu`: RGU cost for each job
    (number of GPU allocated to the job * RGU/GPU ratio)

```
from sarc.client import load_job_series, update_job_series_rgu

frame = load_job_series()
update_job_series_rgu(frame)
```

## How it works

1) We re-compute number of GPUs per job by dividing job billing with
  [GPU billing extracted from Slurm configuration file](cli_slurmconfig.md)
2) We then multiply number of GPUs with
  RGU/GPU ratio retrieved from [package `iguane`](https://github.com/mila-iqia/IGUANE/)
