# Slurm info

After DB is initialized, we must first parse Slurm configuration files from each
DRAC cluster to extract important cluster data:
- GPU types for each node. This is used during [jobs acquisition (next step)](cli_jobs.md)
  to get GPU type for each job if cluster does not have a Prometheus connection.
- GPU billing for each GPU type. This is useful to [get RGU in job series](rgu.md).

Examples:
```
uv run sarc fetch slurmconfig -c cedar
uv run sarc parse slurmconfig -c cedar
```

## Node -> GPU type

- MongoDB collection: `node_gpu_mapping`
- Python class: `sarc.jobs.node_gpu_mapping.NodeGPUMapping`

## GPU type -> GPU billing

- MongoDB collection: `gpu_billing`
- Python class: `sarc.client.gpumetrics.GPUBilling`
