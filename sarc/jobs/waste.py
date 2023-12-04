def compute_cost_and_waste(full_df):
    cpu_jobs = get_cpu_jobs(full_df)
    gpu_jobs = get_gpu_jobs(full_df)

    def compute_device_cost(data, device):
        return data["duration"] * data[device]

    def compute_wastes(data, device):
        data[f"{device}_cost"] = compute_device_cost(data, device)
        data[f"{device}_waste"] = (1 - data[f"{device}_utilization"]) * data[f"{device}_cost"]
        # TODO: Add device-equivalent if available.
        return data

    full_df = compute_wastes(full_df, "cpu")
    full_df = compute_wastes(full_df, "gpu")

    return full_df


def get_cpu_jobs(data):
    return data[data["gpu_requested"] == 0]


def get_gpu_jobs(data):
    return data[data["gpu_requested"] > 0]
