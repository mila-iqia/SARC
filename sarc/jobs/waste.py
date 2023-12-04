def compute_cost_and_waste(full_df):
    full_df = _compute_cost_and_wastes(full_df, "cpu")
    full_df = _compute_cost_and_wastes(full_df, "gpu")
    return full_df


def _compute_cost_and_wastes(data, device):
    device_col = {"cpu": "cpu", "gpu": "gres_gpu"}[device]

    data[f"{device}_cost"] = data["elapsed_time"] * data[f"requested.{device_col}"]
    data[f"{device}_waste"] = (1 - data[f"{device}_utilization"]) * data[
        f"{device}_cost"
    ]

    data[f"{device}_equivalent_cost"] = (
        data["elapsed_time"] * data[f"allocated.{device_col}"]
    )
    data[f"{device}_equivalent_waste"] = (1 - data[f"{device}_utilization"]) * data[
        f"{device}_equivalent_cost"
    ]

    data[f"{device}_overbilling_cost"] = data["elapsed_time"] * (
        data[f"allocated.{device_col}"] - data[f"requested.{device_col}"]
    )

    return data
