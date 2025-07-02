from __future__ import annotations

import copy
import itertools
import logging
from datetime import UTC, datetime, timedelta
from typing import Generator

import pandas as pd
from prometheus_api_client.metric_range_df import MetricRangeDataFrame

from sarc.config import config

logger = logging.getLogger(__name__)


def curate_label_argument(
    label_name: str, label_values: None | str | list[str]
) -> list[dict[str, str]]:
    """Return empty dict, otherwise a list of dicts"""
    if label_values is None:
        return [{}]
    elif isinstance(label_values, str):
        return [{label_name: label_values}]

    return [{label_name: value} for value in label_values]


def generate_label_configs(
    node_id: None | str | list[str],
    cluster: None | str | list[str],
) -> Generator[dict[str, str], None, None]:
    node_configs = curate_label_argument("instance", node_id)
    cluster_configs = curate_label_argument("cluster", cluster)

    # Create list of label_configs based on node_id and cluster_name
    for node_config, cluster_config in itertools.product(node_configs, cluster_configs):
        query_config = copy.deepcopy(node_config)
        query_config.update(cluster_config)
        # yield node_config | cluster_config
        yield query_config


def timedelta_to_str(delta: timedelta) -> str:
    rval = ""
    if delta.days >= 365:
        years = delta.days // 365
        rval += f"{years}y"
        delta = delta - timedelta(days=365 * years)

    if delta.days >= 7:
        weeks = delta.days // 7
        rval += f"{weeks}w"
        delta = delta - timedelta(days=7 * weeks)

    if delta.days >= 1:
        rval += f"{delta.days}d"
        delta = delta - timedelta(days=delta.days)

    hours = int(delta.total_seconds() // (60 * 60))

    if hours >= 1:
        rval += f"{hours}h"
        delta = delta - timedelta(hours=hours)

    minutes = int(delta.total_seconds() // 60)

    if minutes >= 1:
        rval += f"{minutes}m"
        delta = delta - timedelta(minutes=minutes)

    if delta.seconds >= 1:
        rval += f"{int(delta.seconds)}s"

    return rval


def generate_custom_query(
    metric_name: str,
    label_config: dict,
    start: datetime,
    end: datetime,
    running_window: timedelta,
) -> str:
    now = datetime.utcnow()
    if start >= now:
        raise ValueError(f"Start time ({start}) cannot be in the future.")
    start_offset = now - start

    delta = end - start

    if running_window > delta:
        raise ValueError(
            f"Running window ({running_window}) cannot be larger than `end - start` ({end} - {start})."
        )

    labels = ",".join(f'{key}="{value}"' for key, value in label_config.items())

    start_offset_str = timedelta_to_str(start_offset)
    running_window_str = timedelta_to_str(running_window)
    delta_str = timedelta_to_str(delta)

    return f"avg_over_time({metric_name}{{{labels}}}[{running_window_str}])[{delta_str}:{running_window_str}] offset {start_offset_str}"


def query_prom(
    cluster: str,
    metric_name: str,
    label_config: dict,
    start: datetime,
    end: datetime,
    running_window: timedelta,
) -> list | None:
    query = generate_custom_query(metric_name, label_config, start, end, running_window)

    return config("scraping").clusters[cluster].prometheus.custom_query(query)


def get_nodes_time_series(
    metrics: str | list[str],
    cluster: str | list[str],
    start: datetime,
    end: None | datetime = None,
    node_id: None | str | list[str] = None,
    running_window: timedelta = timedelta(days=1),
) -> pd.DataFrame:
    """Fetch node metrics

    Parameters
    ----------
    metrics: str or list of str
        Name of the metrics to fetch.
    start: datetime
        Starting time (inclusive) of the metrics to fetch.
    end: None | datetime
        End time (inclusive) for the metrics to fetch. If None, fetch until the latest metrics.
    node_id: None, str or list of str
        Name of the nodes for which to fetch the metrics. If None,
        fetch the metrics for all nodes.
    cluster_name: None, str or list of str
        Name of the clusters for which to fetch the metrics. If None,
        fetch the metrics for all clusters.
    running_window: timedelta
        The granularity at which we compute the mean value over time. Default is 1 day.
    """

    if isinstance(metrics, str):
        metrics = [metrics]

    if end is None:
        end = datetime.now(UTC)

    df = None
    for metric_name, label_config in itertools.product(
        metrics, generate_label_configs(node_id, cluster)
    ):
        label_config = copy.deepcopy(label_config)
        rval = query_prom(
            label_config.pop("cluster"),
            metric_name,
            label_config=label_config,
            start=start,
            end=end,
            running_window=running_window,
        )
        if rval:
            metric_df: pd.DataFrame = MetricRangeDataFrame(rval)
        else:
            metric_df = pd.DataFrame()

        if df is not None:
            df = pd.concat([df, metric_df])
        else:
            df = metric_df

    if df is None:
        return pd.DataFrame()

    return df


sources = [
    "netdata_cpu_core_throttling_events_persec_average",
    "netdata_cpu_cpu_percentage_average",
    "netdata_cpu_interrupts_interrupts_persec_average",
    "netdata_cpu_softirqs_softirqs_persec_average",
    "netdata_cpu_softnet_stat_events_persec_average",
    "netdata_cpufreq_cpufreq_MHz_average",
    "netdata_cpuidle_cpu_cstate_residency_time_percentage_average",
    "netdata_cpuidle_cpuidle_percentage_average",
    "netdata_disk_avgsz_KiB_operation_average",
    "netdata_disk_await_milliseconds_operation_average",
    "netdata_disk_backlog_milliseconds_average",
    "netdata_disk_busy_milliseconds_average",
    "netdata_disk_ext_avgsz_KiB_operation_average",
    "netdata_disk_ext_await_milliseconds_operation_average",
    "netdata_disk_ext_io_KiB_persec_average",
    "netdata_disk_ext_iotime_milliseconds_persec_average",
    "netdata_disk_ext_mops_merged_operations_persec_average",
    "netdata_disk_ext_ops_operations_persec_average",
    "netdata_disk_inodes_inodes_average",
    "netdata_disk_io_KiB_persec_average",
    "netdata_disk_iotime_milliseconds_persec_average",
    "netdata_disk_latency_io_calls_persec_average",
    "netdata_disk_mops_merged_operations_persec_average",
    "netdata_disk_ops_operations_persec_average",
    "netdata_disk_qops_operations_average",
    "netdata_disk_space_GiB_average",
    "netdata_disk_svctm_milliseconds_operation_average",
    "netdata_disk_util___of_time_working_average",
    "netdata_ipv4_errors_packets_persec_average",
    "netdata_ipv4_fragsin_packets_persec_average",
    "netdata_ipv4_fragsout_packets_persec_average",
    "netdata_ipv4_icmp_errors_packets_persec_average",
    "netdata_ipv4_icmp_packets_persec_average",
    "netdata_ipv4_icmpmsg_packets_persec_average",
    "netdata_ipv4_packets_packets_persec_average",
    "netdata_ipv4_sockstat_frag_mem_KiB_average",
    "netdata_ipv4_sockstat_frag_sockets_fragments_average",
    "netdata_ipv4_sockstat_raw_sockets_sockets_average",
    "netdata_ipv4_sockstat_sockets_sockets_average",
    "netdata_ipv4_sockstat_tcp_mem_KiB_average",
    "netdata_ipv4_sockstat_tcp_sockets_sockets_average",
    "netdata_ipv4_sockstat_udp_mem_KiB_average",
    "netdata_ipv4_sockstat_udp_sockets_sockets_average",
    "netdata_ipv4_tcperrors_packets_persec_average",
    "netdata_ipv4_tcphandshake_events_persec_average",
    "netdata_ipv4_tcpopens_connections_persec_average",
    "netdata_ipv4_tcppackets_packets_persec_average",
    "netdata_ipv4_tcpsock_active_connections_average",
    "netdata_ipv4_udperrors_events_persec_average",
    "netdata_ipv4_udppackets_packets_persec_average",
    "netdata_ipv6_ect_packets_persec_average",
    "netdata_ipv6_errors_packets_persec_average",
    "netdata_ipv6_icmp_messages_persec_average",
    "netdata_ipv6_icmpechos_messages_persec_average",
    "netdata_ipv6_icmperrors_errors_persec_average",
    "netdata_ipv6_icmpmldv2_reports_persec_average",
    "netdata_ipv6_icmpneighbor_messages_persec_average",
    "netdata_ipv6_icmprouter_messages_persec_average",
    "netdata_ipv6_icmptypes_messages_persec_average",
    "netdata_ipv6_mcast_kilobits_persec_average",
    "netdata_ipv6_mcastpkts_packets_persec_average",
    "netdata_ipv6_packets_packets_persec_average",
    "netdata_ipv6_sockstat6_raw_sockets_sockets_average",
    "netdata_ipv6_sockstat6_tcp_sockets_sockets_average",
    "netdata_ipv6_sockstat6_udp_sockets_sockets_average",
    "netdata_ipv6_udperrors_events_persec_average",
    "netdata_ipv6_udppackets_packets_persec_average",
    "netdata_mem_available_MiB_average",
    "netdata_mem_cachestat_dirties_pages_persec_average",
    "netdata_mem_cachestat_hits_hits_persec_average",
    "netdata_mem_cachestat_misses_misses_persec_average",
    "netdata_mem_cachestat_ratio_percent_average",
    "netdata_mem_committed_MiB_average",
    "netdata_mem_ecc_ce_errors_average",
    "netdata_mem_file_segment_calls_persec_average",
    "netdata_mem_file_sync_calls_persec_average",
    "netdata_mem_hwcorrupt_MiB_average",
    "netdata_mem_kernel_MiB_average",
    "netdata_mem_ksm_MiB_average",
    "netdata_mem_ksm_ratios_percentage_average",
    "netdata_mem_ksm_savings_MiB_average",
    "netdata_mem_memory_map_calls_persec_average",
    "netdata_mem_numa_events_persec_average",
    "netdata_mem_oom_kill_kills_persec_average",
    "netdata_mem_pgfaults_faults_persec_average",
    "netdata_mem_slab_MiB_average",
    "netdata_mem_slabfilling_percent_average",
    "netdata_mem_slabmemory_B_average",
    "netdata_mem_slabwaste_B_average",
    "netdata_mem_sync_calls_persec_average",
    "netdata_mem_transparent_hugepages_MiB_average",
    "netdata_mem_writeback_MiB_average",
    "netdata_net_carrier_state_average",
    "netdata_net_drops_drops_persec_average",
    "netdata_net_duplex_state_average",
    "netdata_net_errors_errors_persec_average",
    "netdata_net_events_events_persec_average",
    "netdata_net_fifo_errors_average",
    "netdata_net_mtu_octets_average",
    "netdata_net_net_kilobits_persec_average",
    "netdata_net_operstate_state_average",
    "netdata_net_packets_packets_persec_average",
    "netdata_net_speed_kilobits_persec_average",
    "netdata_nvidia_smi_bar1_allocated_MiB_average",
    "netdata_nvidia_smi_bar1_memory_usage_MiB_average",
    "netdata_nvidia_smi_clocks_MHz_average",
    "netdata_nvidia_smi_encoder_utilization_percentage_average",
    "netdata_nvidia_smi_fan_speed_percentage_average",
    "netdata_nvidia_smi_gpu_utilization_percentage_average",
    "netdata_nvidia_smi_mem_utilization_percentage_average",
    "netdata_nvidia_smi_memory_allocated_MiB_average",
    "netdata_nvidia_smi_pci_bandwidth_KiB_persec_average",
    "netdata_nvidia_smi_power_Watts_average",
    "netdata_nvidia_smi_processes_mem_MiB_average",
    "netdata_nvidia_smi_temperature_celsius_average",
    "netdata_nvidia_smi_user_mem_MiB_average",
    "netdata_nvidia_smi_user_num_num_average",
    "netdata_sensors_fan_Rotations_min_average",
    "netdata_sensors_power_Watt_average",
    "netdata_sensors_temperature_Celsius_average",
    "netdata_services_cpu_percentage_average",
    "netdata_services_io_ops_read_operations_persec_average",
    "netdata_services_io_ops_write_operations_persec_average",
    "netdata_services_io_read_KiB_persec_average",
    "netdata_services_io_write_KiB_persec_average",
    "netdata_services_mem_usage_MiB_average",
    "netdata_services_merged_io_ops_read_operations_persec_average",
    "netdata_services_merged_io_ops_write_operations_persec_average",
    "netdata_services_swap_usage_MiB_average",
    "netdata_services_throttle_io_ops_read_operations_persec_average",
    "netdata_services_throttle_io_ops_write_operations_persec_average",
    "netdata_services_throttle_io_read_KiB_persec_average",
    "netdata_services_throttle_io_write_KiB_persec_average",
    "netdata_system_active_processes_processes_average",
    "netdata_system_clock_status_status_average",
    "netdata_system_clock_sync_offset_milliseconds_average",
    "netdata_system_clock_sync_state_state_average",
    "netdata_system_cpu_full_pressure_percentage_average",
    "netdata_system_cpu_full_pressure_stall_time_ms_average",
    "netdata_system_cpu_percentage_average",
    "netdata_system_cpu_pressure_percentage_average",
    "netdata_system_cpu_some_pressure_percentage_average",
    "netdata_system_cpu_some_pressure_stall_time_ms_average",
    "netdata_system_ctxt_context_switches_persec_average",
    "netdata_system_entropy_entropy_average",
    "netdata_system_exit_calls_persec_average",
    "netdata_system_forks_processes_persec_average",
    "netdata_system_hardirq_latency_milliseconds_average",
    "netdata_system_idlejitter_microseconds_lost_persec_average",
    "netdata_system_interrupts_interrupts_persec_average",
    "netdata_system_intr_interrupts_persec_average",
    "netdata_system_io_full_pressure_percentage_average",
    "netdata_system_io_full_pressure_stall_time_ms_average",
    "netdata_system_io_KiB_persec_average",
    "netdata_system_io_some_pressure_percentage_average",
    "netdata_system_io_some_pressure_stall_time_ms_average",
    "netdata_system_ip_kilobits_persec_average",
    "netdata_system_ipc_semaphore_arrays_arrays_average",
    "netdata_system_ipc_semaphores_semaphores_average",
    "netdata_system_ipv6_kilobits_persec_average",
    "netdata_system_load_load_average",
    "netdata_system_memory_full_pressure_percentage_average",
    "netdata_system_memory_full_pressure_stall_time_ms_average",
    "netdata_system_memory_some_pressure_percentage_average",
    "netdata_system_memory_some_pressure_stall_time_ms_average",
    "netdata_system_message_queue_bytes_bytes_average",
    "netdata_system_message_queue_messages_messages_average",
    "netdata_system_net_kilobits_persec_average",
    "netdata_system_pgpgio_KiB_persec_average",
    "netdata_system_process_status_difference_average",
    "netdata_system_process_thread_calls_persec_average",
    "netdata_system_processes_processes_average",
    "netdata_system_processes_state_processes_average",
    "netdata_system_ram_MiB_average",
    "netdata_system_shared_memory_bytes_bytes_average",
    "netdata_system_shared_memory_calls_calls_persec_average",
    "netdata_system_shared_memory_segments_segments_average",
    "netdata_system_softirq_latency_milliseconds_average",
    "netdata_system_softirqs_softirqs_persec_average",
    "netdata_system_softnet_stat_events_persec_average",
    "netdata_system_swap_MiB_average",
    "netdata_system_swapcalls_calls_persec_average",
    "netdata_system_swapio_KiB_persec_average",
    "netdata_system_uptime_seconds_average",
    "netdata_users_cpu_percentage_average",
    "netdata_users_cpu_system_percentage_average",
    "netdata_users_cpu_user_percentage_average",
    "netdata_users_files_open_files_average",
    "netdata_users_lreads_KiB_persec_average",
    "netdata_users_lwrites_KiB_persec_average",
    "netdata_users_major_faults_page_faults_persec_average",
    "netdata_users_mem_MiB_average",
    "netdata_users_minor_faults_page_faults_persec_average",
    "netdata_users_pipes_open_pipes_average",
    "netdata_users_preads_KiB_persec_average",
    "netdata_users_processes_processes_average",
    "netdata_users_pwrites_KiB_persec_average",
    "netdata_users_sockets_open_sockets_average",
    "netdata_users_swap_MiB_average",
    "netdata_users_threads_threads_average",
    "netdata_users_uptime_avg_seconds_average",
    "netdata_users_uptime_max_seconds_average",
    "netdata_users_uptime_min_seconds_average",
    "netdata_users_uptime_seconds_average",
    "netdata_users_vmem_MiB_average",
]


def get_nodes_metric_names() -> list[str]:
    return sources
