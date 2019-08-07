from typing import List, Dict
from datetime import datetime

# All the metrics that can safely be parsed as a float_metric (see parse_galaxy_job_metrics)
galaxy_float_metrics = {"processor_count", "memtotal", "swaptotal", "runtime_seconds", "memory.stat.pgmajfault",
                        "cpu.stat.nr_throttled", "memory.stat.total_rss_huge", "memory.memsw.failcnt",
                        "memory.oom_control.under_oom", "memory.kmem.failcnt", "memory.stat.total_pgfault",
                        "cpu.stat.nr_periods", "cpuacct.stat.user", "memory.stat.active_file", "memory.stat.mapped_file",
                         "memory.stat.rss_huge", "memory.memsw.limit_in_bytes", "memory.use_hierarchy", "memory.stat.cache",
                        "memory.stat.hierarchical_memsw_limit", "memory.kmem.tcp.failcnt", "cpu.rt_runtime_us",
                        "memory.stat.hierarchical_memory_limit", "memory.stat.total_cache", "memory.kmem.max_usage_in_bytes",
                        "cpuacct.usage", "memory.stat.total_pgmajfault", "memory.kmem.usage_in_bytes",
                        "memory.stat.inactive_file", "memory.swappiness", "memory.move_charge_at_immigrate",
                        "memory.memsw.usage_in_bytes", "cpu.rt_period_us", "memory.stat.inactive_anon", "memory.stat.swap",
                        "memory.stat.active_anon", "memory.stat.pgpgin", "memory.stat.total_inactive_file",
                        "memory.oom_control.oom_kill_disable", "memory.stat.total_pgpgout", "memory.stat.total_unevictable",
                        "memory.kmem.tcp.limit_in_bytes", "memory.stat.pgpgout", "memory.usage_in_bytes",
                        "memory.failcnt", "memory.memsw.max_usage_in_bytes", "memory.limit_in_bytes",
                        "memory.soft_limit_in_bytes", "memory.kmem.tcp.max_usage_in_bytes", "memory.stat.total_rss",
                        "cpu.shares", "memory.stat.total_swap", "memory.stat.rss", "memory.kmem.tcp.usage_in_bytes",
                        "cpu.stat.throttled_time", "memory.stat.unevictable", "memory.kmem.limit_in_bytes",
                        "cpu.cfs_quota_us", "cpuacct.stat.system", "memory.stat.total_active_anon",
                        "memory.max_usage_in_bytes", "memory.stat.total_active_file", "memory.stat.total_mapped_file",
                        "cpu.cfs_period_us", "memory.stat.pgfault", "memory.stat.total_pgpgin",
                        "memory.stat.total_inactive_anon", "preprocessing_time"}
galaxy_string_metrics = {"cpuacct.usage_percpu"}
condor_float_metrics = {"NumRestarts", "NumJobRestarts", "JobStatus"}
condor_string_metrics = {"LastRemoteHost", "GlobalJobId", "Cmd"}
condor_time_metrics = {"JobStartDate", "JobCurrentStartDate", "CompletionDate"}


def parse_galaxy_job_metrics(job_metrics: List) -> Dict[str, Dict]:
    """
    Parses the more or less "raw" metrics from Galaxy, so they can later be ingested by InfluxDB.
    """
    parsed_metrics = {
        "staging_time": {
            "name": "staging_time",
            "type": "float",
            "value": float(0)
        },
    }

    jobstatus_queued = jobstatus_running = None

    for metric in job_metrics:
        if metric["name"] in galaxy_float_metrics:
            parsed_metrics[metric["name"]] = {
                "name": metric["name"],
                "type": "float",
                "plugin": metric["plugin"],
                "value": float(metric["raw_value"])
            }
        if metric["name"] in galaxy_string_metrics:
            parsed_metrics[metric["name"]] = {
                "name": metric["name"],
                "type": "string",
                "plugin": metric["plugin"],
                "value": metric["raw_value"]
            }
        # For calculating the staging time (if the metrics exist)
        if metric["plugin"] == "jobstatus" and metric["name"] == "queued":
            jobstatus_queued = datetime.strptime(metric["value"], "%Y-%m-%d %H:%M:%S.%f")
        if metric["plugin"] == "jobstatus" and metric["name"] == "running":
            jobstatus_running = datetime.strptime(metric["value"], "%Y-%m-%d %H:%M:%S.%f")

    # Calculate staging time
    if jobstatus_queued is not None and jobstatus_running is not None:
        parsed_metrics["staging_time"]["value"] = float((jobstatus_running - jobstatus_queued).seconds +
                                                        (jobstatus_running - jobstatus_queued).microseconds * 0.000001)

    return parsed_metrics


def parse_condor_job_metrics(job_metrics: Dict) -> Dict[str, Dict]:
    parsed_metrics = {}

    for key, value in job_metrics.items():
        if key in condor_float_metrics:
            parsed_metrics[key] = {
                "name": key,
                "type": "float",
                "plugin": "condor_history",
                "value": float(value)
            }
        if key in condor_string_metrics:
            parsed_metrics[key] = {
                "name": key,
                "type": "string",
                "plugin": "condor_history",
                "value": value
            }
        if key in condor_time_metrics:
            parsed_metrics[key] = {
                "name": key,
                "type": "timestamp",
                "plugin": "condor_history",
                "value": value * 1000
            }
        if key == "JobStatus":
            if value == 1:
                status = "idle"
            elif value == 2:
                status = "running"
            elif value == 3:
                status = "removed"
            elif value == 4:
                status = "success"
            elif value == 5:
                status = "held"
            elif value == 6:
                status = "transferring output"
            else:
                status = "unknown"
            parsed_metrics["job_status"] = {
                "name": "job_status",
                "type": "string",
                "plugin": "condor_history",
                "value": status
            }
        if key == "RemoteWallClockTime":
            parsed_metrics["runtime_seconds"] = {
                "name": "runtime_seconds",
                "type": "float",
                "plugin": "condor_history",
                "value": float(value)
            }

    return parsed_metrics
