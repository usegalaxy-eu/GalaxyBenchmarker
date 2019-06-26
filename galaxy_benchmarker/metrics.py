from typing import List, Dict
from datetime import datetime


float_metrics = {"processor_count", "memtotal", "swaptotal", "runtime_seconds", "memory.stat.pgmajfault",
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
                 "memory.stat.total_inactive_anon"}


def parse_galaxy_job_metrics(job_metrics: List) -> Dict[str, Dict]:
    parsed_metrics = {
        "staging_time": {
            "name": "staging_time",
            "type": "float",
            "value": float(0)
        },
    }

    jobstatus_queued = jobstatus_running = None

    for metric in job_metrics:
        if metric["name"] in float_metrics:
            parsed_metrics[metric["name"]] = {
                "name": metric["name"],
                "type": "float",
                "plugin": metric["plugin"],
                "value": float(metric["raw_value"])
            }
        if metric["plugin"] == "jobstatus" and metric["name"] == "queued":
            jobstatus_queued = datetime.strptime(metric["value"], "%Y-%m-%d %H:%M:%S.%f")
        if metric["plugin"] == "jobstatus" and metric["name"] == "running":
            jobstatus_running = datetime.strptime(metric["value"], "%Y-%m-%d %H:%M:%S.%f")

    # Calculate staging time
    if jobstatus_queued is not None and jobstatus_running is not None:
        parsed_metrics["staging_time"]["value"] = float((jobstatus_running - jobstatus_queued).seconds +
                                                        (jobstatus_running - jobstatus_queued).microseconds * 0.000001)

    return parsed_metrics
