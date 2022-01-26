"""Utility for parsing fio results"""

import json
from pathlib import Path


def parse_result_file(directory: str, filename: str, jobname: str) -> dict:
    file = Path(directory) / filename
    if not file.is_file():
        raise ValueError(f"{file} is not a fio result file.")

    with file.open() as file_handle:
        result_json = json.load(file_handle)

    jobs = [job for job in result_json["jobs"] if job["jobname"] == jobname]

    if len(jobs) != 1:
        raise ValueError(f"Job '{jobname}' is missing in result {file}")

    read = result_json["jobs"][0]["read"]
    result = {
        **_parse_job_result(read, "read")
    }

    return result


def _parse_job_result(result, prefix):
    return {
        f"{prefix}_bw_min_in_mb" : result["bw_min"] / 1024,
        f"{prefix}_bw_max_in_mb" : result["bw_max"] / 1024,
        f"{prefix}_bw_mean_in_mb" : result["bw_mean"] / 1024,
        f"{prefix}_iops_min" : result["iops_min"],
        f"{prefix}_iops_max" : result["iops_max"],
        f"{prefix}_iops_mean" : result["iops_mean"],
        f"{prefix}_iops_stddev" : result["iops_stddev"],
        f"{prefix}_lat_min_in_ms" : result["lat_ns"]["min"] / 1_000_000,
        f"{prefix}_lat_max_in_ms" : result["lat_ns"]["max"] / 1_000_000,
        f"{prefix}_lat_mean_in_ms" : result["lat_ns"]["mean"] / 1_000_000,
        f"{prefix}_lat_stddev_in_ms" : result["lat_ns"]["stddev"] / 1_000_000,
    }
