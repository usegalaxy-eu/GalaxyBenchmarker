from typing import List, Dict
from datetime import datetime

# class BaseMetric:
#     value = ""
#
#     def __init__(self, name, regex):
#         self.name = name
#         self.regex = regex
#
#     def parse(self, raw_value):
#         self.value = re.match(self.regex, raw_value)
#
#
# class NumberMetric(BaseMetric):
#     value: int
#
#     def __init__(self, name, regex):
#         super().__init__(name, regex)
#
#
# metrics = {
#     "memory.numa_stat.total"
# }


def parse_galaxy_job_metrics(job_metrics: List) -> Dict[str, Dict]:
    parsed_metrics = {
        "cpuacct.usage": {
            "name": "cpuacct.usage",
            "type": "float",
            "value": float(0)
        },
        "staging_time": {
            "name": "staging_time",
            "type": "float",
            "value": float(0)
        },
        "runtime_seconds": {
            "name": "runtime_seconds",
            "type": "float",
            "value": float(0)
        }
    }

    for metric in job_metrics:
        if metric["name"] == "cpuacct.usage":
            parsed_metrics["cpuacct.usage"]["value"] = float(metric["raw_value"]) / 1000000000  # Convert to seconds
        if metric["name"] == "runtime_seconds":
            parsed_metrics["runtime_seconds"]["value"] = float(metric["raw_value"])
        if metric["plugin"] == "jobstatus" and metric["name"] == "queued":
            jobstatus_queued = datetime.strptime(metric["value"], "%Y-%m-%d %H:%M:%S.%f")
        if metric["plugin"] == "jobstatus" and metric["name"] == "running":
            jobstatus_running = datetime.strptime(metric["value"], "%Y-%m-%d %H:%M:%S.%f")

    # Calculate staging time
    parsed_metrics["staging_time"]["value"] = float((jobstatus_running - jobstatus_queued).seconds +
                                                    (jobstatus_running - jobstatus_queued).microseconds * 0.000001)

    return parsed_metrics
