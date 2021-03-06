from influxdb import InfluxDBClient
from typing import Dict


class InfluxDB:
    def __init__(self, host, port, username, password, db_name):
        self.client = InfluxDBClient(host=host, port=port, username=username, password=password,
                                     ssl=False, database=db_name, retries=20)

    def save_job_metrics(self, tags: Dict, job_results: Dict):
        """
        Saves the parsed job-specific metrics (see metrics.py) to InfluxDB.
        """
        if "parsed_job_metrics" not in job_results:
            return []

        json_points = []

        for metric in job_results["parsed_job_metrics"].values():
            metric_tags = tags.copy()

            if "job_id" in job_results:
                metric_tags["job_id"] = job_results["id"]
            if "tool_id" in job_results:
                metric_tags["tool_id"] = job_results["tool_id"]

            if "plugin" in metric:
                metric_tags["plugin"] = metric["plugin"]

            json_points.append({
                "measurement": metric["name"],
                "tags": metric_tags,
                "fields": {
                    "value": metric["value"]
                }
            })

        self.client.write_points(json_points)

    def save_workflow_metrics(self, tags: Dict, metrics: Dict):
        """
        Saves the workflow-specific metrics to InfluxDB.
        """
        json_points = []

        for metric in metrics.values():
            metric_tags = tags.copy()

            if "plugin" in metric:
                metric_tags["plugin"] = metric["plugin"]

            json_points.append({
                "measurement": metric["name"],
                "tags": metric_tags,
                "fields": {
                    "value": metric["value"]
                }
            })

        self.client.write_points(json_points)
