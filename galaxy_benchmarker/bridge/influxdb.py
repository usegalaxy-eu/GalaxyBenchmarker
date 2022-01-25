from influxdb import InfluxDBClient
from typing import Dict
from dataclasses import dataclass
from serde import serde

@serde
@dataclass
class InfluxDbConfig:
    host: str
    port: int
    username: str
    password: str
    db_name: str

class InfluxDb:
    def __init__(self, config: InfluxDbConfig):
        self.client = InfluxDBClient(
            host=config.host,
            port=config.port,
            username=config.username,
            password=config.password,
            database=config.db_name,
            ssl=False,
            retries=20)

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

    def save_metric(self, tags, metric_name, values):
        datapoints = []

        for value in values:
            datapoints.append({
                "measurement": metric_name,
                "tags": tags,
                "fields": {
                    "value": value
                }
            })
            
        self.client.write_points(datapoints)

