from influxdb import InfluxDBClient
from typing import Dict
from datetime import datetime


class InfluxDB:
    def __init__(self, host, port, username, password, db_name):
        self.client = InfluxDBClient(host=host, port=port, username=username, password=password,
                                     ssl=False, database=db_name)

    def save_job_metrics(self, tags: Dict, job_results: Dict):
        json_points = []
        for metric in job_results["parsed_job_metrics"].values():
            metric_tags = tags.copy()
            metric_tags["job_id"] = job_results["id"]
            json_points.append({
                "measurement": metric["name"],
                "tags": metric_tags,
                "time": job_results["update_time"],
                "fields": {
                    "value": metric["value"]
                }
            })

        self.client.write_points(json_points)

    def save_workflow_status(self, tags: Dict, status: str):
        self.client.write_points([{
            "measurement": "workflow_status",
            "tags": tags,
            "time": str(datetime.now()),
            "fields": {
                "value": status
            }
        }])
