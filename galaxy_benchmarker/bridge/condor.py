import json
import logging
import re
from datetime import datetime
from typing import Dict

import paramiko

from galaxy_benchmarker import metrics

log = logging.getLogger(__name__)


def get_paramiko_client(host, username, key_file):
    key = paramiko.RSAKey.from_private_key_file(key_file)

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(host, username=username, pkey=key)
    return client


def submit_job(client: paramiko.SSHClient, workflow_dir, job_file):
    """
    Submits Condor-Job and returns the ID and a (start, end) of the sub-id range as a Dict.
    """
    stdin, stdout, stderr = client.exec_command(
        "cd {wf_dir}; condor_submit {job} -terse".format(
            wf_dir=workflow_dir, job=job_file
        )
    )

    error = ""
    for err in stderr:
        error += err

    if error != "":
        raise ValueError(
            "An error with condor_submit occurred: {error}".format(error=error)
        )

    for output in stdout:
        if output.find("ERROR") != -1:
            raise Exception(output)
        job_id = output.split(".")[0]
        id_range = tuple(output.replace("\n", "").split(" - "))

    return {"id": job_id, "range": id_range}


def get_job_status(client: paramiko.SSHClient, job_id):
    """
    Determines, job-status. If all jobs were run (status="done"), total_jobs and everything else will be 0
    """
    stdin, stdout, stderr = client.exec_command(
        "condor_q {job_id}".format(job_id=job_id)
    )

    status = None
    for output in stdout:
        if output.find("jobs;") != -1:
            status = list(map(int, re.findall(r"\d+", output)))

    if status is None or len(status) != 7:
        raise Exception("Couldn't parse condor_q")

    return {
        "status": "done"
        if status[0] == 0 or (status[3] == 0 and status[4] == 0)
        else "running",
        "total_jobs": status[0],
        "completed": status[1],
        "idle": status[3],
        "running": status[4],
        "held": status[5],
    }


def get_condor_history(
    client: paramiko.SSHClient, first_id: float, last_id: float = float("inf")
) -> Dict[str, Dict]:
    """
    Returns condor_history as a list of jobs. Returns all job_ids >= first_id
    """
    output_filename = "condor_history_" + str(datetime.now().timestamp()) + ".json"
    stdin, stdout, stderr = client.exec_command(
        "condor_history -backwards -json -since {i} > {filename}".format(
            i=int(first_id) - 1, filename=output_filename
        )
    )

    error = ""
    for err in stderr:
        error += err

    if error != "":
        raise ValueError(
            "An error with condor_history occurred: {error}".format(error=error)
        )

    # Get output file
    ftp_client = client.open_sftp()
    ftp_client.get(output_filename, "results/" + output_filename)
    ftp_client.close()
    with open("results/" + output_filename) as json_file:
        job_list = json.load(json_file)

    result = {}
    for job in job_list:
        job["parsed_job_metrics"] = parse_condor_job_metrics(job)
        job["id"] = job["GlobalJobId"]
        result[job["id"]] = job

    return result


def parse_condor_job_metrics(job_metrics: Dict) -> Dict[str, Dict]:
    parsed_metrics = {}

    for key, value in job_metrics.items():
        try:
            if key in metrics.condor_float_metrics:
                parsed_metrics[key] = {
                    "name": key,
                    "type": "float",
                    "plugin": "condor_history",
                    "value": float(value),
                }
            if key in metrics.condor_string_metrics:
                parsed_metrics[key] = {
                    "name": key,
                    "type": "string",
                    "plugin": "condor_history",
                    "value": value,
                }
            if key in metrics.condor_time_metrics:
                parsed_metrics[key] = {
                    "name": key,
                    "type": "timestamp",
                    "plugin": "condor_history",
                    "value": value * 1000,
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
                    "value": status,
                }
            if key == "RemoteWallClockTime":
                parsed_metrics["runtime_seconds"] = {
                    "name": "runtime_seconds",
                    "type": "float",
                    "plugin": "condor_history",
                    "value": float(value),
                }
        except ValueError as e:
            log.error(
                "Error while trying to parse Condor job metrics '{key} = {value}': {error}. Ignoring..".format(
                    error=e, key=key, value=value
                )
            )

    return parsed_metrics
