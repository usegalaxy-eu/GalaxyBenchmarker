import paramiko
import re
from typing import List, Dict
from datetime import datetime


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
    stdin, stdout, stderr = client.exec_command("cd {wf_dir}; condor_submit {job} -terse".format(wf_dir=workflow_dir,
                                                                                                 job=job_file))

    error = ""
    for err in stderr:
        error += err

    if error != "":
        raise ValueError("An error with condor_submit occurred: {error}".format(error=error))

    for output in stdout:
        if output.find("ERROR") != -1:
            raise Exception(output)
        job_id = output.split(".")[0]
        id_range = tuple(output.replace("\n", "").split(" - "))

    return {
        "id": job_id,
        "range": id_range
    }


def get_job_status(client: paramiko.SSHClient, job_id):
    """
    Determines, job-status. If all jobs were run (status="done"), total_jobs and everything else will be 0
    """
    stdin, stdout, stderr = client.exec_command("condor_q {job_id}".format(job_id=job_id))

    status = None
    for output in stdout:
        if output.find("jobs;") != -1:
            status = list(map(int, re.findall(r'\d+', output)))

    if status is None or len(status) != 7:
        raise Exception("Couldn't parse condor_q")

    return {
        "status": "done" if status[0] == 0 or (status[3] == 0 and status[4] == 0) else "running",
        "total_jobs": status[0],
        "completed": status[1],
        "idle": status[3],
        "running": status[4],
        "held": status[5]
    }


def get_condor_history(client: paramiko.SSHClient, first_id: float, last_id: float = float("inf")) -> Dict[str, Dict]:
    """
    Returns condor_history as a list of jobs. Returns all job_ids >= first_id
    """
    stdin, stdout, stderr = client.exec_command("condor_history -backwards -since {i}".format(i=int(first_id)-1))

    error = ""
    for err in stderr:
        error += err

    if error != "":
        raise ValueError("An error with condor_history occurred: {error}".format(error=error))

    result = dict()
    first = True
    for output in stdout:
        # Check for right output and ignore header
        if first:
            if output.find("OWNER") == -1:
                raise Exception("Unexpected output of 'condor_history': {output}".format(output=output))
            first = False
            continue

        values = output.split()

        # Make sure, that job_id is in boundaries of first/last id. As condor_history is not ordered by id, we need
        # to check each record
        if int(float(values[0])) > first_id or int(float(values[0])) < last_id:
            continue

        try:
            run_time = datetime.strptime(values[4], "0+%H:%M:%S")
        except ValueError:
            run_time = datetime.strptime("0+00:00:00", "0+%H:%M:%S")

        result[values[0]] = {
            "id": values[0],
            "owner": values[1],
            "submitted": values[2] + " " + values[3],
            "run_time": (run_time.hour * 3600 + run_time.minute * 60 + run_time.second),  # RemoteWallClockTime
            "st": values[5], # JobStatus
            "completed": values[6] + " " + values[7],  # CompletionDate
            "cmd": values[8],
            "parsed_job_metrics": {
                "runtime_seconds": {
                    "name": "runtime_seconds",
                    "type": "float",
                    "plugin": "benchmarker",
                    "value": float(run_time.hour * 3600 + run_time.minute * 60 + run_time.second)
                },
                "status": {
                    "name": "job_status",
                    "type": "string",
                    "plugin": "benchmarker",
                    "value": "success" if values[5] == "C" else "error"
                }
            }
        }

    return result
