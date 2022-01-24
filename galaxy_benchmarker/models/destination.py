"""
Definition of different destination-types for workflows.
"""
from __future__ import annotations
from galaxy_benchmarker.bridge import ansible,planemo, condor
from galaxy_benchmarker import metrics
import logging
import time
from multiprocessing import Pool, TimeoutError
from typing import Dict, List
from galaxy_benchmarker.models.task import BaseTask, AnsiblePlaybookTask, BenchmarkerTask
from galaxy_benchmarker.bridge.galaxy import Galaxy
from jinja2 import Template
# from workflow import GalaxyWorkflow, CondorWorkflow
from datetime import datetime

log = logging.getLogger(__name__)


class BaseDestination:
    def __init__(self, name):
        self.name = name

    def run_workflow(self, workflow: BaseWorkflow) -> Dict:
        """
        Runs the given workflow on Destination. Should return a Dict describing the result.
        Needs to be implemented by child.
        """
        raise NotImplementedError

    def run_ansible_playbook_task(self, task: AnsiblePlaybookTask):
        raise NotImplementedError

    def __str__(self):
        return self.name

    __repr__ = __str__


class GalaxyDestination(BaseDestination):
    host = ""
    host_user = ""
    ssh_key = ""
    tool_dependency_dir = "/data/share/tools"
    jobs_directory_dir = "/data/share/staging"
    persistence_dir = "/data/share/persisted_data"
    galaxy_user_name = ""
    galaxy_user_key = ""

    def __init__(self, name, glx: Galaxy, galaxy_user_name=None, galaxy_user_key=None):
        super().__init__(name)
        self.galaxy = glx
        self.galaxy_user_name = galaxy_user_name
        self.galaxy_user_key = galaxy_user_key

        if self.galaxy_user_name is None or self.galaxy_user_key is None:
            self._create_galaxy_destination_user(glx)

    def _create_galaxy_destination_user(self, glx):
        """
        Creates a user specifically for this Destination-Instance. This one is later used for routing the jobs
        to the right Pulsar-Server.
        """
        self.galaxy_user_name = str.lower("dest_user_" + self.name)
        _, self.galaxy_user_key = glx.create_user(self.galaxy_user_name)

    def run_ansible_playbook_task(self, task: AnsiblePlaybookTask):
        """
        Runs the given AnsiblePlaybookTask on the destination.
        """
        ansible.run_playbook(task.playbook, self.host, self.host_user, self.ssh_key,
                                    {"tool_dependency_dir": self.tool_dependency_dir,
                                     "jobs_directory_dir": self.jobs_directory_dir,
                                     "persistence_dir": self.persistence_dir})

    def get_jobs(self, history_name) -> Dict:
        """
        Get all jobs together with their details from a given history_name.
        """
        job_ids = self.glx.get_job_ids_from_history_name(history_name, self.galaxy_user_key)

        infos = dict()
        for job_id in job_ids:
            infos[job_id] = self.galaxy.instance.jobs.show_job(job_id, full_details=True)

            # Get JobMetrics and parse them for future usage in influxDB
            infos[job_id]["job_metrics"] = self.galaxy.instance.jobs.get_metrics(job_id)
            infos[job_id]["parsed_job_metrics"] = self.parse_galaxy_job_metrics(infos[job_id]["job_metrics"])

        return infos

    def run_workflow(self, workflow: GalaxyWorkflow) -> Dict:
        """
        Runs the given workflow on PulsarMQDestination. Returns Dict of the status and
        history_name of the finished workflow.
        """
        log.info("Running workflow '{wf_name}' using Planemo".format(wf_name=workflow.name))

        start_time = time.monotonic()

        if workflow.timeout is None:
            result = planemo.run_planemo(self.galaxy, self, workflow.path)
        else:
            # Run inside a Process to enable timeout
            pool = Pool(processes=1)
            pool_result = pool.apply_async(planemo.run_planemo, (self.galaxy, self, workflow.path))

            try:
                result = pool_result.get(timeout=workflow.timeout)
            except TimeoutError:
                log.info("Timeout after {timeout} seconds".format(timeout=workflow.timeout))
                result = {"status": "error"}

        result["total_workflow_runtime"] = time.monotonic() - start_time

        return result

    @staticmethod
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
            try:
                if metric["name"] in metrics.galaxy_float_metrics:
                    parsed_metrics[metric["name"]] = {
                        "name": metric["name"],
                        "type": "float",
                        "plugin": metric["plugin"],
                        "value": float(metric["raw_value"])
                    }
                if metric["name"] in metrics.galaxy_string_metrics:
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
            except ValueError as e:
                log.error("Error while trying to parse Galaxy job metrics '{name} = {value}': {error}. Ignoring.."
                        .format(error=e, name=metric["name"], value=metric["raw_value"]))

        # Calculate staging time
        if jobstatus_queued is not None and jobstatus_running is not None:
            parsed_metrics["staging_time"]["value"] = float((jobstatus_running - jobstatus_queued).seconds +
                                                            (jobstatus_running - jobstatus_queued).microseconds * 0.000001)

        return parsed_metrics



class PulsarMQDestination(GalaxyDestination):
    def __init__(self, name, glx: Galaxy, job_plugin_params: Dict, job_destination_params: Dict, amqp_url="", galaxy_user_name="", galaxy_user_key=""):
        self.amqp_url = amqp_url
        self.job_plugin_params = job_plugin_params
        self.job_destination_params = job_destination_params
        super().__init__(name, glx, galaxy_user_name, galaxy_user_key)


class GalaxyCondorDestination(GalaxyDestination):
    def __init__(self, name, glx: Galaxy, job_plugin_params: Dict, job_destination_params: Dict, galaxy_user_name="", galaxy_user_key=""):
        self.job_plugin_params = job_plugin_params
        self.job_destination_params = job_destination_params
        super().__init__(name, glx, galaxy_user_name, galaxy_user_key)


class CondorDestination(BaseDestination):
    status_refresh_time = 0.5 # TODO: Figure out, if that timing is to fast

    def __init__(self, name, host, host_user, ssh_key, jobs_directory_dir):
        super().__init__(name)
        self.host = host
        self.host_user = host_user
        self.ssh_key = ssh_key
        self.jobs_directory_dir = jobs_directory_dir

    def deploy_workflow(self, workflow: CondorWorkflow):
        """
        Deploys the given workflow to the Condor-Server with Ansible.
        """
        log.info("Deploying {workflow} to {destination}".format(workflow=workflow.name, destination=self.name))
        # Use ansible-playbook to upload *.job-file to Condor-Manager
        values = {
            "jobs_directory_dir": self.jobs_directory_dir,
            "workflow_name": workflow.name,
            "workflow_directory_path": workflow.path,
            "condor_user": self.host_user
        }
        ansible.run_playbook("deploy_condor_workflow.yml", self.host, self.host_user, self.ssh_key, values)

    def run_workflow(self, workflow: CondorWorkflow) -> Dict:
        """
        Runs the given workflow on CondorDestination. Returns Dict of ...
        """
        ssh_client = condor.get_paramiko_client(self.host, self.host_user, self.ssh_key)

        remote_workflow_dir = "{jobs_dir}/{wf_name}".format(jobs_dir=self.jobs_directory_dir,
                                                            wf_name=workflow.name)

        log.info("Submitting workflow '{wf}' to '{dest}'".format(wf=workflow, dest=self))
        start_time = time.monotonic()
        job_ids = condor.submit_job(ssh_client, remote_workflow_dir, workflow.job_file)
        submit_time = time.monotonic() - start_time
        log.info("Submitted in {seconds} seconds".format(seconds=submit_time))

        # Check every 0.1s if status has changed
        status = "unknown"
        while status != "done":
            try:
                job_status = condor.get_job_status(ssh_client, job_ids["id"])
            except ValueError as error:
                status = "error"
                log.error("There was an error with run of {workflow}: {error}".format(workflow=self.name,
                                                                                      error=error))
                break
            status = job_status["status"]
            time.sleep(self.status_refresh_time)

        total_workflow_runtime = time.monotonic() - start_time

        log.info("Fetching condor_history")
        jobs = condor.get_condor_history(ssh_client, float(job_ids["id"]), float(job_ids["id"]))

        result = {
            "id": job_ids["id"],
            "id_range": job_ids["range"],
            "status": "success" if status == "done" else "error",
            "total_workflow_runtime": total_workflow_runtime,
            "submit_time": submit_time,
            "jobs": jobs
        }

        ssh_client.close()

        return result


def configure_destination(dest_config, glx):
    """
    Initializes and configures a Destination according to the given configuration. Returns the configured Destination.
    """
    # Check, if all set properly
    if "name" not in dest_config:
        raise ValueError("No Destination-Name set! Config: '{config}'".format(config=dest_config))
    if "type" not in dest_config:
        raise ValueError("No Destination-Type set for '{dest}'".format(dest=dest_config["name"]))
    if dest_config["type"] not in ["Galaxy", "PulsarMQ", "Condor", "GalaxyCondor"]:
        raise ValueError("Destination-Type '{type}' not valid".format(type=dest_config["type"]))

    job_plugin_params = dict() if "job_plugin_params" not in dest_config else dest_config["job_plugin_params"]
    job_destination_params = dict() if "job_destination_params" not in dest_config else dest_config["job_destination_params"]

    galaxy_user_name = None if "galaxy_user_name" not in dest_config else dest_config["galaxy_user_name"]
    galaxy_user_key = None if "galaxy_user_key" not in dest_config else dest_config["galaxy_user_key"]

    if dest_config["type"] == "Galaxy":
        destination = GalaxyDestination(dest_config["name"], glx, galaxy_user_name, galaxy_user_key)

    if dest_config["type"] == "PulsarMQ":
        destination = PulsarMQDestination(dest_config["name"], glx, job_plugin_params, job_destination_params,
                                          dest_config["amqp_url"],
                                          galaxy_user_name, galaxy_user_key)
        if "host" in dest_config:
            destination.host = dest_config["host"]
            destination.host_user = dest_config["host_user"]
            destination.ssh_key = dest_config["ssh_key"]
            destination.tool_dependency_dir = dest_config["tool_dependency_dir"]

    if dest_config["type"] == "Condor":
        destination = CondorDestination(dest_config["name"], dest_config["host"], dest_config["host_user"],
                                        dest_config["ssh_key"], dest_config["jobs_directory_dir"])
        if "status_refresh_time" in dest_config:
            destination.status_refresh_time = dest_config["status_refresh_time"]

    if dest_config["type"] == "GalaxyCondor":
        destination = GalaxyCondorDestination(dest_config["name"], glx, job_plugin_params, job_destination_params,
                                              galaxy_user_name,
                                              galaxy_user_key)

    return destination


def create_galaxy_job_conf(glx: Galaxy, destinations: Dict[str, BaseDestination]):
    """
    Creates the job_conf.xml-file for Galaxy using the given Dict (key: dest_name, value dest) and saves it to
    galaxy_files/job_conf.xml.tmp.
    """
    with open('galaxy_files/job_conf.xml') as file_:
        template = Template(file_.read())

    pulsar_destinations = list()
    galaxy_condor_destinations = list()
    job_plugin_params = dict()
    job_destination_params = dict()

    for dest in destinations.values():
        if type(dest) is PulsarMQDestination:
            pulsar_destinations.append(dest)
        if type(dest) is GalaxyCondorDestination:
            galaxy_condor_destinations.append(dest)
        if issubclass(type(dest), PulsarMQDestination) or issubclass(type(dest), GalaxyCondorDestination):
            job_plugin_params[dest.name] = dest.job_plugin_params
            job_destination_params[dest.name] = dest.job_destination_params

    job_conf = template.render(galaxy=glx,
                               pulsar_destinations=pulsar_destinations,
                               galaxy_condor_destinations=galaxy_condor_destinations,
                               job_plugin_params=job_plugin_params,
                               job_destination_params=job_destination_params)

    with open("galaxy_files/job_conf.xml.tmp", "w") as fh:
        fh.write(job_conf)
