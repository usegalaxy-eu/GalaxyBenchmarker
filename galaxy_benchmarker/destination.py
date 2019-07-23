"""
Definition of different destination-types for workflows.
"""
from __future__ import annotations
import ansible_bridge
import planemo_bridge
import condor_bridge
import metrics
import logging
import time
from multiprocessing import Pool, TimeoutError
from typing import Dict
from task import BaseTask, AnsiblePlaybookTask, BenchmarkerTask
from galaxy_bridge import Galaxy
from bioblend.galaxy import GalaxyInstance
from jinja2 import Template
# from workflow import GalaxyWorkflow, CondorWorkflow

log = logging.getLogger("GalaxyBenchmarker")


class BaseDestination:
    def __init__(self, name):
        self.name = name

    def run_workflow(self, workflow: BaseWorkflow) -> Dict:
        """
        Runs the given workflow on Destination. Should return a Dict describing the result.
        Needs to be implemented by child.
        """
        raise NotImplementedError

    def run_task(self, task: BaseTask):
        """
        Runs a given task on the Destination.
        """
        if task is None:
            return
        if type(task) is AnsiblePlaybookTask:
            self.run_ansible_playbook_task(task)

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
        ansible_bridge.run_playbook(task.playbook, self.host, self.host_user, self.ssh_key,
                                    {"tool_dependency_dir": self.tool_dependency_dir})

    def get_jobs(self, history_name) -> Dict:
        """
        Get all jobs together with their details from a given history_name.
        """
        glx_instance = self.galaxy.impersonate(user_key=self.galaxy_user_key)
        job_ids = get_job_ids_from_history_name(history_name, glx_instance)

        infos = dict()
        for job_id in job_ids:
            infos[job_id] = self.galaxy.instance.jobs.show_job(job_id, full_details=True)
            # Parse JobMetrics for future usage in influxDB
            infos[job_id]["parsed_job_metrics"] = metrics.parse_galaxy_job_metrics(infos[job_id]["job_metrics"])

        return infos

    def run_workflow(self, workflow: GalaxyWorkflow) -> Dict:
        """
        Runs the given workflow on PulsarMQDestination. Returns Dict of the status and
        history_name of the finished workflow.
        """
        log.info("Running workflow '{wf_name}' using Planemo".format(wf_name=workflow.name))

        start_time = time.monotonic()

        if workflow.timeout is None:
            result = planemo_bridge.run_planemo(self.galaxy, self, workflow.path)
        else:
            # Run inside a Process to enable timeout
            pool = Pool(processes=1)
            pool_result = pool.apply_async(planemo_bridge.run_planemo, (self.galaxy, self, workflow.path))

            try:
                result = pool_result.get(timeout=workflow.timeout)
            except TimeoutError:
                log.info("Timeout after {timeout} seconds".format(timeout=workflow.timeout))
                result = {"status": "error"}

        result["total_workflow_runtime"] = time.monotonic() - start_time

        return result


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
        ansible_bridge.run_playbook("deploy_condor_workflow.yml", self.host, self.host_user, self.ssh_key, values)

    def run_workflow(self, workflow: CondorWorkflow) -> Dict:
        """
        Runs the given workflow on PulsarMQDestination. Returns Dict of ...
        """
        ssh_client = condor_bridge.get_paramiko_client(self.host, self.host_user, self.ssh_key)

        remote_workflow_dir = "{jobs_dir}/{wf_name}".format(jobs_dir=self.jobs_directory_dir,
                                                            wf_name=workflow.name)

        log.info("Submitting workflow '{wf}' to '{dest}'".format(wf=workflow, dest=self))
        start_time = time.monotonic()
        job_ids = condor_bridge.submit_job(ssh_client, remote_workflow_dir, workflow.job_file)
        submit_time = time.monotonic() - start_time
        log.info("Submitted in {seconds} seconds".format(seconds=submit_time))

        # Check every 0.1s if status has changed
        status = "unknown"
        while status != "done":
            try:
                job_status = condor_bridge.get_job_status(ssh_client, job_ids["id"])
            except ValueError as error:
                status = "error"
                log.error("There was an error with run of {workflow}: {error}".format(workflow=self.name,
                                                                                      error=error))
                break
            status = job_status["status"]
            time.sleep(self.status_refresh_time)

        total_workflow_runtime = time.monotonic() - start_time

        log.info("Fetching condor_history")
        jobs = condor_bridge.get_condor_history(ssh_client, float(job_ids["id"]), float(job_ids["id"]))

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


def get_job_ids_from_history_name(history_name, impersonated_instance: GalaxyInstance):
    """
    For a given history_name return all its associated job-ids. As a history is only accessible from the user it
    was created by, an impersonated_instance is needed.
    """
    histories = impersonated_instance.histories.get_histories(name=history_name)

    if len(histories) >= 1:
        history_id = histories[0]["id"]
        dataset_ids = impersonated_instance.histories.show_history(history_id)["state_ids"]["ok"]
        job_ids = list()

        for dataset_id in dataset_ids:
            job_ids.append(impersonated_instance.histories.show_dataset(history_id, dataset_id)["creating_job"])

        return job_ids

    return []

