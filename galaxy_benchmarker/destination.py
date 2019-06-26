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
from typing import Dict
from task import BaseTask, AnsiblePlaybookTask
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
            self._run_ansible_playbook_task(task)

    def _run_ansible_playbook_task(self, task: AnsiblePlaybookTask):
        raise NotImplementedError


class PulsarMQDestination(BaseDestination):
    host = ""
    host_user = ""
    ssh_key = ""
    tool_dependency_dir = "/data/share/tools"
    jobs_directory_dir = "/data/share/staging"
    persistence_dir = "/data/share/persisted_data"
    galaxy_user_name = ""
    galaxy_user_id = ""
    galaxy_user_key = ""

    def __init__(self, name, glx: Galaxy, amqp_url):
        super().__init__(name)
        self.amqp_url = amqp_url
        self.galaxy = glx
        self._create_galaxy_destination_user(glx)

    def _create_galaxy_destination_user(self, glx):
        self.galaxy_user_name = str.lower("dest_user_" + self.name)
        self.galaxy_user_id, self.galaxy_user_key = glx.create_user(self.galaxy_user_name)

    def _run_ansible_playbook_task(self, task: AnsiblePlaybookTask):
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
        result = planemo_bridge.run_planemo(self.galaxy, self, workflow.path)
        result["total_workflow_runtime"] = time.monotonic() - start_time

        return result


class CondorDestination(BaseDestination):
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
        log.info("Deploying {workflow} to {destination}".format(workflow=self.name, destination=self.name))
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

        start_time = time.monotonic()
        job_ids = condor_bridge.submit_job(ssh_client, remote_workflow_dir, workflow.job_file)

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
            time.sleep(0.1)  # TODO: Figure out, if that timing is to fast

        total_workflow_runtime = time.monotonic() - start_time

        ssh_client.close()

        result = {
            "status": "success" if status == "done" else "error",
            "total_workflow_runtime": total_workflow_runtime
        }

        return result


class GalaxyCondorDestination(BaseDestination):
    def __init__(self, name):
        super().__init__(name)


def configure_destination(dest_config, glx):
    """
    Initializes and configures a Destination according to the given configuration. Returns the configured Destination.
    """
    # Check, if all set properly
    if "name" not in dest_config:
        raise ValueError("No Destination-Name set! Config: '{config}'".format(config=dest_config))
    if "type" not in dest_config:
        raise ValueError("No Destination-Type set for '{dest}'".format(dest=dest_config["name"]))
    if dest_config["type"] not in ["PulsarMQ", "Condor", "GalaxyCondor"]:
        raise ValueError("Destination-Type '{type}' not valid".format(type=dest_config["type"]))

    if dest_config["type"] == "PulsarMQ":
        destination = PulsarMQDestination(dest_config["name"], glx, dest_config["amqp_url"])
        if "host" in dest_config:
            destination.host = dest_config["host"]
            destination.host_user = dest_config["host_user"]
            destination.ssh_key = dest_config["ssh_key"]
            destination.tool_dependency_dir = dest_config["tool_dependency_dir"]

    if dest_config["type"] == "Condor":
        destination = CondorDestination(dest_config["name"], dest_config["host"], dest_config["host_user"],
                                        dest_config["ssh_key"], dest_config["jobs_directory_dir"])

    if dest_config["type"] == "GalaxyCondor":
        destination = GalaxyCondorDestination(dest_config["name"])

    return destination


def create_galaxy_job_conf(glx: Galaxy, destinations: Dict[str, BaseDestination]):
    """
    Creates the job_conf.xml-file for Galaxy using the given Dict (key: dest_name, value dest) and saves it to
    galaxy_files/job_conf.xml.tmp.
    """
    with open('galaxy_files/job_conf.xml') as file_:
        template = Template(file_.read())

    pulsar_destinations = list()

    for dest in destinations.values():
        if type(dest) is PulsarMQDestination:
            pulsar_destinations.append(dest)

    job_conf = template.render(galaxy=glx, destinations=pulsar_destinations)
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

