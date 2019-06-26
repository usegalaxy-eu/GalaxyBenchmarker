"""
Definition of different destination-types for workflows.
"""
from __future__ import annotations
import ansible_bridge
import planemo_bridge
import metrics
import logging
from typing import Dict
from task import BaseTask, AnsiblePlaybookTask
from galaxy_bridge import Galaxy
from bioblend.galaxy import GalaxyInstance
from jinja2 import Template
from workflow import GalaxyWorkflow, CondorWorkflow

log = logging.getLogger("GalaxyBenchmarker")


class BaseDestination:
    def __init__(self, name):
        self.name = name

    def run_task(self, task: BaseTask):
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
        ansible_bridge.run_playbook(task.playbook, self.host, self.host_user, self.ssh_key,
                                    {"tool_dependency_dir": self.tool_dependency_dir})

    def get_jobs(self, history_name) -> Dict:
        """
        Get all jobs together with their details from a given history_name
        """
        glx_instance = self.galaxy.impersonate(user_key=self.galaxy_user_key)
        job_ids = get_job_ids_from_history_name(history_name, glx_instance)

        infos = dict()
        for job_id in job_ids:
            infos[job_id] = self.galaxy.instance.jobs.show_job(job_id, full_details=True)
            infos[job_id]["parsed_job_metrics"] = metrics.parse_galaxy_job_metrics(infos[job_id]["job_metrics"])

        return infos

    def run_workflow(self, workflow: GalaxyWorkflow):
        log.info("Running workflow '{wf_name}' using Planemo".format(wf_name=self.name))
        return planemo_bridge.run_planemo(self.galaxy, self, workflow.path)


class CondorDestination(BaseDestination):
    def __init__(self, name, host, host_user, ssh_key, jobs_directory_dir):
        super().__init__(name)
        self.host = host
        self.host_user = host_user
        self.ssh_key = ssh_key
        self.jobs_directory_dir = jobs_directory_dir

    def deploy_workflow(self, workflow: CondorWorkflow):
        # Use ansible-playbook to upload *.job-file to Condor-Manager
        values = {
            "jobs_directory_dir": self.jobs_directory_dir,
            "workflow_name": workflow.name,
            "workflow_directory_path": workflow.path,
            "condor_user": self.condor_user
        }
        ansible_bridge.run_playbook("deploy_condor_workflow.yml", self.host, self.host_user, self.ssh_key, values)


class GalaxyCondorDestination(BaseDestination):
    def __init__(self, name):
        super().__init__(name)


def configure_destination(dest_config, glx):
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
    histories = impersonated_instance.histories.get_histories(name=history_name)
    if len(histories) >= 1:
        history_id = histories[0]["id"]
        dataset_ids = impersonated_instance.histories.show_history(history_id)["state_ids"]["ok"]
        job_ids = list()
        for dataset_id in dataset_ids:
            job_ids.append(impersonated_instance.histories.show_dataset(history_id, dataset_id)["creating_job"])

        return job_ids

    return []

