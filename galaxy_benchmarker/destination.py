"""
Definition of different destination-types for workflows.
"""
import ansible_bridge
from typing import Dict
from task import BaseTask, AnsiblePlaybookTask
from galaxy_bridge import Galaxy
from jinja2 import Template

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
        self._create_galaxy_destination_user(glx)

    def _create_galaxy_destination_user(self, glx):
        self.galaxy_user_name = str.lower("dest_user_" + self.name)
        self.galaxy_user_id, self.galaxy_user_key = glx.create_user(self.galaxy_user_name)

    def _run_ansible_playbook_task(self, task: AnsiblePlaybookTask):
        ansible_bridge.run_playbook(task.playbook, self.host, self.host_user, self.ssh_key,
                                    {"tool_dependency_dir": self.tool_dependency_dir})


class CondorDestination(BaseDestination):
    def __init__(self, name):
        super().__init__(name)


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
        destination = CondorDestination(dest_config["name"])

    if dest_config["type"] == "GalaxyCondor":
        destination = GalaxyCondorDestination(dest_config["name"])

    return destination


def create_galaxy_job_conf(glx: Galaxy, destinations: Dict[str, BaseDestination]):
    with open('job_conf.xml') as file_:
        template = Template(file_.read())
    job_conf = template.render(galaxy=glx, destinations=destinations.values())
    with open("job_conf.xml.tmp", "w") as fh:
        fh.write(job_conf)
