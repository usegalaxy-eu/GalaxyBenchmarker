from __future__ import annotations
import abc

from random import randrange
from typing import Dict, List

# from galaxy_benchmarker.models.destination import BaseDestination
# from galaxy_benchmarker.models.benchmark import BaseBenchmark
from galaxy_benchmarker.bridge.openstack import OpenStackCompute


def resolve_config(taskname, global_config):
    tasks_config = global_config.get("tasks", {})

    task_config = tasks_config.get(taskname, {})
    if not task_config:
        raise ValueError(f"No definition found for task '{taskname}'")
    return task_config


class BaseTask(abc.ABC):
    def __init__(self, destinations: List[BaseDestination]):
        self.destinations = destinations

    @abc.abstractmethod
    def run(self):
        raise NotImplementedError


class AnsiblePlaybookTask(BaseTask):
    def __init__(self, destinations: List[BaseDestination], playbook: str):
        super().__init__(destinations)
        self.playbook = playbook

    def run(self):
        for destination in self.destinations:
            destination.run_ansible_playbook_task(self)

    def __str__(self):
        return f"Ansible Playbook({self.playbook})"

    __repr__ = __str__


class BenchmarkerTask(BaseTask):
    def __init__(self, destinations: List[BaseDestination], openstack: OpenStackCompute, name, params=dict()):
        super().__init__(destinations)
        self.openstack = openstack
        self.name = name
        self.params = params

    def run(self):
        if self.name == "delete_old_histories":
            for destination in self.destinations:
                self._delete_old_histories(destination)
        elif self.name == "reboot_openstack_servers":
            self._reboot_openstack_servers()
        elif self.name == "reboot_random_openstack_server":
            self._reboot_random_openstack_server()
        elif self.name == "rebuild_random_openstack_server":
            self._rebuild_random_openstack_server()
        else:
            raise ValueError("{name} is not a valid BenchmarkerTask!".format(name=self.name))

    def _delete_old_histories(self, destination):
        destination.galaxy.delete_all_histories_for_user(destination.galaxy_user_name, True)

    def _reboot_openstack_servers(self):
        if "name_contains" not in self.params:
            raise ValueError("'name_contains' is needed for rebooting openstack servers")
        reboot_type = self.params["reboot_type"] if "reboot_type" in self.params else "soft"

        servers = self.openstack.get_servers(self.params["name_contains"])
        self.openstack.reboot_servers(servers, reboot_type == "hard")

    def _reboot_random_openstack_server(self):
        if "name_contains" not in self.params:
            raise ValueError("'name_contains' is needed for rebooting openstack servers")
        reboot_type = self.params["reboot_type"] if "reboot_type" in self.params else "soft"

        servers = self.openstack.get_servers(self.params["name_contains"])

        rand_index = randrange(0, len(servers))
        self.openstack.reboot_servers([servers[rand_index]], reboot_type == "hard")

    def _rebuild_random_openstack_server(self):
        if "name_contains" not in self.params:
            raise ValueError("'name_contains' is needed for rebuilding openstack servers")

        servers = self.openstack.get_servers(self.params["name_contains"])

        rand_index = randrange(0, len(servers))
        self.openstack.rebuild_servers([servers[rand_index]])

    def __str__(self):
        return self.name

    __repr__ = __str__


def configure_task(task_conf: Dict, benchmark: BaseBenchmark):
    if task_conf["type"] == "AnsiblePlaybook":
        return AnsiblePlaybookTask(benchmark.destinations, task_conf["playbook"])

    if task_conf["type"] == "BenchmarkerTask":
        params = task_conf["params"] if "params" in task_conf else {}
        openstack = benchmark.benchmarker.openstack

        return BenchmarkerTask(benchmark.destinations, openstack, task_conf["name"], params)

    raise ValueError("Task type '{type}' not allowed!".format(type=task_conf["type"]))
