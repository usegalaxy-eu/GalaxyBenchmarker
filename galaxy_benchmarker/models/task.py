from __future__ import annotations
import abc
import logging

from random import randrange
from typing import List, Type
from pathlib import Path

# from galaxy_benchmarker.models.destination import BaseDestination
# from galaxy_benchmarker.models.benchmark import BaseBenchmark
from galaxy_benchmarker.bridge.openstack import OpenStackCompute

log = logging.getLogger(__name__)

_registered_tasks: dict[str,Type["Task"]] = {}

def register_task(cls: Type):
    """Register a task for factory method"""

    name = cls.__name__
    log.debug("Registering task %s", name)

    if name in _registered_tasks:
        module = cls.__module__
        raise ValueError(f"Already registered. Use another name for {module}.{name}")

    _registered_tasks[name] = cls

    return cls



class Task(abc.ABC):
    def __init__(self, name: str, config: dict):
        self.name = name

    @staticmethod
    def create(name: str, config: dict):
        """Factory method for tasks

        name: task name
        config: task specific config
        """

        task_type = config.get("type", None)
        if task_type is None:
            raise ValueError(f"'type' property is missing for task {config}")

        if task_type not in _registered_tasks:
            raise ValueError(f"Unkown task type: {task_type}")

        task_class = _registered_tasks[task_type]
        return task_class(name, config)

    @abc.abstractmethod
    def run(self):
        """Execute the task"""

    def __str__(self):
        return f"{self.__class__.__name__}({self.name})"

    __repr__ = __str__


@register_task
class AnsibleTask(Task):
    def __init__(self, name: str, config: dict) -> None:
        super().__init__(name, config)

        playbook_name = config.get("playbook", "")
        playbook_folder = config.get("folder", "playbooks/")

        if not playbook_name:
            raise ValueError(f"'playbook' property is missing for task {name}")
        
        self.playbook = Path(playbook_folder) / playbook_name
        if not self.playbook.is_file():
            raise ValueError(f"Playbook for task {name} is not a vaild file. Path: '{self.playbook}'")

    def run():
        raise NotImplementedError()

@register_task
class AnsibleNoopTask(AnsibleTask):
    """Does nothing, acts as placeholder"""
    def __init__(self, name: str, config: dict):
        self.name = "AnsibleNoopTask"

    def run(self):
        pass



#     def run(self):
#         if self.name == "delete_old_histories":
#             for destination in self.destinations:
#                 self._delete_old_histories(destination)
#         elif self.name == "reboot_openstack_servers":
#             self._reboot_openstack_servers()
#         elif self.name == "reboot_random_openstack_server":
#             self._reboot_random_openstack_server()
#         elif self.name == "rebuild_random_openstack_server":
#             self._rebuild_random_openstack_server()
#         else:
#             raise ValueError("{name} is not a valid BenchmarkerTask!".format(name=self.name))

#     def _delete_old_histories(self, destination):
#         destination.galaxy.delete_all_histories_for_user(destination.galaxy_user_name, True)

#     def _reboot_openstack_servers(self):
#         if "name_contains" not in self.params:
#             raise ValueError("'name_contains' is needed for rebooting openstack servers")
#         reboot_type = self.params["reboot_type"] if "reboot_type" in self.params else "soft"

#         servers = self.openstack.get_servers(self.params["name_contains"])
#         self.openstack.reboot_servers(servers, reboot_type == "hard")

#     def _reboot_random_openstack_server(self):
#         if "name_contains" not in self.params:
#             raise ValueError("'name_contains' is needed for rebooting openstack servers")
#         reboot_type = self.params["reboot_type"] if "reboot_type" in self.params else "soft"

#         servers = self.openstack.get_servers(self.params["name_contains"])

#         rand_index = randrange(0, len(servers))
#         self.openstack.reboot_servers([servers[rand_index]], reboot_type == "hard")

#     def _rebuild_random_openstack_server(self):
#         if "name_contains" not in self.params:
#             raise ValueError("'name_contains' is needed for rebuilding openstack servers")

#         servers = self.openstack.get_servers(self.params["name_contains"])

#         rand_index = randrange(0, len(servers))
#         self.openstack.rebuild_servers([servers[rand_index]])
