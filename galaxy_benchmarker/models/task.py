from __future__ import annotations
import abc
import logging

from random import randrange
from typing import List, Type, Optional, TYPE_CHECKING, Any
from pathlib import Path

if TYPE_CHECKING:
    from galaxy_benchmarker.benchmarker import Benchmarker

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

    @staticmethod
    def from_config(name: str, task_config: Any, benchmarker: Benchmarker) -> Task:
        """Create a task from task_config. Config can be:
        - an object defining the task
        - a string refering to a task definition
        """
        match task_config:
            case str():
                potential_task = benchmarker.tasks.get(task_config, None)
                if not potential_task:
                    raise ValueError(f"Unknown task reference '{task_config}' for '{name}'")
            case dict():
                potential_task = Task.create(name, task_config)
            case _:
                raise ValueError(f"Unknown value for '{name}': {task_config}")
        return potential_task

    @abc.abstractmethod
    def run(self):
        """Execute the task"""

    def __str__(self):
        return f"{self.__class__.__name__}({self.name})"

    __repr__ = __str__


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
