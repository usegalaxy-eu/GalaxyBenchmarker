from __future__ import annotations
import subprocess
from typing import Dict, Any, TYPE_CHECKING, Optional
from pathlib import Path
import logging
from dataclasses import dataclass
import io
import tempfile


import os
if TYPE_CHECKING:
    from galaxy_benchmarker.benchmarker import Benchmarker

log = logging.getLogger(__name__)


@dataclass
class AnsibleDestination:
    host: str
    user: Optional[str] = ""
    private_key: Optional[str] = ""


def run_playbook(playbook: Path, destination: AnsibleDestination, extra_vars: Dict = {}):
    """Run ansible-playbook with the given parameters. Additional variables
    can be given in values as a dict.
    """
    commands = [
        "ansible-playbook",
        str(playbook),
        "-i",
        f"{destination.host},"
    ]

    if destination.user:
        commands.extend(["-u", destination.user])

    if destination.private_key:
        commands.extend(["--private-key",destination.private_key])

    for key, value in extra_vars.items():
        commands.append("-e")
        commands.append(f"{key}={value}")

    log.debug("Run ansible: %s", commands)
    with tempfile.TemporaryFile() as output:
        try:
            subprocess.check_call(commands, stdout=output)
        except subprocess.CalledProcessError:
            # In case of an exception print the whole output of ansible
            output.seek(0)
            for line in output.readlines():
                log.error(line.decode("utf-8"))
            raise


class AnsibleTask:
    def __init__(self, config: dict, name: Optional[str] = None) -> None:
        playbook_name = config.get("playbook", "")
        playbook_folder = config.get("folder", "playbooks/")

        if name:
            self.name = name
        else:
            self.name = playbook_name.split(".")[0]

        if not playbook_name:
            raise ValueError(f"'playbook' property is missing for task {self.name}")
        
        self.playbook = Path(playbook_folder) / playbook_name
        if not self.playbook.is_file():
            raise ValueError(f"Playbook for task {self.name} is not a vaild file. Path: '{self.playbook}'")

        self.destinations = []
        for destination in config.get("destinations", []):
            self.destinations.append(AnsibleDestination(**destination))

        self.values = config.get("values", {})

    @staticmethod
    def from_config(task_config: Any, name: str, benchmarker: Benchmarker) -> AnsibleTask:
        """Create a task from task_config. Config can be:
        - None for default Noop-Task
        - an object defining the task
        - a string refering to a task definition
        """
        match task_config:
            case None:
                potential_task = AnsibleNoopTask()
                log.warning("No task defined for '%s'", name)
            case str():
                potential_task = benchmarker.tasks.get(task_config, None)
                if not potential_task:
                    raise ValueError(f"Unknown task reference '{task_config}' for '{name}'")
            case dict():
                potential_task = AnsibleTask(task_config, name)
            case _:
                raise ValueError(f"Unknown value for '{name}': {task_config}")

        return potential_task

    def run(self):
        if not self.destinations:
            raise ValueError("'destinations' is required, when task is executed through 'run()'")

        for dest in self.destinations:
            self.run_at(dest, self.values)

    def run_at(self, destination: AnsibleDestination, extra_vars: dict = {}) -> None:
        run_playbook(self.playbook, destination, extra_vars)

class AnsibleNoopTask(AnsibleTask):
    """Does nothing, acts as placeholder"""
    def __init__(self, *args, **kwargs):
        self.name = "AnsibleNoopTask"

    def run(self):
        pass

    def run_at(self, destination: AnsibleDestination) -> None:
        pass
