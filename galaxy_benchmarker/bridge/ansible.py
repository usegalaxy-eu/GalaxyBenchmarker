from __future__ import annotations
import subprocess
from typing import Dict, Any, TYPE_CHECKING, Optional
from galaxy_benchmarker.models import task
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
    user: str
    private_key: Optional[str] = ""

def run_playbook(playbook: Path, destination: AnsibleDestination, values: Dict = {}):
    """Run ansible-playbook with the given parameters. Additional variables
    can be given in values as a dict.
    """
    commands = [
        "ansible-playbook",
        str(playbook),
        "-i",
        f"{destination.host},",
        "-u",
        destination.user
    ]

    if destination.private_key:
        commands.extend(["--private-key",destination.private_key])

    for key, value in values.items():
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


@task.register_task
class AnsibleTask(task.Task):
    def __init__(self, name: str, config: dict) -> None:
        super().__init__(name, config)

        playbook_name = config.get("playbook", "")
        playbook_folder = config.get("folder", "playbooks/")

        if not playbook_name:
            raise ValueError(f"'playbook' property is missing for task {name}")
        
        self.playbook = Path(playbook_folder) / playbook_name
        if not self.playbook.is_file():
            raise ValueError(f"Playbook for task {name} is not a vaild file. Path: '{self.playbook}'")

        self.destinations = []
        for destination in config.get("destinations", []):
            self.destinations.append(AnsibleDestination(**destination))

        self.values = config.get("values", {})

    @staticmethod
    def from_config(name: str, task_config: Any, benchmarker: Benchmarker) -> AnsibleTask:
        """Create a task from task_config. Config can be:
        - None for default Noop-Task
        - an object defining the task
        - a string refering to a task definition
        """
        if not task_config:
            potential_task = AnsibleNoopTask()
            log.warning("No task defined for '%s'", name)
        else:
            potential_task = task.Task.from_config(name, task_config, benchmarker)

        if not isinstance(potential_task, AnsibleTask):
            raise ValueError(f"'{name}' is not an AnsibleTask")

        return potential_task

    def run(self):
        if not self.destinations:
            raise ValueError("'destinations' is required, when task is executed through 'run()'")

        for dest in self.destinations:
            self.run_at(dest)

    def run_at(self, destination: AnsibleDestination) -> None:
        run_playbook(self.playbook, destination, self.values)

@task.register_task
class AnsibleNoopTask(AnsibleTask):
    """Does nothing, acts as placeholder"""
    def __init__(self, *args, **kwargs):
        self.name = "AnsibleNoopTask"

    def run(self):
        pass

    def run_at(self, destination: AnsibleDestination) -> None:
        pass
