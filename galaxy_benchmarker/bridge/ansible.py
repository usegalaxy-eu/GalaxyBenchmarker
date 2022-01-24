from __future__ import annotations
import subprocess
from typing import Dict, Any, TYPE_CHECKING
from galaxy_benchmarker.models import task
from pathlib import Path
import logging
from dataclasses import dataclass

if TYPE_CHECKING:
    from galaxy_benchmarker.benchmarker import Benchmarker

log = logging.getLogger(__name__)


@dataclass
class AnsibleDestination:
    host: str
    user: str
    private_key:str

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
        destination.user,
        "--private-key",
        destination.private_key
    ]
    for key, value in values.items():
        commands.append("-e")
        commands.append(f"{key}={value}")

    subprocess.check_call(commands)


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

        self.destination = None
        if "destination" in config:
            self.destination = AnsibleDestination(**config["destination"])

        self.values = config.get("values", {})

    def run(self):
        if not self.destination:
            raise ValueError("'destination' is required, when task is executed through 'run()'")
        self.run_at(self.destination)

    def run_at(self, destination: AnsibleDestination) -> None:
        run_playbook(self.playbook, destination, self.values)

@task.register_task
class AnsibleNoopTask(AnsibleTask):
    """Does nothing, acts as placeholder"""
    def __init__(self, name: str, config: dict):
        self.name = "AnsibleNoopTask"

    def run(self):
        pass

    def run_at(self, destination: AnsibleDestination) -> None:
        pass


def get_ansibletask_from_config(name: str, task_config: Any, benchmarker: Benchmarker) -> AnsibleTask:
    match task_config:
        case None:
            potential_task = AnsibleNoopTask()
            log.warning("No task defined for '%s'", name)
        case str():
            potential_task = benchmarker.tasks.get(task_config, None)
            if not potential_task:
                raise ValueError(f"Unknown task reference '{task_config}' for '{name}'")
        case dict():
            potential_task = task.Task.create(name, task_config)
        case _:
            raise ValueError(f"Unknown value for '{name}': {task_config}")

    if not isinstance(potential_task, AnsibleTask):
        raise ValueError(f"'{name}' is not an AnsibleTask")

    return potential_task