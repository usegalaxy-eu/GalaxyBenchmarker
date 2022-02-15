from __future__ import annotations

import dataclasses
import logging
import subprocess
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Optional, Sequence

if TYPE_CHECKING:
    from galaxy_benchmarker.benchmarker import Benchmarker
    from galaxy_benchmarker.config import NamedConfigDicts

log = logging.getLogger(__name__)

LOG_ANSIBLE_OUTPUT = False

_destinations: NamedConfigDicts = {}


@dataclasses.dataclass
class AnsibleDestination:
    host: str
    user: Optional[str] = ""
    private_key: Optional[str] = ""

    @staticmethod
    def register(configs: NamedConfigDicts) -> None:
        global _destinations
        _destinations = configs

    @classmethod
    def from_config(cls, dest_config: Any, name: str) -> AnsibleDestination:
        """Create a destination from dest_config. Config can be:
        - an object defining the destination
        - a string refering to a destination definition
        """
        d_name, d_config = name, dest_config
        if isinstance(dest_config, str):
            if dest_config not in _destinations:
                raise ValueError(f"Unknown destination reference {dest_config}")
            d_config = _destinations[dest_config]
            d_name = dest_config

        if not isinstance(d_config, dict):
            raise ValueError(
                f"Expected dict as destination config for {d_name}. Received {type(d_config)}"
            )

        # Filter out extra arguments
        names = set([f.name for f in dataclasses.fields(cls)])
        return cls(**{k: v for k, v in d_config.items() if k in names})


def run_playbook(
    playbook: Path, destination: AnsibleDestination, extra_vars: Dict = {}
):
    """Run ansible-playbook with the given parameters. Additional variables
    can be given in values as a dict.
    """
    commands = ["ansible-playbook", str(playbook), "-i", f"{destination.host},"]

    if destination.user:
        commands.extend(["-u", destination.user])

    if destination.private_key:
        commands.extend(["--private-key", destination.private_key])

    for key, value in extra_vars.items():
        commands.append("-e")
        commands.append(f"{key}={value}")

    log.debug("Run ansible: %s", commands)
    with tempfile.TemporaryFile() as cached_output:
        process = subprocess.Popen(
            commands, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        while process.poll() is None:
            assert process.stdout, "Stdout of ansible subprocess is None"
            output = process.stdout.readline()
            cached_output.write(output)
            if LOG_ANSIBLE_OUTPUT:
                log.info(output.decode("utf-8"))

        if process.returncode != 0:
            cached_output.seek(0)
            for line in cached_output.readlines():
                log.error(line.decode("utf-8"))
            raise RuntimeError(
                f"Ansible exited with non-zero exit code: {process.returncode}"
            )


_tasks: NamedConfigDicts = {}


class AnsibleTask:
    def __init__(
        self,
        playbook_name: str,
        playbook_folder: str = "playbooks/",
        name: Optional[str] = None,
        destinations: Sequence[AnsibleDestination] = [],
        extra_vars: dict = {},
    ) -> None:
        """
        playbook_name: Name of the playbook which will be executed
        playbook_folder: path to the playbook folder
        name: Displayname used for logging and stuff
        destinations: Default destinations used when calling run()
        extra_vars: Default extra_vars used when calling run()
        """
        if name:
            self.name = name
        else:
            self.name = playbook_name.split(".")[0]

        if not playbook_name:
            raise ValueError(f"'playbook' property is missing for task {self.name}")

        self.playbook = Path(playbook_folder) / playbook_name
        if not self.playbook.is_file():
            raise ValueError(
                f"Playbook for task {self.name} is not a vaild file. Path: '{self.playbook}'"
            )

        self.destinations = destinations
        self.extra_vars = extra_vars

    @staticmethod
    def register(configs: NamedConfigDicts) -> None:
        global _tasks
        _tasks = configs

    @staticmethod
    def from_config(task_config: Any, name: str) -> AnsibleTask:
        """Create a task from task_config. Config can be:
        - an object defining the task
        - a string refering to a task definition
        """
        t_name, t_config = name, task_config
        if isinstance(task_config, str):
            if task_config not in _tasks:
                raise ValueError(f"Unknown task reference {task_config}")
            t_config = _tasks[task_config]
            t_name = task_config

        if not isinstance(t_config, dict):
            raise ValueError(
                f"Expected dict as task config for {t_name}. Received {type(t_config)}"
            )

        # Parse destinations
        destinations: list[AnsibleDestination] = []
        if "destination" in t_config:
            if "destinations" in t_config:
                raise ValueError(f"'destination' and 'destinations' given for '{name}'")

            dest = AnsibleDestination.from_config(
                t_config["destination"], f"{name}_destination"
            )
            destinations.append(dest)
        elif "destinations" in t_config:
            for i, dest_config in enumerate(t_config.get("destinations", [])):
                dest = AnsibleDestination.from_config(
                    dest_config, f"{name}_destination_{i}"
                )
                destinations.append(dest)
        else:
            raise ValueError(
                f"'destination' or 'destinations' is required for '{name}'"
            )

        return AnsibleTask(
            playbook_name=t_config.get("playbook", ""),
            playbook_folder=t_config.get("folder", "playbooks/"),
            name=t_name,
            destinations=destinations,
            extra_vars=t_config.get("extra_vars", {}),
        )

    def run(self):
        if not self.destinations:
            raise ValueError(
                "'destinations' is required, when task is executed through 'run()'"
            )

        for dest in self.destinations:
            self.run_at(dest, self.extra_vars)

    def run_at(self, destination: AnsibleDestination, extra_vars: dict = {}) -> None:
        run_playbook(self.playbook, destination, extra_vars)
