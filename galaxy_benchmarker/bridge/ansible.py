from __future__ import annotations

import logging
import subprocess
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Optional

if TYPE_CHECKING:
    from galaxy_benchmarker.config import NamedConfigDicts

log = logging.getLogger(__name__)

LOG_ANSIBLE_OUTPUT = False


def run_playbook(playbook: Path, host: str, extra_vars: Dict = {}):
    """Run ansible-playbook with the given parameters. Additional variables
    can be given in values as a dict.
    """
    commands = [
        "ansible-playbook",
        str(playbook),
        "-i",
        "ansible/inventory",
        "-e",
        f"host={host}",
    ]

    for key, value in extra_vars.items():
        commands.append("-e")
        commands.append(f"{key}={value}")

    log.log(
        logging.INFO if LOG_ANSIBLE_OUTPUT else logging.DEBUG,
        "Run ansible: %s",
        commands,
    )

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
            if not LOG_ANSIBLE_OUTPUT:
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
        playbook: str,
        playbook_folder: str = "ansible/",
        name: Optional[str] = None,
        host: str = "",
        extra_vars: dict = {},
        *args,
        **kwargs,
    ) -> None:
        """
        playbook: Name of the playbook which will be executed
        playbook_folder: path to the playbook folder
        name: Displayname used for logging and stuff
        host: ansible host or host_pattern
        extra_vars: Default extra_vars used when calling run()
        """
        if name:
            self.name = name
        else:
            self.name = playbook.split(".")[0]

        if not playbook:
            raise ValueError(f"'playbook' property is missing for task {self.name}")

        self.playbook = Path(playbook_folder) / playbook
        if not self.playbook.is_file():
            raise ValueError(
                f"Playbook for task {self.name} is not a vaild file. Path: '{self.playbook}'"
            )

        if not isinstance(host, str):
            raise ValueError(
                f"'host' property has to be of type 'str' for task {self.name}"
            )
        self.host = host

        if not isinstance(extra_vars, dict):
            raise ValueError(
                f"'extra_vars' property has to be of type 'dict' for task {self.name}"
            )
        self.extra_vars = extra_vars

    @staticmethod
    def register(configs: NamedConfigDicts) -> None:
        """Register name -> config pairs for later use. Tasks can be referenced by
        name and will be substituted with the values given here"""
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

        return AnsibleTask(**t_config)

    def run(self) -> None:
        """Run AnsibleTask on hosts given during creation"""
        if not self.host:
            raise ValueError(
                "'host' is required, when task is executed through 'run()'"
            )

        self.run_at(self.host, self.extra_vars)

    def run_at(self, host: str, extra_vars: dict = {}) -> None:
        """Run AnsibleTask on specified host/hosts"""
        run_playbook(self.playbook, host, extra_vars)
