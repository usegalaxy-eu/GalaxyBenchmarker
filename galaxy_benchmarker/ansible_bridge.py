import os
import subprocess
from typing import List


def run_playbook(playbook_path, host, user, private_key, values: List = None):
    """
    Run ansible-playbook with the given parameters. Additional variables can be given in values as
    a List of (key, value)-pairs.
    """
    commands = ["ansible-playbook", playbook_path, "-i", host+",", "-u", user, "--private-key", private_key]
    if values is not None:
        for key, value in values:
            commands.append("-e")
            commands.append(key + "=" + value)

    with open(os.devnull, 'w') as devnull:
        subprocess.check_call(commands)
