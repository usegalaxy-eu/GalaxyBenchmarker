import os
import subprocess
from typing import Dict




def run_playbook(playbook_path, host, user, private_key, values: Dict = None):
    """
    Run ansible-playbook with the given parameters. Additional variables can be given in values as a dict.
    """
    commands = ["ansible-playbook", playbook_path, "-i", host+",", "-u", user, "--private-key", private_key]
    if values is not None:
        for key, value in values.items():
            commands.append("-e")
            commands.append(key + "=" + value)

    with open(os.devnull, 'w') as devnull:
        subprocess.check_call(commands)
