from random import randrange
from typing import Dict

class BaseTask:
    def __init__(self, benchmark):
        self.benchmark = benchmark

    def run(self):
        raise NotImplementedError


class AnsiblePlaybookTask(BaseTask):
    def __init__(self, benchmark, playbook):
        self.playbook = playbook
        super().__init__(benchmark)

    def run(self):
        for destination in self.benchmark.destinations:
            destination.run_ansible_playbook_task(self)


class BenchmarkerTask(BaseTask):
    def __init__(self, benchmark, name, params=dict()):
        self.name = name
        self.params = params
        super().__init__(benchmark)

    def run(self):
        if self.name == "delete_old_histories":
            for destination in self.benchmark.destinations:
                self._delete_old_histories(destination)
        if self.name == "reboot_openstack_servers":
            self._reboot_openstack_servers()
        if self.name == "reboot_random_openstack_server":
            self._reboot_random_openstack_server()
        if self.name == "rebuild_random_openstack_server":
            self._rebuild_random_openstack_server()

    def _delete_old_histories(self, destination):
        destination.galaxy.delete_all_histories_for_user(destination.galaxy_user_name, True)

    def _reboot_openstack_servers(self):
        if "name_contains" not in self.params:
            raise ValueError("'name_contains' is needed for rebooting openstack servers")
        reboot_type = self.params["reboot_type"] if "reboot_type" in self.params else "soft"

        os = self.benchmark.benchmarker.openstack
        servers = os.get_servers(self.params["name_contains"])
        os.reboot_servers(servers, reboot_type == "hard")

    def _reboot_random_openstack_server(self):
        if "name_contains" not in self.params:
            raise ValueError("'name_contains' is needed for rebooting openstack servers")
        reboot_type = self.params["reboot_type"] if "reboot_type" in self.params else "soft"

        os = self.benchmark.benchmarker.openstack
        servers = os.get_servers(self.params["name_contains"])

        rand_index = randrange(0, len(servers))
        os.reboot_servers([servers[rand_index]], reboot_type == "hard")

    def _rebuild_random_openstack_server(self):
        if "name_contains" not in self.params:
            raise ValueError("'name_contains' is needed for rebuilding openstack servers")

        os = self.benchmark.benchmarker.openstack
        servers = os.get_servers(self.params["name_contains"])

        rand_index = randrange(0, len(servers))
        os.rebuild_servers([servers[rand_index]])


def configure_task(task_conf: Dict, benchmark):
    if task_conf["type"] == "AnsiblePlaybook":
        return AnsiblePlaybookTask(benchmark, task_conf["playbook"])

    if task_conf["type"] == "BenchmarkerTask":
        params = task_conf["params"] if "params" in task_conf else {}
        return BenchmarkerTask(benchmark, task_conf["name"], params)

    raise ValueError("Task type '{type}' not allowed!".format(type=task_conf["type"]))
