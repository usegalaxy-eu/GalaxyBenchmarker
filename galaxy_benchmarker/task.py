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
    def __init__(self, benchmark, name):
        self.name = name
        super().__init__(benchmark)

    def run(self):
        if self.name == "delete_old_histories":
            for destination in self.benchmark.destinations:
                self._delete_old_histories(destination)

    def _delete_old_histories(self, destination):
        destination.galaxy.delete_all_histories_for_user(destination.galaxy_user_name, True)
