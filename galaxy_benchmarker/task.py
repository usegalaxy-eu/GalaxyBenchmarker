class BaseTask:
    def __init__(self):
        return


class AnsiblePlaybookTask(BaseTask):
    def __init__(self, playbook):
        self.playbook = playbook
        super().__init__()
