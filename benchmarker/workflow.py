"""
Definition of different workflow-types.
"""


class BaseWorkflow:
    STATE_INITIALIZED, STATE_STARTED, STATE_FAILED, STATE_FINISHED = "initialized", "started", "failed", "finished"
    state = None
    description = ""

    def __init__(self, name):
        self.name = name

    def run(self):
        """
        Starts workflow
        """
        raise NotImplementedError


class GalaxyWorkflow(BaseWorkflow):
    def __init__(self, name, path, galaxy_instance):
        self.path = path
        self.galaxy_instance = galaxy_instance
        self.state = super().STATE_INITIALIZED
        super().__init__(name)

    def run(self):
        self.state = super().STATE_STARTED
        raise NotImplementedError
