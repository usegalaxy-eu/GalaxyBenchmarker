"""
Definition of different destination-types for workflows.
"""


class BaseDestination:
    def __init__(self, name):
        self.name = name


class PulsarMQDestination(BaseDestination):
    def __init__(self, name, amqp_url):
        self.amqp_url = amqp_url
        super().__init__(name)


class CondorDestination(BaseDestination):
    def __init__(self, name):
        super().__init__(name)


class GalaxyCondorDestination(BaseDestination):
    def __init__(self, name):
        super().__init__(name)
