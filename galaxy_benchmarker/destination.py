"""
Definition of different destination-types for workflows.
"""


class BaseDestination:
    def __init__(self, name):
        self.name = name


class PulsarMQDestination(BaseDestination):
    def __init__(self, name, amqp_url):
        self.amqp_url = amqp_url
        self.galaxy_user_key = "08d3b7a532947c7fb5af27281381c485"
        super().__init__(name)

    def create_galaxy_destination_user(self, glx):
        # TODO
        return


class CondorDestination(BaseDestination):
    def __init__(self, name):
        super().__init__(name)


class GalaxyCondorDestination(BaseDestination):
    def __init__(self, name):
        super().__init__(name)


def configure_destination(dest_config, glx):
    if dest_config["type"] not in ["PulsarMQ", "Condor", "GalaxyCondor"]:
        raise ValueError("Destination-Type '{type}' not valid".format(type=dest_config["type"]))

    if dest_config["type"] == "PulsarMQ":
        destination = PulsarMQDestination(dest_config["name"], dest_config["amqp_url"])

    if dest_config["type"] == "Condor":
        destination = CondorDestination(dest_config["name"])

    if dest_config["type"] == "GalaxyCondor":
        destination = GalaxyCondorDestination(dest_config["name"])

    return destination
