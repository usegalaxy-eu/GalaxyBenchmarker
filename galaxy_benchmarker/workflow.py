"""
Definition of different workflow-types.
"""
import planemo_bridge
import os
import logging
from glx import Galaxy
from destination import BaseDestination, PulsarMQDestination

log = logging.getLogger(__name__)


class BaseWorkflow:
    description = ""

    def __init__(self, name):
        self.name = name

    def run(self, glx: Galaxy, dest: BaseDestination):
        """
        Starts workflow
        """
        raise NotImplementedError


class GalaxyWorkflow(BaseWorkflow):
    def __init__(self, name, path):
        # TODO: Check, if path is existing
        if not os.path.isfile(path):
            raise IOError("Workflow-File at '{path}' in workflow '{wf_name}' could not be found".format(path=path,
                                                                                                        wf_name=name))
        self.path = path
        super().__init__(name)

    def run(self, glx: Galaxy, dest: PulsarMQDestination):
        log.info("Running workflow '{wf_name}' using Planemo".format(wf_name=self.name))
        return planemo_bridge.run_planemo(glx, dest, self.path)


def configure_workflow(wf_config):
    if wf_config["type"] not in ["Galaxy"]:
        raise ValueError("Workflow-Type '{type}' not valid".format(type=wf_config["type"]))

    if wf_config["type"] == "Galaxy":
        workflow = GalaxyWorkflow(wf_config["name"], wf_config["path"])
        if "description" in wf_config:
            workflow.description = wf_config["description"]

    return workflow
