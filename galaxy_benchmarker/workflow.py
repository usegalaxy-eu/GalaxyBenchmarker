"""
Definition of different workflow-types.
"""
import os
import logging
from typing import Dict

log = logging.getLogger("GalaxyBenchmarker")


class BaseWorkflow:
    description = ""

    def __init__(self, name, path):
        self.path = path
        self.name = name

    def __str__(self):
        return self.name

    __repr__ = __str__


class GalaxyWorkflow(BaseWorkflow):
    timeout = None

    def __init__(self, name, path):
        # Make sure that workflow file exists
        if not os.path.isfile(path):
            raise IOError("Workflow-File at '{path}' in workflow '{wf_name}' could not be found".format(path=path,
                                                                                                        wf_name=name))
        super().__init__(name, path)


class CondorWorkflow(BaseWorkflow):
    def __init__(self, name, path, job_file):
        super().__init__(name, path)

        # Check, if all workflow-directory exist
        if not os.path.isdir(path):
            raise IOError("Workflow-Directory at '{path}' in workflow '{wf_name}' could not be found"
                          .format(path=self.path, wf_name=name))

        # Check, if condor-job_file exists
        job_file_path = path + "/" + job_file
        if not os.path.isfile(job_file_path):
            raise IOError("Job-File at '{path}' in workflow '{wf_name}' could not be found".format(path=job_file_path,
                                                                                                   wf_name=name))
        self.job_file = job_file


def configure_workflow(wf_config: Dict) -> BaseWorkflow:
    """
    Initializes and configures a Workflow according to the given configuration. Returns the configured Workflow.
    """
    # Check, if all set properly
    if "name" not in wf_config:
        raise ValueError("No Workflow-Name set! Config: '{config}'".format(config=wf_config))
    if "path" not in wf_config:
        raise ValueError("No Workflow-Path set for '{workflow}'".format(workflow=wf_config["name"]))
    if "type" not in wf_config:
        raise ValueError("No Workflow-Type set for '{workflow}'".format(workflow=wf_config["name"]))
    if wf_config["type"] not in ["Galaxy", "Condor"]:
        raise ValueError("Workflow-Type '{type}' not valid".format(type=wf_config["type"]))

    if wf_config["type"] == "Galaxy":
        workflow = GalaxyWorkflow(wf_config["name"], wf_config["path"])
        if "description" in wf_config:
            workflow.description = wf_config["description"]
        workflow.timeout = None if "timeout" not in wf_config else wf_config["timeout"]

    if wf_config["type"] == "Condor":
        workflow = CondorWorkflow(wf_config["name"], wf_config["path"], wf_config["job_file"])

    return workflow
