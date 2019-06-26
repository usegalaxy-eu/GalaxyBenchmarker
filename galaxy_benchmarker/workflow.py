"""
Definition of different workflow-types.
"""
import planemo_bridge
import os
import logging
import time
from galaxy_bridge import Galaxy
import condor_bridge
from destination import BaseDestination, PulsarMQDestination, CondorDestination

log = logging.getLogger("GalaxyBenchmarker")


class BaseWorkflow:
    description = ""

    def __init__(self, name, path):
        self.path = path
        self.name = name

    def run(self, dest: BaseDestination):
        """
        Starts workflow
        """
        raise NotImplementedError


class GalaxyWorkflow(BaseWorkflow):
    def __init__(self, name, path):
        if not os.path.isfile(path):
            raise IOError("Workflow-File at '{path}' in workflow '{wf_name}' could not be found".format(path=path,
                                                                                                        wf_name=name))
        super().__init__(name, path)

    def run(self, dest: PulsarMQDestination, glx: Galaxy):
        log.info("Running workflow '{wf_name}' using Planemo".format(wf_name=self.name))
        return planemo_bridge.run_planemo(glx, dest, self.path)


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

    def deploy_to_condor_manager(self, destination: CondorDestination):
        log.info("Deploying {workflow} to {destination}".format(workflow=self.name, destination=destination.name))
        destination.deploy_workflow(self)

    def run(self, destination: CondorDestination):
        client = condor_bridge.get_paramiko_client(destination.host, destination.host_user, destination.ssh_key)

        remote_workflow_dir = "{jobs_dir}/{wf_name}".format(jobs_dir=destination.jobs_directory_dir,
                                                            wf_name=self.name)

        start_time = time.monotonic()
        job_ids = condor_bridge.submit_job(client, remote_workflow_dir, self.job_file)

        status = "unknown"
        while status != "done":
            try:
                job_status = condor_bridge.get_job_status(client, job_ids["id"])
            except ValueError as error:
                status = "error"
                log.error("There was an error with run of {workflow}: {error}".format(workflow=self.name,
                                                                                      error=error))
                break
            status = job_status["status"]
            time.sleep(0.1)  # TODO: Figure out, if that timing is to fast

        runtime = time.monotonic() - start_time

        client.close()

        result = {
            "status": "success" if status == "done" else "error",
            "jobs": {
                self.job_file: {
                    "id": 1,
                    "parsed_job_metrics": {
                        "runtime_seconds": {
                            "name": "runtime_seconds",
                            "type": "float",
                            "value": runtime
                        }
                    }
                }
            }
        }

        return result


def configure_workflow(wf_config):
    if wf_config["type"] not in ["Galaxy", "Condor"]:
        raise ValueError("Workflow-Type '{type}' not valid".format(type=wf_config["type"]))

    if wf_config["type"] == "Galaxy":
        workflow = GalaxyWorkflow(wf_config["name"], wf_config["path"])
        if "description" in wf_config:
            workflow.description = wf_config["description"]

    if wf_config["type"] == "Condor":
        workflow = CondorWorkflow(wf_config["name"], wf_config["path"], wf_config["job_file"])

    return workflow
