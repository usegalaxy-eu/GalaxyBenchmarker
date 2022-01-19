"""
Definition of different benchmark-types.
"""
import logging
import time
from datetime import datetime
from galaxy_benchmarker.models.destination import BaseDestination, GalaxyDestination, PulsarMQDestination
from galaxy_benchmarker.models.workflow import BaseWorkflow, GalaxyWorkflow
from galaxy_benchmarker.models.task import BaseTask, AnsiblePlaybookTask
from typing import List
from galaxy_benchmarker.bridge import influxdb


log = logging.getLogger("GalaxyBenchmarker")


class ColdWarmBenchmark:
    """
    The Base-Class of Benchmark. All Benchmarks should inherit from it.
    """
    allowed_dest_types = []
    allowed_workflow_types = []
    benchmarker = None
    galaxy = None
    benchmark_results = dict()
    pre_tasks: List[BaseTask] = None
    post_tasks: List[BaseTask] = None

    allowed_dest_types = [GalaxyDestination, PulsarMQDestination]
    allowed_workflow_types = [GalaxyWorkflow]
    cold_pre_task: AnsiblePlaybookTask = None
    warm_pre_task: AnsiblePlaybookTask = None


    def __init__(self, name, benchmarker, destinations: List[BaseDestination],
                 workflows: List[BaseWorkflow], galaxy, runs_per_workflow=1):
        self.name = name
        self.benchmarker = benchmarker
        self.uuid = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f") + "_" + name
        self.destinations = destinations
        self.workflows = workflows
        self.runs_per_workflow = runs_per_workflow
        self.galaxy = galaxy


    def run_pre_task(self):
        """
        Runs a Task before starting the actual Benchmark.
        """
        if self.pre_tasks is None:
            return
        for task in self.pre_tasks:
            log.info("Running task {task}".format(task=task))
            task.run()

    def run_post_task(self):
        """
        Runs a Task after Benchmark finished (useful for cleaning up etc.).
        """
        if self.post_tasks is None:
            return
        for task in self.post_tasks:
            log.info("Running task {task}".format(task=task))
            task.run()

    def run(self):
        """
        Runs the Workflow on each Destinations. First runs all Workflows "cold" (cleaning Pulsar up before each run),
        after that the "warm"-runs begin.
        """
        try:
            for run_type in ["cold", "warm"]:
                self.benchmark_results[run_type] = dict()
                for destination in self.destinations:
                    self.benchmark_results[run_type][destination.name] = dict()
                    for workflow in self.workflows:
                        self.benchmark_results[run_type][destination.name][workflow.name] = list()
                        results = self.benchmark_results[run_type][destination.name][workflow.name]

                        log.info("Up next: {type} run for workflow {workflow} for destination: {dest}.".format(type=run_type, workflow=workflow.name, dest=destination.name))

                        # Cold runs
                        for i in range(self.runs_per_workflow):
                            log.debug(f"({i+1}/{self.runs_per_workflow}): Running cold pre-task")
                            self.cold_pre_task.run() # TODO: currently runs on all destinations

                            log.debug(f"({i+1}/{self.runs_per_workflow}): Running workflow")
                            result = self._run_workflow(destination, workflow, max_retires=0)
                            results.append(result)

                        # Warm runs
                        for i in range(self.runs_per_workflow):
                            log.debug(f"({i+1}/{self.runs_per_workflow}): Running warm pre-task")
                            self.warm_pre_task.run() # TODO: currently runs on all destinations

                            log.debug(f"({i+1}/{self.runs_per_workflow}): Running workflow")
                            result = self._run_workflow(destination, workflow)
                            results.append(result)

        except KeyboardInterrupt:
            log.info("Received KeyboardInterrupt. Stopping current benchmark")



    def save_results_to_influxdb(self, inflxdb: influxdb.InfluxDB):
        """
        Sends all the metrics of the benchmark_results to influxDB.
        """
        for run_type, per_dest_results in self.benchmark_results.items():
            for dest_name, workflows in per_dest_results.items():
                for workflow_name, runs in workflows.items():
                    for run in runs:
                        if run is None:
                            continue

                        if "workflow_metrics" in run:
                            # Save metrics per workflow-run
                            tags = {
                                "benchmark_name": self.name,
                                "benchmark_uuid": self.uuid,
                                "benchmark_type": type(self),
                                "destination_name": dest_name,
                                "workflow_name": workflow_name,
                                "history_name": run["history_name"] if "history_name" in run else None,
                                "run_type": run_type,
                            }

                            inflxdb.save_workflow_metrics(tags, run["workflow_metrics"])

                        # Save job-metrics if workflow succeeded
                        if runs is None or run["status"] == "error" or "jobs" not in run or run["jobs"] is None:
                            continue

                        for job in run["jobs"].values():
                            tags = {
                                "benchmark_name": self.name,
                                "benchmark_uuid": self.uuid,
                                "benchmark_type": type(self),
                                "destination_name": dest_name,
                                "workflow_name": workflow_name,
                                "history_name": run["history_name"] if "history_name" in run else None,
                                "run_type": run_type,
                            }
                            inflxdb.save_job_metrics(tags, job)

    def _run_benchmark(self, destination, workflow, max_retries=4):
        for current in range(max_retries+1):
            result = destination.run_workflow(workflow)

            if result["status"] == "error":
                wait_seconds = 60 * 2 ** current
                log.warning(f"Error occured! Retrying after %d seconds..", wait_seconds)
                time.sleep(wait_seconds)

                continue

            result["workflow_metrics"] = {
                "status": {
                    "name": "workflow_status",
                    "type": "string",
                    "plugin": "benchmarker",
                    "value": result["status"]
                },
                "total_runtime": {
                    "name": "total_workflow_runtime",
                    "type": "float",
                    "plugin": "benchmarker",
                    "value": result["total_workflow_runtime"]
                }
            }

            if "history_name" in result:
                result["jobs"] = destination.get_jobs(result["history_name"])

            log.info("Finished running '%s' with status '%s' in %d seconds.", workflow.name, result["status"], result["total_workflow_runtime"])

            return result
        # In case of to many failures, return an empty result
        return {}


    def __str__(self):
        return self.name

    __repr__ = __str__
