"""
Definition of different benchmark-types.
"""
import logging
import time
from datetime import datetime
from destination import BaseDestination, PulsarMQDestination, CondorDestination
from workflow import BaseWorkflow, GalaxyWorkflow, CondorWorkflow
from task import BaseTask, AnsiblePlaybookTask
from typing import List, Dict
from influxdb_bridge import InfluxDB


log = logging.getLogger("GalaxyBenchmarker")


class BaseBenchmark:
    allowed_dest_types = []
    allowed_workflow_types = []
    galaxy = None
    benchmark_results = dict()
    pre_task: BaseTask = None
    post_task: BaseTask = None

    def __init__(self, name, destinations: List[BaseDestination],
                 workflows: List[BaseWorkflow], runs_per_workflow=1):
        self.name = name
        self.uuid = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        self.destinations = destinations
        self.workflows = workflows
        self.runs_per_workflow = runs_per_workflow

    def run(self, benchmarker):
        raise NotImplementedError

    def save_results_to_influxdb(self, inflxdb: InfluxDB):
        for run_type, per_dest_results in self.benchmark_results.items():
            for dest_name, workflows in per_dest_results.items():
                for workflow_name, runs in workflows.items():
                    for run in runs:
                        for job in run["jobs"].values():
                            tags = {
                                "benchmark_name": self.name,
                                "benchmark_uid": self.uuid,
                                "benchmark_type": type(self),
                                "destination_name": dest_name,
                                "workflow_name": workflow_name,
                                "history_name": run["history_name"],
                                "run_type": run_type,
                            }
                            inflxdb.save_job_metrics(tags, job)


class ColdWarmBenchmark(BaseBenchmark):
    allowed_dest_types = [PulsarMQDestination]
    allowed_workflow_types = [GalaxyWorkflow]
    cold_pre_task: AnsiblePlaybookTask = None
    warm_pre_task: AnsiblePlaybookTask = None

    def __init__(self, name, destinations: List[PulsarMQDestination],
                 workflows: List[GalaxyWorkflow], runs_per_workflow=1):
        super().__init__(name, destinations, workflows, runs_per_workflow)
        self.destinations = destinations
        self.workflows = workflows

    def run(self, benchmarker):
        if self.pre_task is not None:
            log.info("Running pre-task '{task}'".format(task=self.pre_task))
            self.destinations[0].run_task(self.pre_task)
        for run_type in ["cold", "warm"]:
            self.benchmark_results[run_type] = run_galaxy_benchmark(self, benchmarker.glx, self.destinations,
                                                                    self.workflows,
                                                                    self.runs_per_workflow, run_type)
        if self.post_task is not None:
            log.info("Running post-task '{task}'".format(task=self.post_task))
            self.destinations[0].run_task(self.post_task)


class DestinationComparisonBenchmark(BaseBenchmark):
    allowed_dest_types = [PulsarMQDestination]
    allowed_workflow_types = [GalaxyWorkflow]

    def __init__(self, name, destinations: List[PulsarMQDestination],
                 workflows: List[GalaxyWorkflow], runs_per_workflow=1):
        super().__init__(name, destinations, workflows, runs_per_workflow)
        self.destinations = destinations
        self.workflows = workflows

    def run(self, benchmarker):
        # TODO: Run Pre-Task
        self.benchmark_results["warm"] = run_galaxy_benchmark(self, benchmarker.glx, self.destinations, self.workflows,
                                                              self.runs_per_workflow, "warm")
        # TODO: Run Post-Task


class BurstBenchmark(BaseBenchmark):
    allowed_dest_types = [CondorDestination]
    allowed_workflow_types = [CondorWorkflow]

    def __init__(self, name, destinations: List[BaseDestination],
                 workflows: List[BaseWorkflow], runs_per_workflow=1, burst_rate=0):
        super().__init__(name, destinations, workflows, runs_per_workflow)
        self.burst_rate = burst_rate

    def run(self, benchmarker):
        # TODO: Make sure that workflows are uploaded to Condor-Manager

        # TODO: Run burst_rate workflow at the same time until runs_per_workflows ran
        raise NotImplementedError


def run_galaxy_benchmark(benchmark, galaxy, destinations: List[PulsarMQDestination],
                         workflows: List[GalaxyWorkflow], runs_per_workflow=1, run_type="warm"):
    if run_type not in ["cold", "warm"]:
        raise ValueError("'run_type' must be either 'cold' or 'warm'")

    benchmark_results = dict()

    # Add +1 to warm up Pulsar, if run_type is "warm"
    if run_type == "warm":
        runs_per_workflow += 1

    log.info("Starting to run {type} benchmarks".format(type=run_type))
    for destination in destinations:
        benchmark_results[destination.name] = dict()

        log.info("Running {type} benchmark for destination: {dest}".format(type=run_type, dest=destination.name))
        for workflow in workflows:
            benchmark_results[destination.name][workflow.name] = list()
            for i in range(0, runs_per_workflow):
                if run_type == "warm" and i == 0:
                    log.info("First run! Warming up. Results won't be considered for the first time")
                    workflow.run(destination, galaxy)
                else:
                    if run_type == "cold" and benchmark.cold_pre_task is not None:
                        log.info("Running cold pre-task for Cleanup")
                        destination.run_task(benchmark.cold_pre_task)

                    log.info("Running {type} '{workflow}' for the {i} time".format(type=run_type,
                                                                                   workflow=workflow.name, i=i + 1))
                    start_time = time.monotonic()
                    result = workflow.run(destination, galaxy)
                    result["run_time"] = time.monotonic() - start_time
                    result["jobs"] = destination.get_jobs(result["history_name"])
                    result["metrics_summary"] = destination.get_job_metrics_summary(result["jobs"])

                    log.info("Finished running '{workflow}' with status '{status}' in {time} seconds."
                             .format(workflow=workflow.name, status=result["status"], time=result["run_time"]))
                    benchmark_results[destination.name][workflow.name].append(result)

    return benchmark_results


def configure_benchmark(bm_config: Dict, destinations: Dict, workflows: Dict, glx):
    if bm_config["type"] not in ["ColdvsWarm", "DestinationComparison", "Burst"]:
        raise ValueError("Benchmark-Type '{type}' not valid".format(type=bm_config["type"]))

    runs_per_workflow = bm_config["runs_per_workflow"] if "runs_per_workflow" in bm_config else 1

    if bm_config["type"] == "ColdvsWarm":
        benchmark = ColdWarmBenchmark(bm_config["name"],
                                      get_needed_destinations(bm_config, destinations, ColdWarmBenchmark),
                                      get_needed_workflows(bm_config, workflows, ColdWarmBenchmark), runs_per_workflow)
        benchmark.galaxy = glx
        if "cold_pre_task" in bm_config:
            benchmark.cold_pre_task = AnsiblePlaybookTask(bm_config["cold_pre_task"]["playbook"])

        if "warm_pre_task" in bm_config:
            benchmark.warm_pre_task = AnsiblePlaybookTask(bm_config["warm_pre_task"]["playbook"])

    if bm_config["type"] == "DestinationComparison":
        benchmark = DestinationComparisonBenchmark(bm_config["name"],
                                                   get_needed_destinations(bm_config, destinations,
                                                                           DestinationComparisonBenchmark),
                                                   get_needed_workflows(bm_config, workflows,
                                                                        DestinationComparisonBenchmark),
                                                   runs_per_workflow)
        benchmark.galaxy = glx

    if bm_config["type"] == "Burst":
        benchmark = BurstBenchmark(bm_config["name"], get_needed_destinations(bm_config, destinations, BurstBenchmark),
                                   get_needed_workflows(bm_config, workflows, BurstBenchmark),
                                   runs_per_workflow)
        benchmark.galaxy = glx

    return benchmark


def get_needed_destinations(bm_config: Dict, destinations: Dict, bm_type):
    if "destinations" not in bm_config or len(bm_config["destinations"]) == 0:
        return list()

    needed_destinations = list()
    for dest_name in bm_config["destinations"]:
        # Make sure, that destination-type is allowed
        if type(destinations[dest_name]) not in bm_type.allowed_dest_types:
            raise ValueError("Destination-Type {dest} is not allowed in benchmark-type {bm}. \
                             Error in benchmark name {bm_name}".format(dest=type(destinations[dest_name]),
                                                                       bm=type(bm_type), bm_name=bm_config["name"]))
        needed_destinations.append(destinations[dest_name])

    return needed_destinations


def get_needed_workflows(bm_config: Dict, workflows: Dict, bm_type):
    if "workflows" not in bm_config or len(bm_config["workflows"]) == 0:
        return list()

    needed_workflows = list()
    for wf_name in bm_config["workflows"]:
        # Make sure, that workflow-type is allowed
        if type(workflows[wf_name]) not in bm_type.allowed_workflow_types:
            raise ValueError("Workflow-Type {wf} is not allowed in benchmark-type {bm}. \
                                         Error in benchmark name {bm_name}".format(wf=type(workflows[wf_name]),
                                                                                   bm=type(bm_type),
                                                                                   bm_name=bm_config["name"]))
        needed_workflows.append(workflows[wf_name])

    return needed_workflows
