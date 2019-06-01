"""
Definition of different benchmark-types.
"""
import logging
from destination import BaseDestination, PulsarMQDestination
from workflow import BaseWorkflow, GalaxyWorkflow
from typing import List, Dict

log = logging.getLogger(__name__)


class BaseBenchmark:
    allowed_dest_types = []
    allowed_workflow_types = []
    galaxy = None

    def __init__(self, name, destinations: List[BaseDestination],
                 workflows: List[BaseWorkflow], runs_per_workflow=1):
        self.name = name
        self.destinations = destinations
        self.workflows = workflows
        self.runs_per_workflow = runs_per_workflow

    def run(self, benchmarker):
        raise NotImplementedError


class ColdWarmBenchmark(BaseBenchmark):
    allowed_dest_types = [PulsarMQDestination]
    allowed_workflow_types = [GalaxyWorkflow]

    def __init__(self, name, destinations: List[PulsarMQDestination],
                 workflows: List[GalaxyWorkflow], runs_per_workflow=1):
        super().__init__(name, destinations, workflows, runs_per_workflow)
        self.destinations = destinations
        self.workflows = workflows

    def run(self, benchmarker):
        # TODO: Pre Task

        for run_type in ["cold", "warm"]:
            run_galaxy_benchmark(benchmarker.glx, self.destinations, self.workflows, self.runs_per_workflow, run_type)

        # TODO: Post Task


class DestinationComparisonBenchmark(BaseBenchmark):
    allowed_dest_types = [PulsarMQDestination]
    allowed_workflow_types = [GalaxyWorkflow]

    def __init__(self, name, destinations: List[PulsarMQDestination],
                 workflows: List[GalaxyWorkflow], runs_per_workflow=1):
        super().__init__(name, destinations, workflows, runs_per_workflow)
        self.destinations = destinations
        self.workflows = workflows

    def run(self, benchmarker):
        run_galaxy_benchmark(benchmarker.glx, self.destinations, self.workflows, self.runs_per_workflow, "warm")


class BurstBenchmark(BaseBenchmark):
    def __init__(self, name, destinations: List[BaseDestination],
                 workflows: List[BaseWorkflow], runs_per_workflow=1):

        super().__init__(name, destinations, workflows, runs_per_workflow)

    def run(self, benchmarker):
        raise NotImplementedError


def run_galaxy_benchmark(galaxy, destinations: List[PulsarMQDestination],
                         workflows: List[GalaxyWorkflow], runs_per_workflow=1, run_type="warm"):
    if run_type not in ["cold", "warm"]:
        raise ValueError("run_type must be either 'cold' or 'warm'")

    log.info("Starting to run {type} benchmarks".format(type=run_type))
    for destination in destinations:
        log.info("Running {type} benchmark for destination: {dest}".format(type=run_type, dest=destination.name))
        for workflow in workflows:
            for i in range(0, runs_per_workflow + 1):
                if run_type == "warm" and i == 0:
                    log.info("First run! Warming up. Results won't be considered for the first time")
                    workflow.run(galaxy)
                else:
                    log.info("Running {workflow} for the {i}th time".format(workflow=workflow.name, i=i + 1))
                    workflow.run(galaxy)
                if run_type == "cold":
                    log.info("Cleaning up")
                    # TODO: Clean Up


def configure_benchmark(bm_config: Dict, destinations: Dict, workflows: Dict, glx):
    if bm_config["type"] not in ["ColdvsWarm", "DestinationComparison", "Burst"]:
        raise ValueError("Benchmark-Type '{type}' not valid".format(type=bm_config["type"]))

    runs_per_workflow = bm_config["runs_per_workflow"] if "runs_per_workflow" in bm_config else 1

    if bm_config["type"] == "ColdvsWarm":
        benchmark = ColdWarmBenchmark(bm_config["name"],
                                      get_needed_destinations(bm_config, destinations, ColdWarmBenchmark),
                                      get_needed_workflows(bm_config, workflows, ColdWarmBenchmark), runs_per_workflow)
        benchmark.galaxy = glx

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
