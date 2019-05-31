"""
Definition of different benchmark-types.
"""
import logging
from destination import BaseDestination, PulsarMQDestination
from workflow import BaseWorkflow, GalaxyWorkflow
from typing import List

log = logging.getLogger(__name__)


class BaseBenchmark:
    def __init__(self, name, destinations: List[BaseDestination],
                 workflows: List[BaseWorkflow], runs_per_workflow=1):
        self.name = name
        self.destinations = destinations
        self.workflows = workflows
        self.runs_per_workflow = runs_per_workflow


class ColdWarmBenchmark(BaseBenchmark):
    job_ids: list

    def __init__(self, name, destinations: List[PulsarMQDestination],
                 workflows: List[GalaxyWorkflow], runs_per_workflow=1):
        super().__init__(name, destinations, workflows, runs_per_workflow)
        self.destinations = destinations
        self.workflows = workflows

    def run(self):
        # TODO: Pre Task

        for run_type in ["cold", "warm"]:
            run_galaxy_benchmark(self.destinations, self.workflows, self.runs_per_workflow, run_type)

        # TODO: Post Task


class DestinationComparisonBenchmark(BaseBenchmark):
    def __init__(self, name):
        super().__init__(name)
        self.destinations = destinations
        self.workflows = workflows

    def run(self):
        run_galaxy_benchmark(self.destinations, self.workflows, self.runs_per_workflow, "warm")


class BurstBenchmark(BaseBenchmark):
    def __init__(self, name):

        super().__init__(name)


def run_galaxy_benchmark(destinations: List[PulsarMQDestination],
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
                    workflow.run()
                else:
                    log.info("Running {workflow} for the {i}th time".format(workflow=workflow.name, i=i+1))
                    workflow.run()
                if run_type == "cold":
                    log.info("Cleaning up")
                    # TODO: Clean Up
