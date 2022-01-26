"""
Definition of different benchmark-types.
"""
import logging
import random
import threading
import time
from datetime import datetime
from typing import Dict, List, Union

from bioblend import ConnectionError

from galaxy_benchmarker.bridge import influxdb
from galaxy_benchmarker.models.destination import (
    BaseDestination,
    CondorDestination,
    GalaxyCondorDestination,
    GalaxyDestination,
    PulsarMQDestination,
)
from galaxy_benchmarker.models.task import (
    AnsiblePlaybookTask,
    BaseTask,
    BenchmarkerTask,
    configure_task,
)
from galaxy_benchmarker.models.workflow import (
    BaseWorkflow,
    CondorWorkflow,
    GalaxyWorkflow,
)

log = logging.getLogger(__name__)


class BaseBenchmark:
    """
    The Base-Class of Benchmark. All Benchmarks should inherit from it.
    """
    def save_results_to_influxdb(self, inflxdb: influxdb.InfluxDb):
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
                                "benchmark_id": self.id,
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
                                "benchmark_id": self.id,
                                "benchmark_type": type(self),
                                "destination_name": dest_name,
                                "workflow_name": workflow_name,
                                "history_name": run["history_name"] if "history_name" in run else None,
                                "run_type": run_type,
                            }
                            inflxdb.save_job_metrics(tags, job)



class ColdWarmBenchmark(BaseBenchmark):
    allowed_dest_types = [GalaxyDestination, PulsarMQDestination]
    allowed_workflow_types = [GalaxyWorkflow]
    cold_pre_task: AnsiblePlaybookTask = None
    warm_pre_task: AnsiblePlaybookTask = None

    def __init__(self, name, benchmarker, destinations: List[Union[PulsarMQDestination, GalaxyDestination]],
                 workflows: List[GalaxyWorkflow], galaxy, repetitions=1):
        super().__init__(name, benchmarker, destinations, workflows, repetitions)
        self.destinations = destinations
        self.workflows = workflows
        self.galaxy = galaxy

    def run(self, benchmarker):
        """
        Runs the Workflow on each Destinations. First runs all Workflows "cold" (cleaning Pulsar up before each run),
        after that the "warm"-runs begin.
        """

        for run_type in ["cold", "warm"]:
            try:
                self.benchmark_results[run_type] = self.run_galaxy_benchmark(self, benchmarker.glx, self.destinations,
                                                                        self.workflows,
                                                                        self.repetitions, run_type)
            except KeyboardInterrupt as e:
                self.benchmark_results[run_type] = e.args[0]
                break


class DestinationComparisonBenchmark(BaseBenchmark):
    allowed_dest_types = [GalaxyDestination, PulsarMQDestination, GalaxyCondorDestination]
    allowed_workflow_types = [GalaxyWorkflow]

    def __init__(self, name, benchmarker, destinations: List[Union[PulsarMQDestination, GalaxyDestination]],
                 workflows: List[GalaxyWorkflow], galaxy, repetitions=1, warmup=True):
        super().__init__(name, benchmarker, destinations, workflows, repetitions)
        self.destinations = destinations
        self.workflows = workflows
        self.galaxy = galaxy
        self.warmup = warmup

    def run(self, benchmarker):
        """
        Runs the Workflows on each Destination. Uses "warm"-run, so for each Destination and Workflow, each Workflow
        runs for the first time without consideration (so Pulsar has opportunity to install all tools) to not spoil
        any metrics.
        """
        try:
            self.benchmark_results["warm"] = self.run_galaxy_benchmark(self, benchmarker.glx, self.destinations,
                                                                  self.workflows,
                                                                  self.repetitions, "warm", self.warmup)
        except KeyboardInterrupt as e:
            self.benchmark_results["warm"] = e.args[0]


class BurstBenchmark(BaseBenchmark):
    allowed_dest_types = [GalaxyDestination, GalaxyCondorDestination, PulsarMQDestination, CondorDestination]
    allowed_workflow_types = [GalaxyWorkflow, CondorWorkflow]

    background_tasks: List[Dict] = list()

    def __init__(self, name, benchmarker, destinations: List[BaseDestination],
                 workflows: List[BaseWorkflow], repetitions=1, burst_rate=1):
        super().__init__(name, benchmarker, destinations, workflows, repetitions)
        self.burst_rate = burst_rate

        if len(self.destinations) != 1:
            raise ValueError("BurstBenchmark can only be used with exactly one Destination.")

        if len(self.workflows) != 1:
            raise ValueError("BurstBenchmark can only be used with exactly one Workflow.")

        self.destination_type = type(self.destinations[0])

        if self.destination_type is CondorDestination:
            self.workflows: List[CondorWorkflow]
            self.destinations: List[CondorDestination]

            # Deploy Workflow to Destination
            for destination in self.destinations:
                for workflow in self.workflows:
                    if type(workflow) is not CondorWorkflow:
                        raise ValueError("CondorDestination can only work with CondorWorkflow!")

                    destination.deploy_workflow(workflow)

    def run(self, benchmarker):
        background_task_process = self.BackgroundTaskThread(self)
        background_task_process.start()

        threads = []
        results = [None]*self.repetitions
        total_runs = next_runs = 0
        while total_runs < self.repetitions:
            next_runs += self.burst_rate
            # If burst_rate < 1, workflow should be run less than 1x per second. So just wait, until next_runs > 1
            if next_runs < 1:
                time.sleep(1)
                continue

            # Make sure, repetitions won't be exceeded
            if total_runs + next_runs >= self.repetitions:
                next_runs = self.repetitions - total_runs

            for _ in range(0, int(next_runs)):
                process = self.BurstThread(self, total_runs, results)
                process.start()
                threads.append(process)
                total_runs += 1

            next_runs = 0
            time.sleep(1)

        # Wait for all Benchmarks being executed
        finished_jobs = 0
        for process in threads:
            process.join()
            finished_jobs += 1
            log.info("{finished} out of {total} workflows are finished.".format(finished=finished_jobs,
                                                                                total=total_runs))
        background_task_process.stop = True

        self.benchmark_results = {
            "warm": {
                self.destinations[0].name: {
                    self.workflows[0].name: results
                }
            }
        }

    class BackgroundTaskThread(threading.Thread):
        stop = False

        def __init__(self, bm):
            threading.Thread.__init__(self)
            self.bm = bm

        def run(self):
            if len(self.bm.background_tasks) == 0:
                return

            log.info("Starting to run BackgroundTaskThread")
            for task in self.bm.background_tasks:
                task["next_run"] = time.monotonic() + task["first_run_after"]
                if "run_until" in task:
                    task["run_until"] += time.monotonic()

            while True:
                for task in self.bm.background_tasks:
                    if task["next_run"] <= time.monotonic():
                        log.info("Running background task {task}".format(task=task))
                        task["task"].run()
                        task["next_run"] = time.monotonic() + task["run_every"]
                    if "run_until" in task:
                        if task["run_until"] <= time.monotonic() and task["next_run"] < float("inf"):
                            task["next_run"] = float("inf")
                            log.info("Stopped background task {task}, as run_until passed".format(task=task))
                if self.stop:
                    break

                time.sleep(1)

    class BurstThread(threading.Thread):
        """
        Class to run a Workflow within a thread to allow multiple runs a the same time.
        """
        def __init__(self, bm, thread_id, results: List):
            threading.Thread.__init__(self)
            self.bm = bm
            self.thread_id = thread_id
            self.results = results

        def run(self):
            """
            Runs a GalaxyWorkflow or a CondorWorkflow.
            """
            log.info("Running with thread_id {thread_id}".format(thread_id=self.thread_id))
            if self.bm.destination_type is PulsarMQDestination:
                try:
                    res = BurstBenchmark.run_galaxy_benchmark(self, self.bm.galaxy, self.bm.destinations, self.bm.workflows,
                                               1, "warm", False)
                    self.results[self.thread_id] = res[self.bm.destinations[0].name][self.bm.workflows[0].name][0] # TODO: Handle error-responses
                except ConnectionError:
                    log.error("ConnectionError!")
                    self.results[self.thread_id] = {"status": "error"}

            if self.bm.destination_type is CondorDestination:
                for destination in self.bm.destinations:
                    for workflow in self.bm.workflows:
                        result = destination.run_workflow(workflow)
                        result["history_name"] = str(time.time_ns()) + str(random.randrange(0, 99999))
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
                            },
                            "submit_time": {
                                "name": "submit_time",
                                "type": "float",
                                "plugin": "benchmarker",
                                "value": result["submit_time"]
                            }
                        }

                self.results[self.thread_id] = result


def configure_benchmark(bm_config: Dict, destinations: Dict, workflows: Dict, glx, benchmarker) -> BaseBenchmark:
    """
    Initializes and configures a Benchmark according to the given configuration. Returns the configured Benchmark.
    """
    # Check, if all set properly
    if bm_config["type"] not in ["ColdvsWarm", "DestinationComparison", "Burst"]:
        raise ValueError("Benchmark-Type '{type}' not valid".format(type=bm_config["type"]))

    repetitions = bm_config["repetitions"] if "repetitions" in bm_config else 1

    if bm_config["type"] == "ColdvsWarm":
        benchmark = ColdWarmBenchmark(bm_config["name"], benchmarker,
                                      _get_needed_destinations(bm_config, destinations, ColdWarmBenchmark),
                                      _get_needed_workflows(bm_config, workflows, ColdWarmBenchmark), glx,
                                      repetitions)
        benchmark.galaxy = glx
        if "cold_pre_task" in bm_config:
            if bm_config["cold_pre_task"]["type"] == "AnsiblePlaybook":
                benchmark.cold_pre_task = AnsiblePlaybookTask(benchmark.destinations, bm_config["cold_pre_task"]["playbook"])

        if "warm_pre_task" in bm_config:
            if bm_config["warm_pre_task"]["type"] == "AnsiblePlaybook":
                benchmark.warm_pre_task = AnsiblePlaybookTask(benchmark.destinations, bm_config["warm_pre_task"]["playbook"])

    if bm_config["type"] == "DestinationComparison":
        warmup = True if "warmup" not in bm_config else bm_config["warmup"]
        benchmark = DestinationComparisonBenchmark(bm_config["name"], benchmarker,
                                                   _get_needed_destinations(bm_config, destinations,
                                                                            DestinationComparisonBenchmark),
                                                   _get_needed_workflows(bm_config, workflows,
                                                                         DestinationComparisonBenchmark),
                                                   glx, repetitions, warmup)

    if bm_config["type"] == "Burst":
        benchmark = BurstBenchmark(bm_config["name"], benchmarker,
                                   _get_needed_destinations(bm_config, destinations, BurstBenchmark),
                                   _get_needed_workflows(bm_config, workflows, BurstBenchmark),
                                   repetitions, bm_config["burst_rate"])
        benchmark.galaxy = glx

        if "background_tasks" in bm_config:
            benchmark.background_tasks = list()
            for task_conf in bm_config["background_tasks"]:
                task_conf["task"] = configure_task(task_conf, benchmark)
                benchmark.background_tasks.append(task_conf)

    if "pre_tasks" in bm_config:
        benchmark.pre_tasks = list()
        for task in bm_config["pre_tasks"]:
            if task["type"] == "AnsiblePlaybook":
                benchmark.pre_tasks.append(AnsiblePlaybookTask(benchmark.destinations, task["playbook"]))
            elif task["type"] == "BenchmarkerTask":
                benchmark.pre_tasks.append(BenchmarkerTask(benchmark.destinations, task["name"]))
            else:
                raise ValueError("Task of type '{type}' is not supported".format(type=task["type"]))

    if "post_tasks" in bm_config:
        benchmark.post_tasks = list()
        for task in bm_config["post_tasks"]:
            if task["type"] == "AnsiblePlaybook":
                benchmark.post_tasks.append(AnsiblePlaybookTask(benchmark.destinations, task["playbook"]))
            elif task["type"] == "BenchmarkerTask":
                benchmark.post_tasks.append(BenchmarkerTask(benchmark.destinations, task["name"]))
            else:
                raise ValueError("Task of type '{type}' is not supported".format(type=task["type"]))

    return benchmark


def _get_needed_destinations(bm_config: Dict, destinations: Dict, bm_type) -> List:
    """
    Returns a list of the destinations that were set in the configuration of the benchmark.
    """
    if "destinations" not in bm_config or bm_config["destinations"] is None or len(bm_config["destinations"]) == 0:
        raise ValueError("No destination set in benchmark '{name}'".format(name=bm_config["name"]))

    needed_destinations = list()
    for dest_name in bm_config["destinations"]:
        # Make sure, that destination exists
        if dest_name not in destinations:
            raise ValueError("Destination '{name}' not set in workflows-configuration.".format(name=dest_name))
        # Make sure, that destination-type is allowed
        if type(destinations[dest_name]) not in bm_type.allowed_dest_types:
            raise ValueError("Destination-Type {dest} is not allowed in benchmark-type {bm}. \
                             Error in benchmark name {bm_name}".format(dest=type(destinations[dest_name]),
                                                                       bm=type(bm_type), bm_name=bm_config["name"]))
        needed_destinations.append(destinations[dest_name])

    return needed_destinations


def _get_needed_workflows(bm_config: Dict, workflows: Dict, bm_type) -> List:
    """
    Returns a list of the workflows that were set in the configuration of the benchmark.
    """
    if "workflows" not in bm_config or bm_config["workflows"] is None or len(bm_config["workflows"]) == 0:
        raise ValueError("No workflow set in benchmark '{name}'".format(name=bm_config["name"]))

    needed_workflows = list()
    for wf_name in bm_config["workflows"]:
        # Make sure, that workflow exists
        if wf_name not in workflows:
            raise ValueError("Workflow '{name}' not set in workflows-configuration.".format(name=wf_name))
        # Make sure, that workflow-type is allowed
        if type(workflows[wf_name]) not in bm_type.allowed_workflow_types:
            raise ValueError("Workflow-Type {wf} is not allowed in benchmark-type {bm}. \
                                         Error in benchmark name {bm_name}".format(wf=type(workflows[wf_name]),
                                                                                   bm=type(bm_type),
                                                                                   bm_name=bm_config["name"]))
        needed_workflows.append(workflows[wf_name])

    return needed_workflows
