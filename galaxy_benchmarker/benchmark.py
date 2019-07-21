"""
Definition of different benchmark-types.
"""
import logging
import time
import threading
import random
from datetime import datetime
from destination import BaseDestination, GalaxyDestination, PulsarMQDestination, GalaxyCondorDestination, CondorDestination
from workflow import BaseWorkflow, GalaxyWorkflow, CondorWorkflow
from task import BaseTask, AnsiblePlaybookTask, BenchmarkerTask
from typing import List, Dict, Union
from influxdb_bridge import InfluxDB
import planemo_bridge
from bioblend import ConnectionError


log = logging.getLogger("GalaxyBenchmarker")


class BaseBenchmark:
    """
    The Base-Class of Benchmark. All Benchmarks should inherit from it.
    """
    allowed_dest_types = []
    allowed_workflow_types = []
    galaxy = None
    benchmark_results = dict()
    pre_tasks: List[BaseTask] = None
    post_tasks: List[BaseTask] = None

    def __init__(self, name, destinations: List[BaseDestination],
                 workflows: List[BaseWorkflow], runs_per_workflow=1):
        self.name = name
        self.uuid = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        self.destinations = destinations
        self.workflows = workflows
        self.runs_per_workflow = runs_per_workflow

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

    def run(self, benchmarker):
        raise NotImplementedError

    def save_results_to_influxdb(self, inflxdb: InfluxDB):
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


class ColdWarmBenchmark(BaseBenchmark):
    allowed_dest_types = [GalaxyDestination, PulsarMQDestination]
    allowed_workflow_types = [GalaxyWorkflow]
    cold_pre_task: AnsiblePlaybookTask = None
    warm_pre_task: AnsiblePlaybookTask = None

    def __init__(self, name, destinations: List[Union[PulsarMQDestination, GalaxyDestination]],
                 workflows: List[GalaxyWorkflow], galaxy, runs_per_workflow=1):
        super().__init__(name, destinations, workflows, runs_per_workflow)
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
                self.benchmark_results[run_type] = run_galaxy_benchmark(self, benchmarker.glx, self.destinations,
                                                                        self.workflows,
                                                                        self.runs_per_workflow, run_type)
            except KeyboardInterrupt as e:
                self.benchmark_results[run_type] = e.args[0]
                break


class DestinationComparisonBenchmark(BaseBenchmark):
    allowed_dest_types = [GalaxyDestination, PulsarMQDestination, GalaxyCondorDestination]
    allowed_workflow_types = [GalaxyWorkflow]

    def __init__(self, name, destinations: List[Union[PulsarMQDestination, GalaxyDestination]],
                 workflows: List[GalaxyWorkflow], galaxy, runs_per_workflow=1, warmup=True):
        super().__init__(name, destinations, workflows, runs_per_workflow)
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
            self.benchmark_results["warm"] = run_galaxy_benchmark(self, benchmarker.glx, self.destinations,
                                                                  self.workflows,
                                                                  self.runs_per_workflow, "warm", self.warmup)
        except KeyboardInterrupt as e:
            self.benchmark_results["warm"] = e.args[0]


class BurstBenchmark(BaseBenchmark):
    allowed_dest_types = [GalaxyDestination, PulsarMQDestination, CondorDestination]
    allowed_workflow_types = [GalaxyWorkflow, CondorWorkflow]

    def __init__(self, name, destinations: List[BaseDestination],
                 workflows: List[BaseWorkflow], runs_per_workflow=1, burst_rate=1):
        super().__init__(name, destinations, workflows, runs_per_workflow)
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
        threads = []
        results = [None]*self.runs_per_workflow
        total_runs = next_runs = 0
        while total_runs < self.runs_per_workflow:
            next_runs += self.burst_rate
            # If burst_rate < 1, workflow should be run less than 1x per second. So just wait, until next_runs > 1
            if next_runs < 1:
                time.sleep(1)
                continue

            # Make sure, runs_per_workflow won't be exceeded
            if total_runs + next_runs >= self.runs_per_workflow:
                next_runs = self.runs_per_workflow - total_runs

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

        self.benchmark_results = {
            "warm": {
                self.destinations[0].name: {
                    self.workflows[0].name: results
                }
            }
        }

    class BurstThread(threading.Thread):
        """
        Class to run a Workflow within a thread to allow multiple runs a the same time.
        """
        def __init__(self, bm, thread_id, results: List):
            threading.Thread.__init__(self)
            self.bm = bm
            self.thread_id = thread_id
            self.results = results
            pass

        def run(self):
            """
            Runs a GalaxyWorkflow or a CondorWorkflow.
            """
            log.info("Running with thread_id {thread_id}".format(thread_id=self.thread_id))
            if self.bm.destination_type is PulsarMQDestination:
                try:
                    res = run_galaxy_benchmark(self, self.bm.galaxy, self.bm.destinations, self.bm.workflows,
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


def run_galaxy_benchmark(benchmark, galaxy, destinations: List[PulsarMQDestination],
                         workflows: List[GalaxyWorkflow], runs_per_workflow=1, run_type="warm", warmup=True):
    """
    Runs the given list of Workflows on the given list of Destinations as a cold or warm benchmark on a
    PulsarMQDestination for runs_per_workflow times. Handles failures too and retries up to one time.
    """
    if run_type not in ["cold", "warm"]:
        raise ValueError("'run_type' must be of type 'cold' or 'warm'.")

    benchmark_results = dict()

    # Add +1 to warm up Pulsar, if run_type is "warm" and warmup should happen
    if warmup and run_type == "warm":
        runs_per_workflow += 1

    log.info("Starting to run {type} benchmarks.".format(type=run_type))
    try:
        for destination in destinations:
            benchmark_results[destination.name] = dict()

            log.info("Running {type} benchmark for destination: {dest}.".format(type=run_type, dest=destination.name))
            for workflow in workflows:
                benchmark_results[destination.name][workflow.name] = list()
                retries = 0
                i = 0
                while i < runs_per_workflow:
                    if warmup and run_type == "warm" and i == 0:
                        log.info("First run! Warming up. Results won't be considered for the first time.")
                        result = destination.run_workflow(workflow)
                        if result["status"] == "error":
                            retries += 1
                    else:
                        if run_type == "cold" and benchmark.cold_pre_task is not None:
                            log.info("Running cold pre-task for Cleanup.")
                            destination.run_task(benchmark.cold_pre_task)

                        log.info("Running {type} '{workflow}' for the {i} time on {dest}.".format(type=run_type,
                                                                                                  workflow=workflow.name,
                                                                                                  i=i + 1,
                                                                                                  dest=destination.name))
                        result = destination.run_workflow(workflow)

                        if "history_name" in result and result["status"] == "success":
                            result["jobs"] = destination.get_jobs(result["history_name"])

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

                        log.info("Finished running '{workflow}' with status '{status}' in {time} seconds."
                                 .format(workflow=workflow.name, status=result["status"],
                                         time=result["total_workflow_runtime"]))

                        # Handle possible errors and maybe retry
                        if result["status"] == "error":
                            log.info("Result won't be considered.")

                            if retries < 2:
                                retry_wait = 60 * 2 ** retries
                                log.info("Retrying after {wait} seconds..".format(wait=retry_wait))
                                time.sleep(retry_wait)
                                retries += 1
                                i -= 1
                            # If too many retries, continue with next workflow
                            else:
                                break
                        else:
                            benchmark_results[destination.name][workflow.name].append(result)
                            retries = 0

                    i += 1
    except KeyboardInterrupt:
        log.info("Received KeyboardInterrupt. Stopping benchmark and saving current results.")
        # So previous results are saved
        raise KeyboardInterrupt(benchmark_results)

    return benchmark_results


def configure_benchmark(bm_config: Dict, destinations: Dict, workflows: Dict, glx) -> BaseBenchmark:
    """
    Initializes and configures a Benchmark according to the given configuration. Returns the configured Benchmark.
    """
    # Check, if all set properly
    if bm_config["type"] not in ["ColdvsWarm", "DestinationComparison", "Burst"]:
        raise ValueError("Benchmark-Type '{type}' not valid".format(type=bm_config["type"]))

    runs_per_workflow = bm_config["runs_per_workflow"] if "runs_per_workflow" in bm_config else 1

    if bm_config["type"] == "ColdvsWarm":
        benchmark = ColdWarmBenchmark(bm_config["name"],
                                      _get_needed_destinations(bm_config, destinations, ColdWarmBenchmark),
                                      _get_needed_workflows(bm_config, workflows, ColdWarmBenchmark), glx,
                                      runs_per_workflow)
        benchmark.galaxy = glx
        if "cold_pre_task" in bm_config:
            if bm_config["cold_pre_task"]["type"] == "AnsiblePlaybook":
                benchmark.cold_pre_task = AnsiblePlaybookTask(benchmark, bm_config["cold_pre_task"]["playbook"])

        if "warm_pre_task" in bm_config:
            if bm_config["warm_pre_task"]["type"] == "AnsiblePlaybook":
                benchmark.warm_pre_task = AnsiblePlaybookTask(benchmark, bm_config["warm_pre_task"]["playbook"])

    if bm_config["type"] == "DestinationComparison":
        warmup = True if "warmup" not in bm_config else bm_config["warmup"]
        benchmark = DestinationComparisonBenchmark(bm_config["name"],
                                                   _get_needed_destinations(bm_config, destinations,
                                                                            DestinationComparisonBenchmark),
                                                   _get_needed_workflows(bm_config, workflows,
                                                                         DestinationComparisonBenchmark),
                                                   glx, runs_per_workflow, warmup)

    if bm_config["type"] == "Burst":
        benchmark = BurstBenchmark(bm_config["name"], _get_needed_destinations(bm_config, destinations, BurstBenchmark),
                                   _get_needed_workflows(bm_config, workflows, BurstBenchmark),
                                   runs_per_workflow, bm_config["burst_rate"])
        benchmark.galaxy = glx

    if "pre_tasks" in bm_config:
        benchmark.pre_tasks = list()
        for task in bm_config["pre_tasks"]:
            if task["type"] == "AnsiblePlaybook":
                benchmark.pre_tasks.append(AnsiblePlaybookTask(benchmark, task["playbook"]))
            elif task["type"] == "BenchmarkerTask":
                benchmark.pre_tasks.append(BenchmarkerTask(benchmark, task["name"]))
            else:
                raise ValueError("Task of type '{type}' is not supported".format(type=task["type"]))

    if "post_tasks" in bm_config:
        benchmark.post_tasks = list()
        for task in bm_config["post_tasks"]:
            if task["type"] == "AnsiblePlaybook":
                benchmark.post_tasks.append(AnsiblePlaybookTask(benchmark, task["playbook"]))
            elif task["type"] == "BenchmarkerTask":
                benchmark.post_tasks.append(BenchmarkerTask(benchmark, task["name"]))
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
