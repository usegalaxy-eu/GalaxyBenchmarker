"""
Definition of different benchmark-types.
"""
import logging
import time
import threading
import random
from datetime import datetime
from destination import BaseDestination, PulsarMQDestination, CondorDestination
from workflow import BaseWorkflow, GalaxyWorkflow, CondorWorkflow
from task import BaseTask, AnsiblePlaybookTask
from typing import List, Dict
from influxdb_bridge import InfluxDB
from bioblend import ConnectionError


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

    def run_pre_task(self):
        pass

    def run_post_task(self):
        pass

    def run(self, benchmarker):
        raise NotImplementedError

    def save_results_to_influxdb(self, inflxdb: InfluxDB):
        for run_type, per_dest_results in self.benchmark_results.items():
            for dest_name, workflows in per_dest_results.items():
                for workflow_name, runs in workflows.items():
                    for run in runs:
                        if run is None:
                            continue
                        # Save metrics per workflow-run
                        tags = {
                            "benchmark_name": self.name,
                            "benchmark_uuid": self.uuid,
                            "benchmark_type": type(self),
                            "destination_name": dest_name,
                            "workflow_name": workflow_name,
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

    def run_pre_task(self):
        if self.pre_task is not None:
            log.info("Running pre-task '{task}'.".format(task=self.pre_task))
            self.destinations[0].run_task(self.pre_task)

    def run_post_task(self):
        if self.post_task is not None:
            log.info("Running post-task '{task}'.".format(task=self.post_task))
            self.destinations[0].run_task(self.post_task)

    def run(self, benchmarker):
        for run_type in ["cold", "warm"]:
            self.benchmark_results[run_type] = run_galaxy_benchmark(self, benchmarker.glx, self.destinations,
                                                                    self.workflows,
                                                                    self.runs_per_workflow, run_type)


class DestinationComparisonBenchmark(BaseBenchmark):
    allowed_dest_types = [PulsarMQDestination]
    allowed_workflow_types = [GalaxyWorkflow]

    def __init__(self, name, destinations: List[PulsarMQDestination],
                 workflows: List[GalaxyWorkflow], runs_per_workflow=1):
        super().__init__(name, destinations, workflows, runs_per_workflow)
        self.destinations = destinations
        self.workflows = workflows

    def run_pre_task(self):
        pass  # TODO

    def run_post_task(self):
        pass  # TODO

    def run(self, benchmarker):
        self.benchmark_results["warm"] = run_galaxy_benchmark(self, benchmarker.glx, self.destinations, self.workflows,
                                                              self.runs_per_workflow, "warm")


class BurstBenchmark(BaseBenchmark):
    allowed_dest_types = [PulsarMQDestination, CondorDestination]
    allowed_workflow_types = [GalaxyWorkflow, CondorWorkflow]

    class BurstThread(threading.Thread):
        def __init__(self, bm, thread_id, results: List):
            threading.Thread.__init__(self)
            self.bm = bm
            self.thread_id = thread_id
            self.results = results
            pass

        def run(self):
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
                result = self.bm.workflows[0].run(self.bm.destinations[0])
                result["history_name"] = str(time.time_ns()) + str(random.randrange(0, 99999))
                self.results[self.thread_id] = result

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

            if type(self.workflows[0]) is not CondorWorkflow:
                raise ValueError("CondorDestination can only work with CondorWorkflow!")

            self.workflows[0].deploy_to_condor_manager(self.destinations[0])

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


def run_galaxy_benchmark(benchmark, galaxy, destinations: List[PulsarMQDestination],
                         workflows: List[GalaxyWorkflow], runs_per_workflow=1, run_type="warm", warmup=True):
    if run_type not in ["cold", "warm"]:
        raise ValueError("'run_type' must be either 'cold' or 'warm'.")

    benchmark_results = dict()

    # Add +1 to warm up Pulsar, if run_type is "warm" and warmup should happen
    if warmup and run_type == "warm":
        runs_per_workflow += 1

    log.info("Starting to run {type} benchmarks.".format(type=run_type))
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
                    workflow.run(destination, galaxy)
                else:
                    if run_type == "cold" and benchmark.cold_pre_task is not None:
                        log.info("Running cold pre-task for Cleanup.")
                        destination.run_task(benchmark.cold_pre_task)

                    log.info("Running {type} '{workflow}' for the {i} time on {dest}.".format(type=run_type,
                                                                                              workflow=workflow.name,
                                                                                              i=i + 1,
                                                                                              dest=destination.name))
                    start_time = time.monotonic()
                    result = workflow.run(destination, galaxy)
                    total_runtime = time.monotonic() - start_time

                    result["jobs"] = destination.get_jobs(result["history_name"])

                    result["workflow_metrics"] = {
                        "status": {
                            "name": "status",
                            "type": "string",
                            "plugin": "benchmarker",
                            "value": result["status"]
                        },
                        "total_runtime": {
                            "name": "total_workflow_runtime",
                            "type": "float",
                            "plugin": "benchmarker",
                            "value": total_runtime
                        }
                    }

                    log.info("Finished running '{workflow}' with status '{status}' in {time} seconds."
                             .format(workflow=workflow.name, status=result["status"], time=total_runtime))

                    # Handle possible errors and maybe retry
                    if result["status"] == "error":
                        log.info("Result won't be considered.")
                        if retries < 2:
                            log.info("Retrying..")
                            retries += 1
                            i -= 1
                    else:
                        benchmark_results[destination.name][workflow.name].append(result)
                        retries = 0

                i += 1

    return benchmark_results


def configure_benchmark(bm_config: Dict, destinations: Dict, workflows: Dict, glx):
    if bm_config["type"] not in ["ColdvsWarm", "DestinationComparison", "Burst"]:
        raise ValueError("Benchmark-Type '{type}' not valid".format(type=bm_config["type"]))

    runs_per_workflow = bm_config["runs_per_workflow"] if "runs_per_workflow" in bm_config else 1

    if bm_config["type"] == "ColdvsWarm":
        benchmark = ColdWarmBenchmark(bm_config["name"],
                                      _get_needed_destinations(bm_config, destinations, ColdWarmBenchmark),
                                      _get_needed_workflows(bm_config, workflows, ColdWarmBenchmark), runs_per_workflow)
        benchmark.galaxy = glx
        if "cold_pre_task" in bm_config:
            benchmark.cold_pre_task = AnsiblePlaybookTask(bm_config["cold_pre_task"]["playbook"])

        if "warm_pre_task" in bm_config:
            benchmark.warm_pre_task = AnsiblePlaybookTask(bm_config["warm_pre_task"]["playbook"])
        if "pre_task" in bm_config:
            benchmark.pre_task = AnsiblePlaybookTask(bm_config["pre_task"]["playbook"])
        if "post_task" in bm_config:
            benchmark.post_task = AnsiblePlaybookTask(bm_config["post_task"]["playbook"])

    if bm_config["type"] == "DestinationComparison":
        benchmark = DestinationComparisonBenchmark(bm_config["name"],
                                                   _get_needed_destinations(bm_config, destinations,
                                                                            DestinationComparisonBenchmark),
                                                   _get_needed_workflows(bm_config, workflows,
                                                                         DestinationComparisonBenchmark),
                                                   runs_per_workflow)
        benchmark.galaxy = glx

    if bm_config["type"] == "Burst":
        benchmark = BurstBenchmark(bm_config["name"], _get_needed_destinations(bm_config, destinations, BurstBenchmark),
                                   _get_needed_workflows(bm_config, workflows, BurstBenchmark),
                                   runs_per_workflow, bm_config["burst_rate"])
        benchmark.galaxy = glx

    return benchmark


def _get_needed_destinations(bm_config: Dict, destinations: Dict, bm_type) -> List:
    """
    Returns a list of the destinations that were set in the configuration of the benchmark.
    """
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


def _get_needed_workflows(bm_config: Dict, workflows: Dict, bm_type) -> List:
    """
    Returns a list of the workflows that were set in the configuration of the benchmark.
    """
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
