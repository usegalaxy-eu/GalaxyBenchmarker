"""
Definition of different benchmark-types.
"""
from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING

from galaxy_benchmarker.benchmarks import base
from galaxy_benchmarker.bridge import ansible, influxdb

if TYPE_CHECKING:
    from galaxy_benchmarker.benchmarker import Benchmarker

log = logging.getLogger(__name__)


@base.register_benchmark
class ColdWarmBenchmark(base.Benchmark):
    """Compare the runtime between a cold and a warm start"""

    def __init__(self, name: str, config: dict, benchmarker: Benchmarker):
        super().__init__(name, config, benchmarker)

        self.cold_pre_task = ansible.AnsibleTask.from_config(
            config.get("cold_pre_task", None), "cold_pre_task"
        )

        self.warm_pre_task = ansible.AnsibleTask.from_config(
            config.get("warm_pre_task", None), "warm_pre_task"
        )

        self.destinations: list[ansible.AnsibleDestination] = []  # TODO

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
                        self.benchmark_results[run_type][destination.name][
                            workflow.name
                        ] = list()
                        results = self.benchmark_results[run_type][destination.name][
                            workflow.name
                        ]

                        log.info(
                            "Up next: {type} run for workflow {workflow} for destination: {dest}.".format(
                                type=run_type,
                                workflow=workflow.name,
                                dest=destination.name,
                            )
                        )

                        # Cold runs
                        for i in range(self.repetitions):
                            log.debug(
                                f"({i+1}/{self.repetitions}): Running cold pre-task"
                            )
                            self.cold_pre_task.run_at(destination)

                            log.debug(f"({i+1}/{self.repetitions}): Running workflow")
                            result = self._run_workflow(
                                destination, workflow, max_retires=0
                            )
                            results.append(result)

                        # Warm runs
                        for i in range(self.repetitions):
                            log.debug(
                                f"({i+1}/{self.repetitions}): Running warm pre-task"
                            )
                            self.warm_pre_task.run_at(destination)

                            log.debug(f"({i+1}/{self.repetitions}): Running workflow")
                            result = self._run_workflow(destination, workflow)
                            results.append(result)

        except KeyboardInterrupt:
            log.info("Received KeyboardInterrupt. Stopping current benchmark")

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
                                "history_name": run["history_name"]
                                if "history_name" in run
                                else None,
                                "run_type": run_type,
                            }

                            inflxdb.save_workflow_metrics(tags, run["workflow_metrics"])

                        # Save job-metrics if workflow succeeded
                        if (
                            runs is None
                            or run["status"] == "error"
                            or "jobs" not in run
                            or run["jobs"] is None
                        ):
                            continue

                        for job in run["jobs"].values():
                            tags = {
                                "benchmark_name": self.name,
                                "benchmark_id": self.id,
                                "benchmark_type": type(self),
                                "destination_name": dest_name,
                                "workflow_name": workflow_name,
                                "history_name": run["history_name"]
                                if "history_name" in run
                                else None,
                                "run_type": run_type,
                            }
                            inflxdb.save_job_metrics(tags, job)

    def _run_benchmark(self, destination, workflow, max_retries=4):
        for current in range(max_retries + 1):
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
                    "value": result["status"],
                },
                "total_runtime": {
                    "name": "total_workflow_runtime",
                    "type": "float",
                    "plugin": "benchmarker",
                    "value": result["total_workflow_runtime"],
                },
            }

            if "history_name" in result:
                result["jobs"] = destination.get_jobs(result["history_name"])

            log.info(
                "Finished running '%s' with status '%s' in %d seconds.",
                workflow.name,
                result["status"],
                result["total_workflow_runtime"],
            )

            return result
        # In case of to many failures, return an empty result
        return {}
