"""
Definition of different benchmark-types.
"""
from __future__ import annotations
from typing import TYPE_CHECKING
import logging
import time
from galaxy_benchmarker.bridge import influxdb, ansible
from galaxy_benchmarker.benchmarks import base

if TYPE_CHECKING:
    from galaxy_benchmarker.benchmarker import Benchmarker

log = logging.getLogger(__name__)


@base.register_benchmark
class PosixSetupTimeBenchmark(base.Benchmark):
    """Compare the setuptime/ansible connection time between different destinations"""

    def __init__(self, name: str, config: dict, benchmarker: Benchmarker):
        super().__init__(name, config, benchmarker)

        self.destinations: list[ansible.AnsibleDestination] = []
        for item in config.get("destinations", []):
            self.destinations.append(ansible.AnsibleDestination(**item))

        if not self.destinations:
            raise ValueError(f"At least one destination is required for benchmark {self.__class__.__name__}")

        self._run_task = ansible.AnsibleTask({
            "playbook": "connection_test.yml",
            "destinations": config.get("destinations")
        })


    def run(self):
        """Runs the connection_test playbook on each destinations."""

        for dest in self.destinations:
            log.info("Start %s for %s", self.name, dest.host)
            results = []
            for i in range(self.repetitions):
                log.info("Run %d of %d", i+1, self.repetitions)
                start_time = time.monotonic()

                self._run_task.run_at(dest)

                total_runtime = time.monotonic() - start_time
                results.append(total_runtime)
            self.benchmark_results[dest.host] = results

    def save_results_to_influxdb(self, inflxdb: influxdb.InfluxDb):
        """
        Sends all the metrics of the benchmark_results to influxDB.
        """
        tags = self.get_influxdb_tags()

        for hostname, results in self.benchmark_results.items():
            scoped_tags = {
                **tags,
                "host": hostname
            }

            inflxdb.save_metric(scoped_tags, "total_runtime", results)


    def get_influxdb_tags(self) -> dict:
        return super().get_influxdb_tags()



@base.register_benchmark
class PosixFioBenchmark(base.Benchmark):
    """Compare the runtime between a cold and a warm start"""

    def __init__(self, name: str, config: dict, benchmarker: Benchmarker):
        super().__init__(name, config, benchmarker)

        self.destinations: list[ansible.AnsibleDestination] = []
        for item in config.get("destinations", []):
            self.destinations.append(ansible.AnsibleDestination(**item))

        if not self.destinations:
            raise ValueError(f"At least one destination is required for benchmark {self.__class__.__name__}")

        self._pre_task = ansible.AnsibleTask({
            "playbook": "prepare_posix_fio_benchmark.yml",
            "destinations": config.get("destinations")
        })


    def run_pre_tasks(self):
        self._pre_task.run()

    def run(self):
        """
        Runs the Workflow on each Destinations. First runs all Workflows "cold" (cleaning Pulsar up before each run),
        after that the "warm"-runs begin.
        """

    def save_results_to_influxdb(self, inflxdb: influxdb.InfluxDb):
        """
        Sends all the metrics of the benchmark_results to influxDB.
        """
