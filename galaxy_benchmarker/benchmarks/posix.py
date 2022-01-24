"""
Definition of different benchmark-types.
"""
from __future__ import annotations
from typing import TYPE_CHECKING
import logging
import time
from galaxy_benchmarker.models import task
from galaxy_benchmarker.bridge import influxdb, ansible
from galaxy_benchmarker.benchmarks import base

if TYPE_CHECKING:
    from galaxy_benchmarker.benchmarker import Benchmarker

log = logging.getLogger(__name__)


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

        self.pre_tasks.append(ansible.AnsibleTask("prepare_posix_fio_benchmark", {
            "playbook": "prepare_posix_fio_benchmark.yml",
            "destinations": config.get("destinations")
        }))


    def run(self):
        """
        Runs the Workflow on each Destinations. First runs all Workflows "cold" (cleaning Pulsar up before each run),
        after that the "warm"-runs begin.
        """

    def save_results_to_influxdb(self, inflxdb: influxdb.InfluxDb):
        """
        Sends all the metrics of the benchmark_results to influxDB.
        """
