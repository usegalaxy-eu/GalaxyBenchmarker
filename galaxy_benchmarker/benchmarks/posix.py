"""
Definition of different benchmark-types.
"""
from __future__ import annotations

import logging
import tempfile
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

from galaxy_benchmarker.benchmarks import base
from galaxy_benchmarker.bridge import ansible, influxdb
from galaxy_benchmarker.utils import fio

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

        self._run_task = ansible.AnsibleTask({"playbook": "connection_test.yml"})


    def run(self):
        """Run the connection_test playbook on each destination"""

        for dest in self.destinations:
            log.info("Start %s for %s", self.name, dest.host)
            results = []
            for i in range(self.repetitions):
                log.info("Run %d of %d", i+1, self.repetitions)
                start_time = time.monotonic()

                self._run_task.run_at(dest)

                total_runtime = time.monotonic() - start_time
                results.append({
                    "runtime_in_s": total_runtime
                })
            self.benchmark_results[dest.host] = results

    def save_results_to_influxdb(self, inflxdb: influxdb.InfluxDb):
        """Send the runtime to influxDB."""
        tags = self.get_influxdb_tags()

        for hostname, results in self.benchmark_results.items():
            scoped_tags = {
                **tags,
                "host": hostname
            }

            inflxdb.save_measurement(
                scoped_tags,
                self.name,
                results
            )


@dataclass
class PosixBenchmarkDestination(ansible.AnsibleDestination):
    filesystem_type: str = ""
    target_folder: str = ""

    def __post_init__(self):
        if not self.filesystem_type:
            raise ValueError(f"Property 'filesystem_type' is missing for host '{self.host}'")

        if not self.target_folder:
            raise ValueError(f"Property 'target_folder' is missing for host '{self.host}'")



@base.register_benchmark
class PosixFioBenchmark(base.Benchmark):
    """Compare different posix compatible mounts with the benchmarking tool 'fio'"""

    def __init__(self, name: str, config: dict, benchmarker: Benchmarker):
        super().__init__(name, config, benchmarker)

        self.destinations: list[PosixBenchmarkDestination] = []
        for item in config.get("destinations", []):
            self.destinations.append(PosixBenchmarkDestination(**item))

        if not self.destinations:
            raise ValueError(f"At least one destination is required for benchmark {self.__class__.__name__}")

        # TODO: Fix
        # self._pre_task = ansible.AnsibleTask({
        #     "playbook": "posix_fio_benchmark_check.yml",
        #     "destinations": config.get("destinations")
        # })

        self._run_task = ansible.AnsibleTask({"playbook": "posix_fio_benchmark_run.yml"})


    def run_pre_tasks(self):
        # self._pre_task.run()
        pass

    def run(self):
        """Run 'fio' on each destination"""

        with tempfile.TemporaryDirectory() as temp_dir:
            for dest in self.destinations:
                log.info("Start %s for %s", self.name, dest.host)
                results = []
                for i in range(self.repetitions):
                    log.info("Run %d of %d", i+1, self.repetitions)
                    start_time = time.monotonic()

                    result_file = f"{self.name}_{dest.host}_{i}.json"

                    self._run_task.run_at(dest, {
                        "fio_dir": dest.target_folder,
                        "fio_result_file": result_file,
                        "controller_dir": temp_dir
                    })

                    total_runtime = time.monotonic() - start_time

                    result = fio.parse_result_file(temp_dir, result_file, "IOPS-read")
                    result["runtime_in_s"] = total_runtime
                    results.append(result)
                self.benchmark_results[dest.host] = results

    def save_results_to_influxdb(self, inflxdb: influxdb.InfluxDb):
        """Send the runtime to influxDB."""
        tags = self.get_influxdb_tags()

        for hostname, results in self.benchmark_results.items():
            scoped_tags = {
                **tags,
                "host": hostname
            }

            inflxdb.save_measurement(
                scoped_tags,
                self.name,
                results
            )
