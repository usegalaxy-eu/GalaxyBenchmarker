"""
Definition of different benchmark-types.
"""
from __future__ import annotations

import dataclasses
import logging
import tempfile
import time
from collections import defaultdict
from pathlib import Path
from typing import TYPE_CHECKING

from galaxy_benchmarker.benchmarks import base
from galaxy_benchmarker.bridge import ansible, influxdb
from galaxy_benchmarker.utils import fio

if TYPE_CHECKING:
    from galaxy_benchmarker.benchmarker import Benchmarker

log = logging.getLogger(__name__)


@base.register_benchmark
class PosixSetupTimeBenchmark(base.Benchmark):
    """Compare the setuptime/ansible connection time between different destinations.

    Useful for basic setup tests and to check if everything is configured correctly
    """

    def __init__(self, name: str, config: dict, benchmarker: Benchmarker):
        super().__init__(name, config, benchmarker)

        self.destinations: list[ansible.AnsibleDestination] = []
        for item in config.get("destinations", []):
            self.destinations.append(ansible.AnsibleDestination(**item))

        if not self.destinations:
            raise ValueError(
                f"At least one destination is required for benchmark {self.__class__.__name__}"
            )

        self._run_task = ansible.AnsibleTask(playbook_name="connection_test.yml")

    def run(self):
        """Run the connection_test playbook on each destination"""

        for dest in self.destinations:
            log.info("Start %s for %s", self.name, dest.host)
            results = []
            for i in range(self.repetitions):
                log.info("Run %d of %d", i + 1, self.repetitions)
                start_time = time.monotonic()

                self._run_task.run_at(dest)

                total_runtime = time.monotonic() - start_time
                results.append({"runtime_in_s": total_runtime})
            self.benchmark_results[dest.host] = results

    def save_results_to_influxdb(self, inflxdb: influxdb.InfluxDb):
        """Send the runtime to influxDB."""
        tags = self.get_influxdb_tags()

        for hostname, results in self.benchmark_results.items():
            scoped_tags = {**tags, "host": hostname}

            inflxdb.save_measurement(scoped_tags, self.name, results)


@dataclasses.dataclass
class PosixBenchmarkDestination(ansible.AnsibleDestination):
    filesystem_type: str = ""
    target_folder: str = ""

    def __post_init__(self):
        if not self.filesystem_type:
            raise ValueError(
                f"Property 'filesystem_type' is missing for host '{self.host}'"
            )

        if not self.target_folder:
            raise ValueError(
                f"Property 'target_folder' is missing for host '{self.host}'"
            )


@dataclasses.dataclass
class FioConfig:
    mode: str
    jobname: str
    blocksize: str
    numjobs: int
    iodepth: int
    runtime_in_s: int
    filesize: str

    def items(self):
        return dataclasses.asdict(self).items()


class PosixFioBenchmark(base.Benchmark):
    """Compare different posix compatible mounts with the benchmarking tool 'fio'"""

    # Available FIO parameters
    fio_mode = ""
    fio_jobname = ""
    fio_blocksize = ""
    fio_numjobs = 0
    fio_iodepth = 0
    fio_runtime_in_s = 60
    fio_filesize = "5G"

    def __init__(self, name: str, config: dict, benchmarker: Benchmarker):
        super().__init__(name, config, benchmarker)

        self.fio_config = FioConfig(
            mode=self.fio_mode,
            jobname=self.fio_jobname,
            blocksize=self.fio_blocksize,
            numjobs=self.fio_numjobs,
            iodepth=self.fio_iodepth,
            runtime_in_s=self.fio_runtime_in_s,
            filesize=self.fio_filesize,
        )

        self.destinations: list[PosixBenchmarkDestination] = []
        for item in config.get("destinations", []):
            self.destinations.append(PosixBenchmarkDestination(**item))

        if not self.destinations:
            raise ValueError(
                f"At least one destination is required for benchmark {self.__class__.__name__}"
            )

        self._pre_task = ansible.AnsibleTask(
            playbook_name="posix_fio_benchmark_check.yml",
            destinations=self.destinations,
        )

        self._run_task = ansible.AnsibleTask(
            playbook_name="posix_fio_benchmark_run.yml"
        )

    def run_pre_tasks(self):
        self._pre_task.run()

    def run(self):
        """Run 'fio' on each destination"""

        with tempfile.TemporaryDirectory() as temp_dir:
            for dest in self.destinations:
                log.info("Start %s for %s", self.name, dest.host)
                self.benchmark_results[dest.host] = []
                for i in range(self.repetitions):
                    log.info("Run %d of %d", i + 1, self.repetitions)
                    result_file = Path(temp_dir) / f"{self.name}_{dest.host}_{i}.json"

                    result = self._run_at(result_file, dest, self.fio_config)
                    self.benchmark_results[dest.host].append(result)

    def _run_at(
        self, result_file: Path, dest: PosixBenchmarkDestination, fio_config: FioConfig
    ) -> dict:
        """Perform a single run"""

        start_time = time.monotonic()

        self._run_task.run_at(
            dest,
            {
                "fio_dir": dest.target_folder,
                "fio_result_file": result_file.name,
                "controller_dir": result_file.parent,
                **{f"fio_{key}": value for key, value in fio_config.items()},
            },
        )

        total_runtime = time.monotonic() - start_time

        result = fio.parse_result_file(result_file, fio_config.jobname)
        result["runtime_in_s"] = total_runtime
        log.info("Run took %d s", total_runtime)

        return result

    def save_results_to_influxdb(self, inflxdb: influxdb.InfluxDb):
        """Send the runtime to influxDB."""
        tags = self.get_influxdb_tags()

        for hostname, results in self.benchmark_results.items():
            scoped_tags = {**tags, "host": hostname}

            inflxdb.save_measurement(scoped_tags, self.name, results)

    def get_influxdb_tags(self) -> dict:
        tags = super().get_influxdb_tags()
        return {
            **tags,
            **{f"fio_{key}": value for key, value in self.fio_config.items()},
        }


@base.register_benchmark
class PosixFioThroughputReadBenchmark(PosixFioBenchmark):
    """Compare different posix compatible mounts.

    Test read throughput / sequential reads
    """

    fio_mode = "read"
    fio_jobname = "ThroughputRead"
    fio_blocksize = "1024k"
    fio_numjobs = 4
    fio_iodepth = 32


@base.register_benchmark
class PosixFioThroughputWriteBenchmark(PosixFioBenchmark):
    """Compare different posix compatible mounts.

    Test write throughput / sequential writes
    """

    fio_mode = "write"
    fio_jobname = "ThroughputWrite"
    fio_blocksize = "1024k"
    fio_numjobs = 4
    fio_iodepth = 32


@base.register_benchmark
class PosixFioThroughputReadWriteBenchmark(PosixFioBenchmark):
    """Compare different posix compatible mounts.

    Test read-write throughput / sequential read-writes
    """

    fio_mode = "rw"
    fio_jobname = "ThroughputReadWrite"
    fio_blocksize = "1024k"
    fio_numjobs = 4
    fio_iodepth = 32


@base.register_benchmark
class PosixFioIopsReadBenchmark(PosixFioBenchmark):
    """Compare different posix compatible mounts.

    Test read IOPS / random reads
    """

    fio_mode = "randread"
    fio_jobname = "IopsRead"
    fio_blocksize = "4k"
    fio_numjobs = 4
    fio_iodepth = 32


@base.register_benchmark
class PosixFioIopsWriteBenchmark(PosixFioBenchmark):
    """Compare different posix compatible mounts.

    Test write IOPS / random writes
    """

    fio_mode = "randwrite"
    fio_jobname = "IopsWrite"
    fio_blocksize = "4k"
    fio_numjobs = 4
    fio_iodepth = 32


@base.register_benchmark
class PosixFioIopsReadWriteBenchmark(PosixFioBenchmark):
    """Compare different posix compatible mounts.

    Test read-write IOPS / random read-writes
    """

    fio_mode = "randrw"
    fio_jobname = "IopsReadWrite"
    fio_blocksize = "4k"
    fio_numjobs = 4
    fio_iodepth = 32


@base.register_benchmark
class PosixFioLatencyReadBenchmark(PosixFioBenchmark):
    """Compare different posix compatible mounts.

    Test read latency
    """

    fio_mode = "randread"
    fio_jobname = "LatencyRead"
    fio_blocksize = "4k"
    fio_numjobs = 1
    fio_iodepth = 1


@base.register_benchmark
class PosixFioLatencyWriteBenchmark(PosixFioBenchmark):
    """Compare different posix compatible mounts.

    Test write latency
    """

    fio_mode = "randwrite"
    fio_jobname = "LatencyWrite"
    fio_blocksize = "4k"
    fio_numjobs = 1
    fio_iodepth = 1


@base.register_benchmark
class PosixFioLatencyReadWriteBenchmark(PosixFioBenchmark):
    """Compare different posix compatible mounts.

    Test read-write latency
    """

    fio_mode = "randrw"
    fio_jobname = "LatencyReadWrite"
    fio_blocksize = "4k"
    fio_numjobs = 1
    fio_iodepth = 1


@base.register_benchmark
class PosixFioIopsOverTime(PosixFioBenchmark):
    """Compare different posix compatible mounts.

    Test write latency
    """

    fio_mode = "randread"
    fio_jobname = "ReadIopsOverTime"
    fio_blocksize = "4k"
    fio_numjobs = 4
    fio_iodepth = 32
    fio_filesize = "512Mi"

    def _run_at(
        self, result_file: Path, dest: PosixBenchmarkDestination, fioConfig: FioConfig
    ) -> dict:
        """Perform multiple runs for a single destination"""

        dest_results = {}

        runtimes = [10, 30, 1 * 60, 2 * 60, 5 * 60, 1 * 600, 2 * 600]

        for runtime_in_s in runtimes:
            log.info("Run with runtime set to %d", runtime_in_s)

            current_config = dataclasses.replace(fioConfig, runtime_in_s=runtime_in_s)
            current_result_file = result_file.with_suffix(
                f".{runtime_in_s}{result_file.suffix}"
            )

            result = super()._run_at(current_result_file, dest, current_config)

            dest_results[runtime_in_s] = result

        return dest_results

    def save_results_to_influxdb(self, inflxdb: influxdb.InfluxDb):
        """Send the runtime to influxDB."""
        tags = self.get_influxdb_tags()

        for hostname, dest_results in self.benchmark_results.items():
            # Reorder results

            # We have:
            # benchmark_results = dict[dest -> list[dest_results]
            # dest_results = dict[runtime -> results]

            # We need:
            # list[results] for each runtime
            results_by_runtime = defaultdict(list)
            for dest_result in dest_results:
                for runtime, run_results in dest_result.items():
                    results_by_runtime[runtime].append(run_results)

            # Send results
            dest_tags = {**tags, "host": hostname}
            for runtime, results in results_by_runtime.items():
                scoped_tags = {
                    **dest_tags,
                    "fio_runtime_in_s": runtime,
                }

                inflxdb.save_measurement(scoped_tags, self.name, results)
