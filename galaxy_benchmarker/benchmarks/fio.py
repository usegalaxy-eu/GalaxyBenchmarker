"""
Definition of fio-based benchmarks
"""
from __future__ import annotations

import dataclasses
import json
import logging
import tempfile
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

from galaxy_benchmarker.benchmarks import base
from galaxy_benchmarker.bridge import ansible
from galaxy_benchmarker.typing import RunResult
from galaxy_benchmarker.utils.posix import PosixBenchmarkDestination

if TYPE_CHECKING:
    from galaxy_benchmarker.benchmarker import Benchmarker

log = logging.getLogger(__name__)


@dataclasses.dataclass
class FioConfig(base.BenchmarkConfig):
    """Available parameters for fio"""

    blocksize: str = ""
    filesize: str = "5G"
    iodepth: int = 32
    ioengine: str = "libaio"
    mode: str = ""
    numjobs: int = 4
    ramptime_in_s: int = 0
    refill_buffers: bool = True
    runtime_in_s: int = 60
    time_based: bool = True


@base.register_benchmark
class FioFixedParams(base.Benchmark):
    """Run fio with fixed params"""

    def __init__(self, name: str, config: dict, benchmarker: Benchmarker):
        super().__init__(name, config, benchmarker)

        if not "fio" in config:
            raise ValueError(
                f"'fio' property (type: dict) is missing for '{self.name}'"
            )
        self.config = FioConfig(**config.get("fio", {}))

        dest = config.get("destination", {})
        if not dest:
            raise ValueError(
                f"'destination' property (type: dict) is missing for '{self.name}'"
            )
        self.destination = PosixBenchmarkDestination(**dest)

        self._run_task = ansible.AnsibleTask(playbook="run_fio_benchmark.yml")

    def _run_at(
        self, result_file: Path, repetition: int, fio_config: FioConfig
    ) -> RunResult:
        """Perform a single run"""

        start_time = time.monotonic()

        self._run_task.run_at(
            self.destination.host,
            {
                "fio_dir": self.destination.target_folder,
                "fio_result_file": result_file.name,
                "controller_dir": result_file.parent,
                "fio_jobname": self.name,
                **{f"fio_{key}": value for key, value in fio_config.asdict().items()},
            },
        )

        total_runtime = time.monotonic() - start_time

        result = parse_result_file(result_file, self.name)
        if self.benchmarker.config.results_save_raw_results:
            new_path = self.benchmarker.results / self.result_file.stem
            new_path.mkdir(exist_ok=True)
            result_file.rename(new_path / result_file.name)

        log.info("Run took %d s", total_runtime)

        return result

    def get_tags(self) -> dict[str, str]:
        return {**super().get_tags(), "fio": self.config.asdict()}


@base.register_benchmark
class FioOneDimParams(base.BenchmarkOneDimMixin, FioFixedParams):
    """Run fio with multiple values for a singel dimension"""


@base.register_benchmark
class FioNotContainerized(FioFixedParams):
    """Run fio outside of a container"""

    def __init__(self, name: str, config: dict, benchmarker: Benchmarker):
        super().__init__(name, config, benchmarker)

        self._pre_task = ansible.AnsibleTask(
            playbook="run_fio_benchmark_not_containerized_check.yml",
            host=f"{self.destination.host},",
        )

        self._run_task = ansible.AnsibleTask(
            playbook="run_fio_benchmark_not_containerized.yml"
        )


@base.register_benchmark
class FioFullPosix(FioFixedParams):
    def run(self):
        """Run 'fio'"""

        with tempfile.TemporaryDirectory() as temp_dir:
            log.info("Start %s", self.name)

            throughput_write_config = dataclasses.replace(
                self.config,
                mode="write",
                blocksize="1024k",
                numjobs=4,
                iodepth=32,
            )
            throughput_read_config = dataclasses.replace(
                self.config,
                mode="read",
                blocksize="1024k",
                numjobs=4,
                iodepth=32,
            )
            iops_write_config = dataclasses.replace(
                self.config,
                mode="randwrite",
                blocksize="4k",
                numjobs=4,
                iodepth=32,
            )
            iops_read_config = dataclasses.replace(
                self.config,
                mode="randread",
                blocksize="4k",
                numjobs=4,
                iodepth=32,
            )
            latency_write_config = dataclasses.replace(
                self.config,
                mode="randwrite",
                blocksize="4k",
                numjobs=1,
                iodepth=1,
            )
            latency_read_config = dataclasses.replace(
                self.config,
                mode="randread",
                blocksize="4k",
                numjobs=1,
                iodepth=1,
            )

            runs = [
                ("throughput_write", throughput_write_config),
                ("throughput_read", throughput_read_config),
                ("iops_write", iops_write_config),
                ("iops_read", iops_read_config),
                ("latency_write", latency_write_config),
                ("latency_read", latency_read_config),
            ]

            for name, config in runs:
                self.benchmark_results[name] = []
                log.info("Start %s", name)
                for i in range(self.repetitions):
                    log.info("Run %d of %d", i + 1, self.repetitions)
                    result_file = Path(temp_dir) / f"{name}_{i}.json"

                    result = self._run_at(result_file, i, config)
                    self.benchmark_results[name].append(result)


def parse_result_file(file: Path, jobname: str) -> dict[str, Any]:
    if not file.is_file():
        raise ValueError(f"{file} is not a fio result file.")

    with file.open() as file_handle:
        lines = file_handle.readlines()
        lines = [l for l in lines if not l.startswith("fio: pid=")]
        result_json = json.loads("\n".join(lines))

    jobs = [job for job in result_json["jobs"] if job["jobname"] == jobname]

    if len(jobs) != 1:
        raise ValueError(f"Job '{jobname}' is missing in result {file}")

    result = {}
    mode = result_json["global options"].get("rw", None)
    if mode is None:
        # Fallback
        mode = jobs[0]["job options"]["rw"]

    if mode in ["read", "randread", "rw", "randrw"]:
        read = result_json["jobs"][0]["read"]
        result.update(_parse_job_result(read, "read"))
    if mode in ["write", "randwrite", "rw", "randrw"]:
        write = result_json["jobs"][0]["write"]
        result.update(_parse_job_result(write, "write"))

    return result


def _parse_job_result(result: dict, prefix: str) -> dict[str, Any]:
    return {
        f"{prefix}_bw_min_in_MiB": result["bw_min"] / 1024,
        f"{prefix}_bw_max_in_MiB": result["bw_max"] / 1024,
        f"{prefix}_bw_mean_in_MiB": result["bw_mean"] / 1024,
        f"{prefix}_iops_min": result["iops_min"],
        f"{prefix}_iops_max": result["iops_max"],
        f"{prefix}_iops_mean": result["iops_mean"],
        f"{prefix}_iops_stddev": result["iops_stddev"],
        f"{prefix}_lat_min_in_ms": result["lat_ns"]["min"] / 1_000_000,
        f"{prefix}_lat_max_in_ms": result["lat_ns"]["max"] / 1_000_000,
        f"{prefix}_lat_mean_in_ms": result["lat_ns"]["mean"] / 1_000_000,
        f"{prefix}_lat_stddev_in_ms": result["lat_ns"]["stddev"] / 1_000_000,
    }
