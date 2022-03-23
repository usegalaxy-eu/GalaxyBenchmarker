"""
Definition of different benchmark-types.
"""
from __future__ import annotations

import dataclasses
import json
import logging
import tempfile
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

from galaxy_benchmarker.benchmarks import base
from galaxy_benchmarker.bridge import ansible
from galaxy_benchmarker.utils.posix import PosixBenchmarkDestination

if TYPE_CHECKING:
    from galaxy_benchmarker.benchmarker import Benchmarker

log = logging.getLogger(__name__)


@dataclasses.dataclass
class FioConfig:
    """Available parameters for fio"""

    mode: Optional[str] = None
    blocksize: Optional[str] = None
    numjobs: Optional[int] = None
    iodepth: Optional[int] = None
    runtime_in_s: Optional[int] = None
    filesize: Optional[str] = None
    refill_buffers: Optional[bool] = None
    time_based: Optional[bool] = None

    def asdict(self):
        return {k: v for k, v in dataclasses.asdict(self).items() if v is not None}

    def validate(self):
        # TODO: Impelemnet validation logic
        pass

@base.register_benchmark
class FioFixedParams(base.Benchmark):
    """Run fio with fixed params on one or more destinations"""

    fio_config_default = FioConfig(
        runtime_in_s=60,
        filesize="5G",
        refill_buffers=True,
        time_based=True,
    )

    def __init__(self, name: str, config: dict, benchmarker: Benchmarker):
        super().__init__(name, config, benchmarker)

        merged_dict = {**self.fio_config_default.asdict(), **config.get("fio", {})}
        self.merged_fio_config = FioConfig(**merged_dict)
        self.merged_fio_config.validate()

        self.destinations: list[PosixBenchmarkDestination] = []
        for item in config.get("destinations", []):
            self.destinations.append(PosixBenchmarkDestination(**item))

        if not self.destinations:
            raise ValueError(
                f"At least one destination is required for benchmark {self.__class__.__name__}"
            )

        self._run_task = ansible.AnsibleTask(playbook="run_fio_benchmark.yml")

    def run(self):
        """Run 'fio' on each destination"""

        with tempfile.TemporaryDirectory() as temp_dir:
            for dest in self.destinations:
                log.info("Start %s for %s", self.name, dest.name)
                self.benchmark_results[dest.name] = []
                for i in range(self.repetitions):
                    log.info("Run %d of %d", i + 1, self.repetitions)
                    result_file = Path(temp_dir) / f"{self.name}_{dest.name}_{i}.json"

                    result = self._run_at(result_file, dest, self.merged_fio_config)
                    self.benchmark_results[dest.name].append(result)

    def _run_at(
        self, result_file: Path, dest: PosixBenchmarkDestination, fio_config: FioConfig
    ) -> dict:
        """Perform a single run"""

        start_time = time.monotonic()

        self._run_task.run_at(
            dest.host,
            {
                "fio_dir": dest.target_folder,
                "fio_result_file": result_file.name,
                "controller_dir": result_file.parent,
                "fio_jobname": self.name,
                **{f"fio_{key}": value for key, value in fio_config.asdict().items()},
            },
        )

        total_runtime = time.monotonic() - start_time

        result = parse_result_file(result_file, self.name)
        result["runtime_in_s"] = total_runtime
        log.info("Run took %d s", total_runtime)

        return result

    def get_tags(self) -> dict[str, str]:
        return {**super().get_tags(), "fio": self.merged_fio_config.asdict()}


@base.register_benchmark
class FioOneDimParams(FioFixedParams):
    """Run fio with multiple values for a singel dimension"""

    def __init__(self, name: str, config: dict, benchmarker: Benchmarker):
        super().__init__(name, config, benchmarker)

        self.dim_key = config.get("dim_key", None)
        if not self.dim_key:
            raise ValueError(
                f"Property 'dim_key' (str) is missing for {name}. Must be a vaild fio_config property name"
            )

        self.dim_values = config.get("dim_values", [])
        if not self.dim_values:
            raise ValueError(
                f"Property 'dim_values' (list) is missing for {name}. Must be a list of values for 'dim_key'"
            )

        key = self.dim_key
        for value in self.dim_values:
            fio_config = dataclasses.replace(self.merged_fio_config, **{key: value})
            fio_config.validate()

    def _run_at(
        self, result_file: Path, dest: PosixBenchmarkDestination, fioConfig: FioConfig
    ) -> dict:
        """Perform multiple runs for a single destination"""

        dest_results = {}

        key = self.dim_key
        for value in self.dim_values:
            log.info("Run with %s set to %s", key, value)

            current_config = dataclasses.replace(fioConfig, **{key: value})
            current_result_file = result_file.with_suffix(
                f".{value}{result_file.suffix}"
            )

            result = super()._run_at(current_result_file, dest, current_config)

            dest_results[value] = result

        return dest_results

    def get_tags(self) -> dict[str, str]:
        return {
            **super().get_tags(),
            "dim_key": self.dim_key,
            "dim_values": self.dim_values,
        }


class FioNotContainerized(FioFixedParams):
    """Run fio outside of a container"""

    def __init__(self, name: str, config: dict, benchmarker: Benchmarker):
        super().__init__(name, config, benchmarker)

        hosts = set(dest.host for dest in self.destinations)
        self._pre_task = ansible.AnsibleTask(
            playbook="run_fio_benchmark_not_containerized_check.yml",
            host=",".join(hosts) + ",",
        )

        self._run_task = ansible.AnsibleTask(
            playbook="run_fio_benchmark_not_containerized.yml"
        )


def parse_result_file(file: Path, jobname: str) -> dict[str, Any]:
    if not file.is_file():
        raise ValueError(f"{file} is not a fio result file.")

    with file.open() as file_handle:
        result_json = json.load(file_handle)

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
        f"{prefix}_bw_min_in_mb": result["bw_min"] / 1024,
        f"{prefix}_bw_max_in_mb": result["bw_max"] / 1024,
        f"{prefix}_bw_mean_in_mb": result["bw_mean"] / 1024,
        f"{prefix}_iops_min": result["iops_min"],
        f"{prefix}_iops_max": result["iops_max"],
        f"{prefix}_iops_mean": result["iops_mean"],
        f"{prefix}_iops_stddev": result["iops_stddev"],
        f"{prefix}_lat_min_in_ms": result["lat_ns"]["min"] / 1_000_000,
        f"{prefix}_lat_max_in_ms": result["lat_ns"]["max"] / 1_000_000,
        f"{prefix}_lat_mean_in_ms": result["lat_ns"]["mean"] / 1_000_000,
        f"{prefix}_lat_stddev_in_ms": result["lat_ns"]["stddev"] / 1_000_000,
    }
