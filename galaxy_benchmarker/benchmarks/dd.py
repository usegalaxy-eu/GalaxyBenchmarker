"""
Definition of different benchmark-types.
"""
from __future__ import annotations

import dataclasses
import logging
import re
import tempfile
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

from galaxy_benchmarker.benchmarks import base
from galaxy_benchmarker.bridge import ansible
from galaxy_benchmarker.utils.posix import PosixBenchmarkDestination

if TYPE_CHECKING:
    from galaxy_benchmarker.benchmarker import Benchmarker

log = logging.getLogger(__name__)


@dataclasses.dataclass
class DdConfig(base.BenchmarkConfig):
    blocksize: str = ""
    blockcount: str = ""
    input: str = "/dev/zero"
    output: str = "/mnt/volume_under_test/dd-testfile.bin"
    flush: bool = True
    cleanup: bool = True


def parse_result_file(file: Path) -> dict[str, Any]:
    if not file.is_file():
        raise ValueError(f"{file} is not a file.")

    with file.open() as file_handle:
        last_line = file_handle.readlines()[-1]

    match = re.search(
        r"([0-9]+) bytes .* copied, ([0-9\.]+) s, ([0-9\.]+) MB/s$", last_line
    )

    if not match:
        return {}

    bytes, time, bw_in_MB = match.groups()
    bytes, time, bw_in_MB = int(bytes), float(time), float(bw_in_MB)
    bw_in_MiB = (bytes / 1024**2) / time
    bw_in_MB_calulated = (bytes / 1000**2) / time

    if bw_in_MB != round(bw_in_MB_calulated, 0):
        log.warning(
            "Missmatch between calculated and parsed bandwidth in MB: Parsed: %.2f, Calculated %.2f",
            bw_in_MB,
            bw_in_MB_calulated,
        )

    return {"bw_in_MiB": bw_in_MiB, "bw_in_mb": bw_in_MB}


@base.register_benchmark
class DdFixedParams(base.Benchmark):
    """Benchmarking system with 'dd'"""

    def __init__(self, name: str, config: dict, benchmarker: Benchmarker):
        super().__init__(name, config, benchmarker)

        self.config = DdConfig(**config.get("dd", {}))

        dest = config.get("destination", {})
        if not dest:
            raise ValueError(
                f"'destination' property (type: dict) is missing for '{self.name}'"
            )
        self.destination = PosixBenchmarkDestination(**dest)

        self._run_task = ansible.AnsibleTask(playbook="run_dd_benchmark.yml")

    def _run_at(self, result_file: Path, repetition: int, dd_config: DdConfig) -> dict:
        """Perform a single run"""

        start_time = time.monotonic()

        self._run_task.run_at(
            self.destination.host,
            {
                "dd_dir": self.destination.target_folder,
                "dd_result_file": result_file.name,
                "controller_dir": result_file.parent,
                **{f"dd_{key}": value for key, value in dd_config.asdict().items()},
            },
        )

        total_runtime = time.monotonic() - start_time

        result = parse_result_file(result_file)
        if self.benchmarker.config.results_save_raw_results:
            new_path = self.benchmarker.results / self.result_file.stem
            new_path.mkdir(exist_ok=True)
            result_file.rename(new_path / result_file.name)

        log.info("Run took %d s", total_runtime)

        return result


@base.register_benchmark
class DdOneDimParams(DdFixedParams):
    """Run dd with multiple values for a singel dimension"""

    def __init__(self, name: str, config: dict, benchmarker: Benchmarker):
        super().__init__(name, config, benchmarker)

        self.dim_key = config.get("dim_key", None)
        if not self.dim_key:
            raise ValueError(
                f"Property 'dim_key' (str) is missing for {name}. Must be a vaild dd_config property name"
            )

        self.dim_values = config.get("dim_values", [])
        if not self.dim_values:
            raise ValueError(
                f"Property 'dim_values' (list) is missing for {name}. Must be a list of values for 'dim_key'"
            )

        # Validate configurations
        key = self.dim_key
        for value in self.dim_values:
            dataclasses.replace(self.config, **{key: value})

    def run(self):
        """Run 'dd' with one changing parameter"""

        with tempfile.TemporaryDirectory() as temp_dir:
            key = self.dim_key
            for value in self.dim_values:
                log.info("Run with %s set to %s", key, value)

                current_config = dataclasses.replace(self.config, **{key: value})

                self.benchmark_results[value] = []
                for i in range(self.repetitions):
                    log.info("Run %d of %d", i + 1, self.repetitions)

                    result_file = Path(temp_dir) / f"{self.name}_{value}_{i}.json"
                    result = self._run_at(result_file, i, current_config)
                    self.benchmark_results[value].append(result)

    def get_tags(self) -> dict[str, str]:
        return {
            **super().get_tags(),
            "dim_key": self.dim_key,
            "dim_values": self.dim_values,
        }


@base.register_benchmark
class DdPrepareNetappRead(DdOneDimParams):
    """Setup directory struture for DD read benchmark"""

    def _run_at(self, result_file: Path, repetition: int, dd_config: DdConfig) -> dict:
        """Perform a single run"""

        start_time = time.monotonic()

        filename = f"{dd_config.blockcount}x{dd_config.blocksize}.{repetition}.bin"
        current_config = dataclasses.replace(
            dd_config,
            input="/dev/urandom",
            output=f"/mnt/volume_under_test/{filename}",
            cleanup=False,
        )

        self._run_task.run_at(
            self.destination.host,
            {
                "dd_dir": self.destination.target_folder,
                "dd_result_file": result_file.name,
                "controller_dir": result_file.parent,
                **{
                    f"dd_{key}": value for key, value in current_config.asdict().items()
                },
            },
        )

        total_runtime = time.monotonic() - start_time
        log.info("Run took %d s", total_runtime)
        return {"runtime_in_s": total_runtime}


@base.register_benchmark
class DdNetappRead(DdOneDimParams):
    def _run_at(self, result_file: Path, repetition: int, dd_config: DdConfig) -> dict:
        """Perform a single run"""

        start_time = time.monotonic()

        filename = f"{dd_config.blockcount}x{dd_config.blocksize}.{repetition}.bin"
        current_config = dataclasses.replace(
            dd_config,
            input=f"/mnt/volume_under_test/{filename}",
            output="/dev/null",
            flush=False,
            cleanup=False,
        )

        self._run_task.run_at(
            self.destination.host,
            {
                "dd_dir": self.destination.target_folder,
                "dd_result_file": result_file.name,
                "controller_dir": result_file.parent,
                **{
                    f"dd_{key}": value for key, value in current_config.asdict().items()
                },
            },
        )

        total_runtime = time.monotonic() - start_time

        result = parse_result_file(result_file)
        if self.benchmarker.config.results_save_raw_results:
            new_path = self.benchmarker.results / self.result_file.stem
            new_path.mkdir(exist_ok=True)
            result_file.rename(new_path / result_file.name)

        log.info("Run took %d s", total_runtime)

        return result
