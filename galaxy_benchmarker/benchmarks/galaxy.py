"""
Definition of galaxyjob-based benchmarks
"""
from __future__ import annotations

import dataclasses
import logging
import json
import time
import shutil
import shlex
from pathlib import Path
from typing import TYPE_CHECKING, Any

from galaxy_benchmarker.benchmarks import base
from galaxy_benchmarker.bridge import ansible
from galaxy_benchmarker.utils.destinations import BenchmarkDestination

if TYPE_CHECKING:
    from galaxy_benchmarker.benchmarker import Benchmarker

log = logging.getLogger(__name__)


def parse_result_file(file: Path) -> dict[str, Any]:
    # if not file.is_file():
    #     raise ValueError(f"{file} is not a file.")

    # # Example output
    # # 512+0 records in
    # # 512+0 records out
    # # 512+0 records in
    # # 512+0 records out
    # # 536870912 bytes (537 MB, 512 MiB) copied, 2.32072 s, 231 MB/s
    # # 536870912 bytes (537 MB, 512 MiB) copied, 2.3709 s, 226 MB/s

    # pattern = re.compile(
    #     r"([0-9]+) bytes .* copied, ([0-9\.]+) s, ([0-9\.]+) MB/s$"
    # )

    # matches = []
    # with file.open() as file_handle:
    #     for line in file_handle:
    #         match = pattern.findall(line)
    #         if match:
    #             matches.extend(match)

    # total_bw_in_MiB = 0.0
    # total_bw_in_mb = 0.0
    # for bytes, time, bw_in_MB in matches:
    #     bytes, time, bw_in_MB = int(bytes), float(time), float(bw_in_MB)
    #     bw_in_MiB = (bytes / 1024**2) / time
    #     bw_in_MB_calulated = (bytes / 1000**2) / time

    #     if bw_in_MB not in [round(bw_in_MB_calulated, 0),round(bw_in_MB_calulated, 1)]:
    #         log.warning(
    #             "Missmatch between calculated and parsed bandwidth in MB: Parsed: %.2f, Calculated %.2f",
    #             bw_in_MB,
    #             bw_in_MB_calulated,
    #         )
    #     total_bw_in_MiB += bw_in_MiB
    #     total_bw_in_mb += bw_in_MB

    # return {
    #     "bw_in_MiB": total_bw_in_MiB,
    #     "bw_in_mb": total_bw_in_mb,
    #     "detected_matches": len(matches)
    # }
    pass


@dataclasses.dataclass
class GalaxyJobConfig(base.BenchmarkConfig):
    # Currently only the input is required
    input: object


class GalaxyJob(base.Benchmark):
    """Benchmarking system with 'dd'"""
    galaxy_tool_id = ""
    galaxy_tool_input_class = None


    def __init__(self, name: str, config: dict, benchmarker: Benchmarker):
        super().__init__(name, config, benchmarker)

        if not self.galaxy_tool_id:
            raise ValueError("Subclass of GalaxyJob has to specify class property 'galaxy_tool_id' (str)")
        if not self.galaxy_tool_input_class:
            raise ValueError("Subclass of GalaxyJob has to specify class property 'galaxy_tool_input_class' (dataclass)")

        if not "galaxy_job_input" in config:
            raise ValueError(
                f"'galaxy_job_input' property (type: dict) is missing for '{self.name}'"
            )
        self.config = GalaxyJobConfig(
            input=self.galaxy_tool_input_class(**config.get("galaxy_job_input", {}))
        )

        dest = config.get("destination", {})
        if not dest:
            raise ValueError(
                f"'destination' property (type: dict) is missing for '{self.name}'"
            )
        self.destination = BenchmarkDestination(**dest)

        self._run_task = ansible.AnsibleTask(playbook="run_galaxy_job.yml")

    def _run_at(self, result_file: Path, repetition: int, galaxy_job_config: GalaxyJobConfig) -> dict:
        """Perform a single run"""

        start_time = time.monotonic()

        input_str = json.dumps(
            dataclasses.asdict(galaxy_job_config.input)
        )

        self._run_task.run_at(
            self.destination.host,
            {
                "glx_result_file": result_file.name,
                "controller_dir": result_file.parent,
                "glx_tool_id": self.galaxy_tool_id,
                "glx_tool_input": shlex.quote(input_str)
            },
        )

        total_runtime = time.monotonic() - start_time

        if self.benchmarker.config.results_save_raw_results:
            new_path = self.benchmarker.results / self.result_file.stem
            new_path.mkdir(exist_ok=True)
            shutil.copy(result_file, new_path / result_file.name)

        result = parse_result_file(result_file)
        log.info("Run took %d s", total_runtime)

        return result

    def get_tags(self) -> dict[str, str]:
        return {
            **super().get_tags(),
            "galaxy_tool_id": self.galaxy_tool_id,
            "galaxy_tool_config": self.config.asdict()
        }


@dataclasses.dataclass
class GalaxyFileGenInput:
    num_files: int
    file_size_in_bytes: int


@base.register_benchmark
class GalaxyFileGen(GalaxyJob):
    galaxy_tool_id = "file_gen"
    galaxy_tool_input_class = GalaxyFileGenInput
