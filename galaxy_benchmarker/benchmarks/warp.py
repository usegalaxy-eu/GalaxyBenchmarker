"""
Definition of warp-based benchmarks
"""
from __future__ import annotations

import dataclasses
import logging
import re
import time
import shutil
from pathlib import Path
from typing import TYPE_CHECKING, Any

from galaxy_benchmarker.benchmarks import base
from galaxy_benchmarker.bridge import ansible
from galaxy_benchmarker.utils.destinations import PosixBenchmarkDestination

if TYPE_CHECKING:
    from galaxy_benchmarker.benchmarker import Benchmarker

log = logging.getLogger(__name__)


@dataclasses.dataclass
class WarpConfig(base.BenchmarkConfig):
    mode: str = ""
    access_key_id: str = ""
    base_url: str = ""
    bucket_name: str = ""
    filesize: str = ""
    region: str = ""
    runtime: str = ""
    secret_access_key: str = ""
    concurrent_ops: int = 1


def parse_result_file(file: Path) -> dict[str, Any]:

    if not file.is_file():
        raise ValueError(f"{file} is not a file.")

    # Example output
    # Operation: DELETE, 11%, Concurrency: 20, Ran 3m47s.
    #  * Throughput: 0.28 obj/s

    # Operation: GET, 38%, Concurrency: 20, Ran 4m15s.
    #  * Throughput: 8.21 MiB/s, 0.82 obj/s

    # Operation: PUT, 13%, Concurrency: 20, Ran 4m19s.
    #  * Throughput: 2.85 MiB/s, 0.29 obj/s

    # Operation: STAT, 33%, Concurrency: 20, Ran 4m2s.
    #  * Throughput: 0.65 obj/s
    result = {}
    op = ""
    pattern_op = re.compile(r"Operation: ([A-Z]+),?")
    pattern_throughput = re.compile(r"([0-9\.]+) MiB/s,")
    pattern_ops = re.compile(r"([0-9\.]+) obj/s")

    with file.open() as file_handle:
        for line in file_handle:
            if line.startswith("Operation: "):
                # Get op to parse next line
                op = pattern_op.match(line).groups()[0]
                continue
            if not op:
                continue

            throughput_match = pattern_throughput.search(line)
            ops_match = pattern_ops.search(line)

            if op == "GET":
                result["get_bw_in_MiB"] = throughput_match.groups()[0]
                result["get_ops"] = ops_match.groups()[0]
            elif op == "PUT":
                result["put_bw_in_MiB"] = throughput_match.groups()[0]
                result["put_ops"] = ops_match.groups()[0]
            elif op == "DELETE":
                result["delete_ops"] = ops_match.groups()[0]
            elif op == "STAT":
                result["stat_ops"] = ops_match.groups()[0]
            op = ""

    return result

@base.register_benchmark
class WarpFixedParams(base.Benchmark):
    """Benchmarking system with 'warp'"""

    def __init__(self, name: str, config: dict, benchmarker: Benchmarker):
        super().__init__(name, config, benchmarker)

        if not "warp" in config:
            raise ValueError(
                f"'warp' property (type: dict) is missing for '{self.name}'"
            )
        self.config = WarpConfig(**config.get("warp"))

        dest = config.get("destination", {})
        if not dest:
            raise ValueError(
                f"'destination' property (type: dict) is missing for '{self.name}'"
            )
        self.destination = PosixBenchmarkDestination(**dest)

        self._run_task = ansible.AnsibleTask(playbook="run_warp_benchmark.yml")

    def _run_at(
        self, result_file: Path, repetition: int, warp_config: WarpConfig
    ) -> dict:
        """Perform a single run"""

        start_time = time.monotonic()

        self._run_task.run_at(
            self.destination.host,
            {
                "warp_result_file": result_file.name,
                "controller_dir": result_file.parent,
                **{
                    f"warp_{key}": value
                    for key, value in warp_config.asdict().items()
                },
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
        return {**super().get_tags(), "warp": self.config.asdict()}

@base.register_benchmark
class WarpOneDimParams(base.BenchmarkOneDimMixin, WarpFixedParams):
    """Run warp with multiple values for a singel dimension"""
