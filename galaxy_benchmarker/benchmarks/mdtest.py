"""
Definition of mdtest-based benchmarks
"""
from __future__ import annotations

import dataclasses
import logging
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
class MdtestConfig(base.BenchmarkConfig):
    """Available parameters for mdtest"""

    num_files: int = 1


@base.register_benchmark
class MdtestFixedParams(base.Benchmark):
    """Run mdtest with fixed params"""

    def __init__(self, name: str, config: dict, benchmarker: Benchmarker):
        super().__init__(name, config, benchmarker)

        self.config = MdtestConfig(**config.get("mdtest", {}))

        dest = config.get("destination", {})
        if not dest:
            raise ValueError(
                f"'destination' property (type: dict) is missing for '{self.name}'"
            )
        self.destination = PosixBenchmarkDestination(**dest)

        self._run_task = ansible.AnsibleTask(playbook="run_mdtest_benchmark.yml")

    def _run_at(
        self, result_file: Path, repetition: int, mdtest_config: MdtestConfig
    ) -> RunResult:
        """Perform a single run"""

        start_time = time.monotonic()

        self._run_task.run_at(
            self.destination.host,
            {
                "mdtest_dir": self.destination.target_folder,
                "mdtest_result_file": result_file.name,
                "controller_dir": result_file.parent,
                **{
                    f"mdtest_{key}": value
                    for key, value in mdtest_config.asdict().items()
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

    def get_tags(self) -> dict[str, str]:
        return {**super().get_tags(), "mdtest": self.config.asdict()}


def parse_result_file(file: Path) -> dict[str, Any]:
    if not file.is_file():
        raise ValueError(f"{file} is not a mdtest result file.")

    with file.open() as file_handle:
        lines = file_handle.readlines()

    metrics = {
        "Directory creation": "dir_create",
        "Directory stat": "dir_stat",
        "Directory rename": "dir_rename",
        "Directory removal": "dir_remove",
        "File creation": "file_create",
        "File stat": "file_stat",
        "File read": "file_read",
        "File removal": "file_remove",
        "Tree creation": "tree_create",
        "Tree removal": "tree_remove",
    }

    result = {}

    for line in lines:
        l = line.split()
        if len(l) < 2:
            continue
        if (key := f"{l[0]} {l[1]}") not in metrics:
            continue
        # Line has to be of format:
        # Op                  Max    Min    Mean   Std Dev
        # "Directory creation 48.473 48.473 48.473 0.000"
        assert len(l) == 6
        metric = metrics[key]
        result[metric] = float(l[4])

    return result
