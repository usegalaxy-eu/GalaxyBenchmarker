"""
Definition of s3benchmark-based benchmarks
"""
from __future__ import annotations

import dataclasses
import logging
import os
import re
import shutil
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

from galaxy_benchmarker.benchmarks import base
from galaxy_benchmarker.bridge import ansible
from galaxy_benchmarker.utils.destinations import BenchmarkDestination

if TYPE_CHECKING:
    from galaxy_benchmarker.benchmarker import Benchmarker

log = logging.getLogger(__name__)


@dataclasses.dataclass
class S3BenchmarkConfig(base.BenchmarkConfig):
    ## Credentials are loaded from AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
    # access_key_id: str = ""
    base_url: str = ""
    bucket_name: str = ""
    filesize: str = ""
    region: str = ""
    runtime_in_s: int = 60
    # secret_access_key: str = ""
    threads: int = 1


def parse_result_file(file: Path) -> dict[str, Any]:
    if not file.is_file():
        raise ValueError(f"{file} is not a file.")

    # Example output
    # Loop 1: PUT time 10.2 secs, objects = 1074, speed = 105.3MB/sec, 105.3 operations/sec. Slowdowns = 0
    # Loop 1: GET time 10.1 secs, objects = 1124, speed = 111.4MB/sec, 111.4 operations/sec. Slowdowns = 0
    # Loop 1: DELETE time 0.8 secs, 1286.9 deletes/sec. Slowdowns = 0

    l_get, l_put, l_delete = "", "", ""
    with file.open() as file_handle:
        for line in file_handle:
            if not line.startswith("Loop"):
                continue

            striped = line.split(":")[1].lstrip()
            if striped.startswith("GET"):
                l_get = striped
            elif striped.startswith("PUT"):
                l_put = striped
            elif striped.startswith("DELETE"):
                l_delete = striped
            else:
                raise ValueError(f"Unknown linestart: {striped}")

    pattern = re.compile(
        r", objects = ([0-9]+), speed = ([0-9\.]+)MB/sec, ([0-9\.]+) operations/sec"
    )
    get_num_obj, get_bw, get_ops = pattern.search(l_get).groups()
    put_num_obj, put_bw, put_ops = pattern.search(l_put).groups()
    del_ops = re.search(r", ([0-9\.]+) deletes/sec", l_delete).groups()[0]

    return {
        "get_num_objects": get_num_obj,
        "get_bw_in_mibi": get_bw,
        "get_op_per_s": get_ops,
        "put_num_objects": put_num_obj,
        "put_bw_in_mibi": put_bw,
        "put_op_per_s": put_ops,
        "del_op_per_s": del_ops,
    }


@base.register_benchmark
class S3BenchmarkFixedParams(base.Benchmark):
    """Benchmarking system with 's3benchmark'"""

    def __init__(self, name: str, config: dict, benchmarker: Benchmarker):
        super().__init__(name, config, benchmarker)

        if not "s3benchmark" in config:
            raise ValueError(
                f"'s3benchmark' property (type: dict) is missing for '{self.name}'"
            )
        self.config = S3BenchmarkConfig(**config.get("s3benchmark"))

        dest = config.get("destination", {})
        if not dest:
            raise ValueError(
                f"'destination' property (type: dict) is missing for '{self.name}'"
            )
        self.destination = BenchmarkDestination(**dest)

        self._run_task = ansible.AnsibleTask(playbook="run_s3-benchmark_benchmark.yml")

    def _run_at(
        self, result_file: Path, repetition: int, s3benchmark_config: S3BenchmarkConfig
    ) -> dict:
        """Perform a single run"""

        start_time = time.monotonic()

        self._run_task.run_at(
            self.destination.host,
            {
                "s3b_result_file": result_file.name,
                "controller_dir": result_file.parent,
                "s3b_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
                "s3b_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
                **{
                    f"s3b_{key}": value
                    for key, value in s3benchmark_config.asdict().items()
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
        return {**super().get_tags(), "s3b": self.config.asdict()}


@base.register_benchmark
class S3BenchmarkOneDimParams(base.BenchmarkOneDimMixin, S3BenchmarkFixedParams):
    """Run s3benchmark with multiple values for a singel dimension"""
