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
class S3BenchmarkConfig:
    access_key_id: str
    base_url: str
    bucket_name: str
    filesize: str
    region: str
    runtime_in_s: int
    secret_access_key: str
    threads: int

    def asdict(self):
        return {k: v for k, v in dataclasses.asdict(self).items() if v is not None}


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

    s3benchmark_config_default = S3BenchmarkConfig(
        access_key_id="",
        base_url="",
        bucket_name="",
        filesize="",
        region="",
        runtime_in_s=60,
        secret_access_key="",
        threads="",
    )

    def __init__(self, name: str, config: dict, benchmarker: Benchmarker):
        super().__init__(name, config, benchmarker)

        merged_dict = {
            **self.s3benchmark_config_default.asdict(),
            **config.get("s3benchmark", {}),
        }
        self.merged_s3benchmark_config = S3BenchmarkConfig(**merged_dict)

        dest = config.get("destination", {})
        if not dest:
            raise ValueError(
                f"'destination' property (type: dict) is missing for '{self.name}'"
            )
        self.destination = PosixBenchmarkDestination(**dest)

        self._run_task = ansible.AnsibleTask(playbook="run_s3-benchmark_benchmark.yml")

    def run(self):
        """Run 's3-benchmark'"""

        with tempfile.TemporaryDirectory() as temp_dir:
            log.info("Start %s", self.name)
            self.benchmark_results[self.name] = []
            for i in range(self.repetitions):
                log.info("Run %d of %d", i + 1, self.repetitions)
                result_file = Path(temp_dir) / f"{self.name}_{i}.json"

                result = self._run_at(result_file, i, self.merged_s3benchmark_config)
                self.benchmark_results[self.name].append(result)

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
                **{
                    f"s3b_{key}": value
                    for key, value in s3benchmark_config.asdict().items()
                },
            },
        )

        total_runtime = time.monotonic() - start_time

        result = parse_result_file(result_file)
        log.info("Run took %d s", total_runtime)

        return result
