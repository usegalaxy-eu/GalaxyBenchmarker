"""
Definition of galaxyjob-based benchmarks
"""
from __future__ import annotations

import dataclasses
import json
import logging
import os
import shlex
import time
from pathlib import Path
from typing import TYPE_CHECKING

import boto3

from galaxy_benchmarker.benchmarks import base
from galaxy_benchmarker.bridge import ansible
from galaxy_benchmarker.utils.destinations import (
    BenchmarkDestination,
    PosixBenchmarkDestination,
)

if TYPE_CHECKING:
    from galaxy_benchmarker.benchmarker import Benchmarker

log = logging.getLogger(__name__)


@dataclasses.dataclass
class GalaxyJobConfig(base.BenchmarkConfig):
    # Currently only the input is required
    input: object


class GalaxyJob(base.Benchmark):
    """Benchmarking system with 'dd'"""

    galaxy_tool_id = ""
    galaxy_tool_input_class = None
    galaxy_job_config_class = GalaxyJobConfig

    def __init__(self, name: str, config: dict, benchmarker: Benchmarker):
        super().__init__(name, config, benchmarker)

        if not self.galaxy_tool_id:
            raise ValueError(
                "Subclass of GalaxyJob has to specify class property 'galaxy_tool_id' (str)"
            )
        if not self.galaxy_tool_input_class:
            raise ValueError(
                "Subclass of GalaxyJob has to specify class property 'galaxy_tool_input_class' (dataclass)"
            )

        if not "galaxy_job" in config:
            raise ValueError(
                f"'galaxy_job' property (type: dict) is missing for '{self.name}'"
            )
        glx_job_config = config.get("galaxy_job")

        if not "input" in glx_job_config:
            raise ValueError(
                f"'galaxy_job'->'input' property (type: dict) is missing for '{self.name}'"
            )

        self.config = self.galaxy_job_config_class(
            input=self.galaxy_tool_input_class(**glx_job_config.pop("input")),
            **glx_job_config,
        )

        dest = config.get("destination", {})
        if not dest:
            raise ValueError(
                f"'destination' property (type: dict) is missing for '{self.name}'"
            )
        self.destination = BenchmarkDestination(**dest)

        self._run_task = ansible.AnsibleTask(playbook="run_galaxy_job.yml")

    def _run_at(
        self, result_file: Path, repetition: int, galaxy_job_config: GalaxyJobConfig
    ) -> dict:
        """Perform a single run"""

        start_time = time.monotonic()

        input_str = json.dumps(dataclasses.asdict(galaxy_job_config.input))

        self._run_task.run_at(
            self.destination.host,
            {
                "glx_tool_id": self.galaxy_tool_id,
                "glx_tool_input": shlex.quote(input_str),
                **{
                    f"glx_{key}": value
                    for key, value in galaxy_job_config.asdict().items()
                },
            },
        )

        total_runtime = time.monotonic() - start_time

        result = {"total_runtime_in_s": total_runtime}
        log.info("Run took %d s", total_runtime)

        return result

    def get_tags(self) -> dict[str, str]:
        return {
            **super().get_tags(),
            "galaxy_tool_id": self.galaxy_tool_id,
            "galaxy_tool_config": self.config.asdict(),
        }


@dataclasses.dataclass
class GalaxyFileGenOnMountVolumeConfig(GalaxyJobConfig):
    input: GalaxyFileGenInput
    expected_num_files: int
    verification_timeout_in_s: int
    path_to_files: str


@dataclasses.dataclass
class GalaxyFileGenInput:
    num_files: int
    file_size_in_bytes: int


@base.register_benchmark
class GalaxyFileGenOnMountVolume(GalaxyJob):
    galaxy_tool_id = "file_gen"
    galaxy_tool_input_class = GalaxyFileGenInput
    galaxy_job_config_class = GalaxyFileGenOnMountVolumeConfig

    def __init__(self, name: str, config: dict, benchmarker: Benchmarker):
        # Store destination
        dest_conf = config.pop("destination", {})
        config["destination"] = {"host": "dummy"}

        super().__init__(name, config, benchmarker)

        # Restore destination
        config["destination"] = dest_conf
        self.destination = PosixBenchmarkDestination(**dest_conf)

        self._run_task = ansible.AnsibleTask(playbook="run_galaxy_mount_benchmark.yml")
        self._pre_tasks.append(
            ansible.AnsibleTask(
                playbook="setup_galaxy_server.yml",
                host=self.destination.host,
                extra_vars={
                    "galaxy_use_mount": True,
                    "galaxy_host_volume": self.destination.target_folder,
                },
            )
        )
        self._post_tasks.append(
            ansible.AnsibleTask(
                playbook="cleanup_galaxy_server.yml",
                host=self.destination.host,
                extra_vars={
                    "galaxy_host_volume": self.destination.target_folder,
                },
            )
        )


@dataclasses.dataclass
class GalaxyFileGenOnS3Config(GalaxyJobConfig):
    input: GalaxyFileGenInput
    expected_num_files: int
    verification_timeout_in_s: int

    ## Credentials are loaded from AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
    # access_key_id: str
    base_url: str
    bucket_name: str
    # secret_access_key: str


@base.register_benchmark
class GalaxyFileGenOnS3(GalaxyJob):
    galaxy_tool_id = "file_gen"
    galaxy_tool_input_class = GalaxyFileGenInput
    galaxy_job_config_class = GalaxyFileGenOnS3Config

    def __init__(self, name: str, config: dict, benchmarker: Benchmarker):
        super().__init__(name, config, benchmarker)

        access_key = os.getenv("AWS_ACCESS_KEY_ID")
        secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        if access_key is None or secret_key is None:
            raise ValueError("Missing S3 credentials in env vars: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY")

        self._pre_tasks.append(
            ansible.AnsibleTask(
                playbook="setup_galaxy_server.yml",
                host=self.destination.host,
                extra_vars={
                    "galaxy_use_s3": True,
                    "S3_ACCESS_KEY": os.getenv("AWS_ACCESS_KEY_ID"),
                    "S3_SECRET_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
                    "S3_BASE_URL": self.config.base_url,
                    "S3_BUCKET_NAME": self.config.bucket_name,
                },
            )
        )
        self._post_tasks.append(
            ansible.AnsibleTask(
                playbook="cleanup_galaxy_server.yml", host=self.destination.host
            )
        )

    def _run_at(
        self,
        result_file: Path,
        repetition: int,
        galaxy_job_config: GalaxyFileGenOnS3Config,
    ) -> dict:
        """Perform a single run"""

        start_time = time.monotonic()

        # Trigger galaxy job
        super()._run_at(result_file, repetition, galaxy_job_config)

        # Check s3 bucket for files
        num_files = 0

        while num_files < galaxy_job_config.expected_num_files:
            current_runtime = time.monotonic() - start_time
            if current_runtime >= galaxy_job_config.verification_timeout_in_s:
                raise RuntimeError("Verification timed out")

            num_files = self._get_current_num_files(galaxy_job_config)
            log.info("Currently %d files present", num_files)
            time.sleep(5)

        total_runtime = time.monotonic() - start_time
        result = {"total_runtime_in_s": total_runtime}
        log.info("Run took %d s", total_runtime)

        log.info("Empty s3 bucket")
        client = boto3.resource(
            "s3",
            endpoint_url=galaxy_job_config.base_url,
        )
        bucket = client.Bucket(galaxy_job_config.bucket_name)
        bucket.objects.all().delete()
        log.info("Empty s3 bucket done")

        return result

    def _get_current_num_files(self, galaxy_job_config: GalaxyFileGenOnS3Config) -> int:
        client = boto3.client(
            "s3",
            endpoint_url=galaxy_job_config.base_url,
        )
        result = client.list_objects_v2(
            Bucket=galaxy_job_config.bucket_name, Delimiter="/"
        )
        if "CommonPrefixes" not in result:
            return 0

        # Each prefix contains 1000 files, except the last one
        num = (len(result["CommonPrefixes"][:-1])) * 1000
        # Subtract one because prefix 000 only has 999 files
        num = max(0, num - 1)

        last_prefix = result["CommonPrefixes"][-1]["Prefix"]
        resp = client.list_objects_v2(
            Bucket="frct-smoe-bench-ec61-01", Prefix=last_prefix
        )
        num += len(resp["Contents"])
        return num
