"""
Definition of galaxyjob-based benchmarks
"""
from __future__ import annotations

import dataclasses
import json
import logging
import shlex
import time
from pathlib import Path
from typing import TYPE_CHECKING

from galaxy_benchmarker.benchmarks import base
from galaxy_benchmarker.bridge import ansible
from galaxy_benchmarker.utils.destinations import (
    BenchmarkDestination,
    PosixBenchmarkDestination,
)
from galaxy_benchmarker.utils import s3

if TYPE_CHECKING:
    from galaxy_benchmarker.benchmarker import Benchmarker

log = logging.getLogger(__name__)


@dataclasses.dataclass
class GalaxyFileGenInput:
    num_files: int
    file_size_in_bytes: int

@dataclasses.dataclass
class GalaxyFileGenConfig(base.BenchmarkConfig):
    input: GalaxyFileGenInput
    expected_num_files: int
    verification_timeout_in_s: int


class GalaxyFileGenJob(base.Benchmark):
    """Benchmarking galaxy with the file_gen job"""

    galaxy_tool_id = "file_gen"
    galaxy_job_config_class = GalaxyFileGenConfig

    def __init__(self, name: str, config: dict, benchmarker: Benchmarker):
        super().__init__(name, config, benchmarker)

        if not self.galaxy_tool_id:
            raise ValueError(
                "Subclass of GalaxyJob has to specify class property 'galaxy_tool_id' (str)"
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

        self.config  = self.galaxy_job_config_class(
            input=GalaxyFileGenInput(**glx_job_config.pop("input")),
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
        self, result_file: Path, repetition: int, galaxy_job_config: GalaxyFileGenConfig
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
class GalaxyFileGenOnMountVolumeConfig(GalaxyFileGenConfig):
    path_to_files: str


@base.register_benchmark
class GalaxyFileGenOnMountVolumeJob(GalaxyFileGenJob):
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
class GalaxyFileGenOnS3Config(s3.S3Config, GalaxyFileGenConfig):
    pass


@base.register_benchmark
class GalaxyFileGenOnS3Job(GalaxyFileGenJob):
    galaxy_job_config_class = GalaxyFileGenOnS3Config
    config: GalaxyFileGenOnS3Config

    def __init__(self, name: str, config: dict, benchmarker: Benchmarker):
        super().__init__(name, config, benchmarker)

        self._pre_tasks.append(
            ansible.AnsibleTask(
                playbook="setup_galaxy_server.yml",
                host=self.destination.host,
                extra_vars={
                    "galaxy_use_s3": True,
                    "S3_ACCESS_KEY": self.config.access_key_id,
                    "S3_SECRET_KEY": self.config.secret_access_key,
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
        s3.check_bucket_for_files(galaxy_job_config, galaxy_job_config.expected_num_files, galaxy_job_config.verification_timeout_in_s)

        total_runtime = time.monotonic() - start_time
        result = {"total_runtime_in_s": total_runtime}
        log.info("Run took %d s", total_runtime)

        log.info("Empty s3 bucket")
        s3.empty_bucket(galaxy_job_config)
        log.info("Empty s3 bucket done")

        return result


@dataclasses.dataclass
class GalaxyFileGenOnIrodsOnMountVolumeConfig(GalaxyFileGenConfig):
    irods_server_host: str
    path_to_files: str


@base.register_benchmark
class GalaxyFileGenOnIrodsOnMountVolumeJob(GalaxyFileGenJob):
    galaxy_job_config_class = GalaxyFileGenOnIrodsOnMountVolumeConfig

    def __init__(self, name: str, config: dict, benchmarker: Benchmarker):
        super().__init__(name, config, benchmarker)

        self._run_task = ansible.AnsibleTask(playbook="run_galaxy_mount_benchmark.yml")
        self._pre_tasks.append(
            ansible.AnsibleTask(
                playbook="setup_galaxy_server.yml",
                host=self.destination.host,
                extra_vars={
                    "galaxy_use_irods": True,
                    "IRODS_HOST": self.config.irods_server_host,
                },
            )
        )
        self._post_tasks.append(
            ansible.AnsibleTask(
                playbook="cleanup_galaxy_server.yml", host=self.destination.host
            )
        )


@dataclasses.dataclass
class GalaxyFileGenOnIrodsOnS3Config(s3.S3Config, GalaxyFileGenConfig):
    irods_server_host: str


@base.register_benchmark
class GalaxyFileGenOnIrodsOnS3Job(GalaxyFileGenJob):
    galaxy_job_config_class = GalaxyFileGenOnIrodsOnS3Config
    config: GalaxyFileGenOnIrodsOnS3Config

    def __init__(self, name: str, config: dict, benchmarker: Benchmarker):
        super().__init__(name, config, benchmarker)

        self._pre_tasks.append(
            ansible.AnsibleTask(
                playbook="setup_galaxy_server.yml",
                host=self.destination.host,
                extra_vars={
                    "galaxy_use_irods": True,
                    "IRODS_HOST": self.config.irods_server_host,
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
        s3.check_bucket_for_files(galaxy_job_config, galaxy_job_config.expected_num_files, galaxy_job_config.verification_timeout_in_s)

        total_runtime = time.monotonic() - start_time
        result = {"total_runtime_in_s": total_runtime}
        log.info("Run took %d s", total_runtime)

        log.info("Empty s3 bucket")
        s3.empty_bucket(galaxy_job_config)
        log.info("Empty s3 bucket done")

        return result
