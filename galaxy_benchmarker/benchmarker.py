from __future__ import annotations

import logging
import signal
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from types import FrameType
from typing import Optional

from serde import serde

from galaxy_benchmarker.benchmarks.base import Benchmark
from galaxy_benchmarker.utils import ansible
from galaxy_benchmarker.bridge.galaxy import Galaxy, GalaxyConfig
from galaxy_benchmarker.bridge.openstack import OpenStackCompute, OpenStackComputeConfig
from galaxy_benchmarker.typing import NamedConfigDicts

log = logging.getLogger(__name__)


@serde
@dataclass
class BenchmarkerConfig:
    openstack: Optional[OpenStackComputeConfig] = None
    galaxy: Optional[GalaxyConfig] = None

    results_path: str = "results/"
    results_save_to_file: bool = True
    results_save_raw_results: bool = False
    results_print: bool = True

    log_ansible_output: bool = False


@serde
@dataclass
class GlobalConfig:
    config: Optional[BenchmarkerConfig]

    tasks: Optional[NamedConfigDicts]
    benchmarks: NamedConfigDicts


class Benchmarker:
    def __init__(self, config: BenchmarkerConfig, benchmarks: NamedConfigDicts):
        self.config = config
        self.glx = Galaxy(config.galaxy) if config.galaxy else None
        self.openstack = (
            OpenStackCompute(config.openstack) if config.openstack else None
        )
        self.current_benchmark: Optional[Benchmark] = None

        self.benchmarks: list[Benchmark] = []
        for name, b_config in benchmarks.items():
            self.benchmarks.append(Benchmark.create(name, b_config, self))

        self.results = Path(config.results_path)
        if self.results.exists():
            if not self.results.is_dir():
                raise ValueError("'results_path' has to be a folder")
        else:
            self.results.mkdir(parents=True)

        if config.log_ansible_output:
            ansible.LOG_ANSIBLE_OUTPUT = True

        # Safe results in case of interrupt
        def handle_signal(signum: int, frame: Optional[FrameType]) -> None:
            ansible.stop_playbook(signum)
            self.save_results_of_current_benchmark()
            exit(0)

        signal.signal(signal.SIGINT, handle_signal)

        # self.benchmarks = benchmarks.parse_from_dic(config)
        # self.workflows = dict()
        # for wf_config in config["workflows"]:
        #     self.workflows[wf_config["name"]] = workflow.configure_workflow(wf_config)

        # self.destinations = dict()
        # for dest_config in config["destinations"]:
        #     self.destinations[dest_config["name"]] = destination.configure_destination(dest_config, self.glx)

        # self.benchmarks = dict()
        # for bm_config in config["benchmarks"]:
        #     self.benchmarks[bm_config["name"]] = benchmark.configure_benchmark(bm_config, self.destinations,
        #                                                                        self.workflows, self.glx, self)

        # if glx_conf.get("configure_job_destinations", False):
        #     log.info("Creating job_conf for Galaxy and deploying it")
        #     destination.create_galaxy_job_conf(self.glx, self.destinations)
        #     self.glx.deploy_job_conf()

        # if glx_conf["shed_install"]:
        #     self.glx.install_tools_for_workflows(list(self.workflows.values()))

    def run(
        self,
        run_pretasks: bool = True,
        run_benchmarks: bool = True,
        run_posttasks: bool = True,
        filter_benchmarks: list[str] = [],
    ) -> None:
        """Run all benchmarks sequentially

        Steps:
        - Run pre_tasks
        - Run benchmark
        - Print results (optional)
        - Save results to file (optional)
        - Run post_tasks
        """

        for i, benchmark in enumerate(self.benchmarks):
            self.current_benchmark = benchmark
            current_run = f"({i+1}/{len(self.benchmarks)})"

            if filter_benchmarks and benchmark.name not in filter_benchmarks:
                log.info("%s Skipping %s", current_run, benchmark.name)
                continue

            try:
                if run_pretasks:
                    log.info("%s Pre task for %s", current_run, benchmark.name)
                    benchmark.run_pre_tasks()

                if run_benchmarks:
                    log.info("%s Start run for %s", current_run, benchmark.name)
                    benchmark.run()
            except Exception as e:
                log.exception(
                    "Benchmark run failed with exception. Continuing with next benchmark"
                )

            if run_benchmarks:
                # Save results only when actual benchmark ran
                self.save_results_of_current_benchmark()

            if run_posttasks:
                log.info("%s Post task for %s", current_run, benchmark.name)
                benchmark.run_post_tasks()

            time = datetime.now().replace(microsecond=0).isoformat()
            log.info("%s Finished benchmark run at %s", current_run, time)

    def save_results_of_current_benchmark(self) -> None:
        """Save the result of the current benchmark

        Extracted as function for SIGNAL-handling"""
        if not self.current_benchmark:
            log.warning("Nothing to save.")
            return

        if self.config.results_print:
            print(f"#### Results for benchmark {self.current_benchmark}")
            print(self.current_benchmark.benchmark_results)

        if self.config.results_save_to_file:
            file = self.current_benchmark.save_results_to_file(self.results)
            log.info("Saving results to file: '%s'.", file)
