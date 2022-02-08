import logging
import signal
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from serde import serde
from serde.yaml import from_yaml

from galaxy_benchmarker.benchmarks.base import Benchmark
from galaxy_benchmarker.bridge.ansible import AnsibleTask
from galaxy_benchmarker.bridge.galaxy import Galaxy, GalaxyConfig
from galaxy_benchmarker.bridge.influxdb import InfluxDb, InfluxDbConfig
from galaxy_benchmarker.bridge.openstack import OpenStackCompute, OpenStackComputeConfig

log = logging.getLogger(__name__)


@serde
@dataclass
class BenchmarkerConfig:
    openstack: Optional[OpenStackComputeConfig]
    galaxy: Optional[GalaxyConfig]
    influxdb: Optional[InfluxDbConfig]
    tasks: Optional[dict[str, dict]]

    benchmarks: dict[str, dict]
    shared: Optional[list[dict]]

    results_path: str = "results/"
    results_save_to_file: bool = True
    results_save_to_influxdb: bool = False
    results_print: bool = True


class Benchmarker:
    def __init__(self, config: BenchmarkerConfig):
        self.config = config
        self.glx = Galaxy(config.galaxy) if config.galaxy else None
        self.influxdb = InfluxDb(config.influxdb) if config.influxdb else None
        self.openstack = (
            OpenStackCompute(config.openstack) if config.openstack else None
        )
        self.current_benchmark: Optional[Benchmark] = None

        self.tasks: dict[str, AnsibleTask] = {}
        task_configs = self.tasks or {}
        for name, t_config in task_configs:
            self.tasks[name] = AnsibleTask.from_config(t_config, name, self)

        self.benchmarks: list[Benchmark] = []
        for name, b_config in config.benchmarks.items():
            self.benchmarks.append(Benchmark.create(name, b_config, self))

        self.results = Path(config.results_path)
        if self.results.exists():
            if not self.results.is_dir():
                raise ValueError("'results_path' has to be a folder")
        else:
            self.results.mkdir(parents=True)

        if config.results_save_to_influxdb:
            if self.influxdb:
                self.influxdb.test_connection()
            else:
                raise ValueError(
                    "'influxdb' is required when 'results_save_to_influxdb'=True"
                )

        # Safe results in case of interrupt
        def handle_signal(*args):
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

    def run(self):
        """Run all benchmarks sequentially

        Steps:
        - Run pre_tasks
        - Run benchmark
        - Print results (optional)
        - Save results to file (optional)
        - Save results to influxdb (optional)
        - Run post_tasks
        """

        for i, benchmark in enumerate(self.benchmarks):
            self.current_benchmark = benchmark
            current_run = f"({i+1}/{len(self.benchmarks)})"
            log.info("%s Pre task for %s", current_run, benchmark.name)
            benchmark.run_pre_tasks()

            log.info("%s Start run for %s", current_run, benchmark.name)
            benchmark.run()
            self.save_results_of_current_benchmark()

            log.info("%s Post task for %s", current_run, benchmark.name)
            benchmark.run_post_tasks()

    def save_results_of_current_benchmark(self):
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

        if self.config.results_save_to_influxdb:
            log.info("Sending results to influxDB.")
            self.current_benchmark.save_results_to_influxdb(self.influxdb)

    @staticmethod
    def from_config(path: str) -> "Benchmarker":
        """Construct a benchmarker based on a config

        path: Path to yaml
        """
        config_path = Path(path)
        if not config_path.is_file():
            raise ValueError(f"Path to config '{path}' is not a file")

        benchmarker_config = from_yaml(BenchmarkerConfig, config_path.read_text())
        return Benchmarker(benchmarker_config)
