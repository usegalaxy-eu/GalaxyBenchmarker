from typing import Optional
from galaxy_benchmarker.bridge.galaxy import Galaxy, GalaxyConfig
from galaxy_benchmarker.bridge.influxdb import InfluxDb, InfluxDbConfig
from galaxy_benchmarker.bridge.openstack import OpenStackCompute, OpenStackComputeConfig
from galaxy_benchmarker.benchmarks.base import Benchmark
from galaxy_benchmarker.models.task import Task
import logging
import json
from dataclasses import dataclass
from pathlib import Path
from datetime import datetime

from serde import serde
from serde.yaml import from_yaml

log = logging.getLogger(__name__)


@serde
@dataclass
class BenchmarkerConfig:
    openstack: Optional[OpenStackComputeConfig]
    galaxy: GalaxyConfig
    influxdb: Optional[InfluxDbConfig]
    tasks: Optional[dict[str, dict]]

    benchmarks: dict[str, dict]
    shared: Optional[list[dict]]

    results_path: str = "results/"
    results_save_to_file: bool = True
    results_save_to_influxdb: bool = True
    results_print: bool = True


class Benchmarker:
    def __init__(self, config: BenchmarkerConfig):
        self.config = config
        self.glx = Galaxy(config.galaxy)
        self.influxdb = InfluxDb(config.influxdb) if config.influxdb else None
        self.openstack = OpenStackCompute(config.openstack) if config.openstack else None

        self.tasks: dict[str, Task] = {}
        for name, t_config in config.tasks.items():
            self.tasks[name] = Task.create(name, t_config)

        self.benchmarks: list[Benchmark] = []
        for name, b_config in config.benchmarks.items():
            self.benchmarks.append(Benchmark.create(name, b_config, config))

        self.results = Path(config.results_path)
        if self.results.exists():
            if not self.results.is_dir():
                raise ValueError("'results_path' has to be a folder")
        else:
            self.results.mkdir(parents=True)

        if config.results_save_to_influxdb and not self.influxdb:
            raise ValueError("'influxdb' is required when 'results_save_to_influxdb'=True")

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

        for benchmark in self.benchmarks:
            log.info("Pre task for %s", benchmark.name)
            benchmark.run_pre_task()

            log.info("Start run for %s", benchmark.name)
            benchmark.run()

            if self.config.results_print:
                print(benchmark.benchmark_results)

            if self.config.results_save_to_file:
                self._save_results_to_file(benchmark)

            if self.config.results_save_to_influxdb:
                log.info("Sending results to influxDB.")
                benchmark.save_results_to_influxdb(self.inflx_db)

            log.info("Post task for %s", benchmark.name)
            benchmark.run_post_tasks()

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

    def _save_results_to_file(self, benchmark: Benchmark):
        """Save results as json to file"""
        # Construct filename
        timestamp = datetime.now().replace(microsecond=0)
        filename = f"{timestamp.isoformat()}_{benchmark}.json"
        file =  self.results / filename

        log.info("Saving results to file: '%s'.", file)

        # Write results
        json_results = json.dumps(benchmark.benchmark_results, indent=2)
        file.write_text(json_results)
