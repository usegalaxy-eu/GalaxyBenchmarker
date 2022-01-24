from typing import Optional
from galaxy_benchmarker.models import workflow
from galaxy_benchmarker.models import destination
from galaxy_benchmarker.models import benchmark
from galaxy_benchmarker.bridge.galaxy import Galaxy, GalaxyConfig
from galaxy_benchmarker.bridge.influxdb import InfluxDb, InfluxDbConfig
from galaxy_benchmarker.bridge.openstack import OpenStackCompute, OpenStackComputeConfig
from galaxy_benchmarker.benchmarks.base import Benchmark
import logging
import json
from dataclasses import dataclass
from pathlib import Path

from serde import serde
from serde.yaml import from_yaml

log = logging.getLogger(__name__)


@serde
@dataclass
class BenchmarkerConfig:
    openstack: Optional[OpenStackComputeConfig]
    galaxy: GalaxyConfig
    influxdb: Optional[InfluxDbConfig]
    benchmarks: list[dict]
    shared: Optional[list[dict]]


class Benchmarker:
    def __init__(self, config: BenchmarkerConfig):
        self.config = config
        self.glx = Galaxy(config.galaxy)
        self.influxdb = InfluxDb(config.influxdb) if config.influxdb else None
        self.openstack = OpenStackCompute(config.openstack) if config.openstack else None

        self.benchmarks = [Benchmark.create(b_config, config) for b_config in config.benchmarks]

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

    def run_pre_tasks(self):
        log.info("Running pre-tasks for benchmarks")
        for bm in self.benchmarks:
            bm.run_pre_task()

    def run_post_tasks(self):
        log.info("Running post-tasks for benchmarks")
        for bm in self.benchmarks:
            bm.run_post_task()

    def run(self):
        for bm in self.benchmarks:
            log.info("Running benchmark '{bm_name}'".format(bm_name=bm.name))
            bm.run(self)

    def get_results(self):
        for bm in self.benchmarks.values():
            print(bm.benchmark_results)

    def save_results(self, filename="results"):
        results = list()
        for bm in self.benchmarks.values():
            results.append(bm.benchmark_results)

        json_results = json.dumps(results, indent=2)
        with open(filename+".json", "w") as fh:
            fh.write(json_results)

    def send_results_to_influxdb(self):
        for bm in self.benchmarks.values():
            bm.save_results_to_influxdb(self.inflx_db)

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
