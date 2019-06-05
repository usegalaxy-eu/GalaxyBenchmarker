from typing import Dict
import workflow
import destination
import benchmark
from galaxy_bridge import Galaxy
import logging
import json

log = logging.getLogger("GalaxyBenchmarker")


class Benchmarker:
    glx: Galaxy
    workflows: Dict[str, workflow.BaseWorkflow]
    destinations: Dict[str, destination.BaseDestination]
    benchmarks: Dict[str, benchmark.BaseBenchmark]

    def __init__(self, config):
        self.glx = Galaxy(config["galaxy"]["url"], config["galaxy"]["admin_key"],
                          config["galaxy"]["ssh_user"], config["galaxy"]["ssh_key"],
                          config["galaxy"]["galaxy_root_path"], config["galaxy"]["galaxy_config_dir"],
                          config["galaxy"]["galaxy_user"])

        self.workflows = dict()
        for wf_config in config["workflows"]:
            self.workflows[wf_config["name"]] = workflow.configure_workflow(wf_config)

        self.destinations = dict()
        for dest_config in config["destinations"]:
            self.destinations[dest_config["name"]] = destination.configure_destination(dest_config, self.glx)

        self.benchmarks = dict()
        for bm_config in config["benchmarks"]:
            self.benchmarks[bm_config["name"]] = benchmark.configure_benchmark(bm_config, self.destinations,
                                                                               self.workflows, self.glx)
        log.info("Creating job_conf for Galaxy and deploying it")
        destination.create_galaxy_job_conf(self.glx, self.destinations)
        self.glx.deploy_job_conf()

    def run(self):
        for bm in self.benchmarks.values():
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
