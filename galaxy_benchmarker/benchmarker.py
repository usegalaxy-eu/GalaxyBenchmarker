from typing import Dict
import workflow
import destination
import benchmark
import glx
import logging

log = logging.getLogger(__name__)


class Benchmarker:
    glx: glx.Galaxy
    workflows: Dict[str, workflow.BaseWorkflow]
    destinations: Dict[str, destination.BaseDestination]
    benchmarks: Dict[str, benchmark.BaseBenchmark]

    def __init__(self, config):
        self.glx = glx.Galaxy(config["galaxy"]["url"], config["galaxy"]["admin_key"], config["galaxy"]["ssh_key"])

        self.workflows = dict()
        for wf_config in config["workflows"]:
            self.workflows[wf_config["name"]] = workflow.configure_workflow(wf_config)

        self.destinations = dict()
        for dest_config in config["destinations"]:
            self.destinations[dest_config["name"]] = destination.configure_destination(dest_config)

        self.benchmarks = dict()
        for bm_config in config["benchmarks"]:
            self.benchmarks[bm_config["name"]] = benchmark.configure_benchmark(bm_config, self.destinations,
                                                                               self.workflows, self.glx)

    def run(self):
        for bm in self.benchmarks.values():
            log.info("Running benchmark '{bm_name}'".format(bm_name=bm.name))
            bm.run(self)
