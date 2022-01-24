from __future__ import annotations
import logging
from typing import Type, Optional, TYPE_CHECKING
from datetime import datetime
from galaxy_benchmarker.bridge.influxdb import InfluxDb

if TYPE_CHECKING:
    from galaxy_benchmarker.benchmarker import BenchmarkerConfig

log = logging.getLogger(__name__)

_registered_benchmarks: dict[str,Type["Benchmark"]] = {}

def register_benchmark(cls: Type):
    """Register a benchmark for factory method"""

    name = cls.__name__
    log.debug("Registering benchmark %s", name)

    if name in _registered_benchmarks:
        module = cls.__module__
        raise ValueError(f"Already registered. Use another name for {module}.{name}")

    _registered_benchmarks[name] = cls

    return cls

class Benchmark:
    """
    The Base-Class of Benchmark. All Benchmarks should inherit from it.
    """

    def __init__(self, config: dict, global_config: BenchmarkerConfig):
        self.name:str = config.get("name", "")
        if not self.name:
            raise ValueError(f"'name' property is missing for benchmark {config}")

        runs_per_workflow = config.get("runs_per_workflow", None)
        if not runs_per_workflow:
            raise ValueError(f"'runs_per_workflow' property is missing for {config}")

        try:
            self.runs_per_workflow = int(runs_per_workflow)
        except ValueError as e:
            raise ValueError(f"'runs_per_workflow' has to be a number for {config}") from e


        self.uuid = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f") + "_" + self.name
        self.benchmark_results = {}

        #TODO: pre/post_tasks
        self.pre_tasks = []
        self.post_tasks = []

    @classmethod
    def create(cls, config: dict, global_config: BenchmarkerConfig):
        """Factory method for benchmarks
        
        config: benchmark specific config
        global_config: global config for lookups
        """

        benchmark_type = config.get("type", None)
        if benchmark_type is None:
            raise ValueError(f"'type' property is missing for benchmark {config}")

        if benchmark_type not in _registered_benchmarks:
            raise ValueError(f"Unkown benchmark type: {benchmark_type}")

        benchmark_class = _registered_benchmarks[benchmark_type]
        return benchmark_class(config, global_config)


    def run_pre_task(self):
        """Run setup tasks"""
        for task in self.pre_tasks:
            log.info("Pre-task: Running {task}".format(task=task))
            task.run()

    def run_post_task(self):
        """Run clean up task"""
        for task in self.post_tasks:
            log.info("Post-task: Running {task}".format(task=task))
            task.run()

    def run(self):
        """Run benchmark"""
        raise NotImplementedError("Benchmark.run is not defined. Overwrite in child class")

    def save_results_to_influxdb(self, inflxdb: InfluxDb):
        """Send all metrics to influxDB."""
        raise NotImplementedError("Benchmark.save_results_to_influxdb is not defined. Overwrite in child class")


    def __str__(self):
        return self.name

    __repr__ = __str__
