from __future__ import annotations
import logging
from typing import Type, Optional, TYPE_CHECKING
from datetime import datetime
from galaxy_benchmarker.bridge.influxdb import InfluxDb

if TYPE_CHECKING:
    from galaxy_benchmarker.benchmarker import Benchmarker

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

    def __init__(self, name: str, config: dict, benchmarker: Benchmarker):
        self.name = name

        repetitions = config.get("repetitions", None)
        if not repetitions:
            raise ValueError(f"'repetitions' property is missing for {config}")

        try:
            self.repetitions = int(repetitions)
        except ValueError as e:
            raise ValueError(f"'repetitions' has to be a number for {config}") from e


        self.uuid = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f") + "_" + self.name
        self.benchmark_results = {}

    @staticmethod
    def create(name: str, config: dict, benchmarker: Benchmarker):
        """Factory method for benchmarks

        name: benchmark name
        config: benchmark specific config
        global_config: global config for lookups
        """

        benchmark_type = config.get("type", None)
        if benchmark_type is None:
            raise ValueError(f"'type' property is missing for benchmark {config}")

        if benchmark_type not in _registered_benchmarks:
            raise ValueError(f"Unkown benchmark type: {benchmark_type}")

        benchmark_class = _registered_benchmarks[benchmark_type]
        return benchmark_class(name, config, benchmarker)


    def run_pre_tasks(self):
        """Run setup tasks"""

    def run_post_tasks(self):
        """Run clean up task"""

    def run(self):
        """Run benchmark"""
        raise NotImplementedError("Benchmark.run is not defined. Overwrite in child class")

    def save_results_to_influxdb(self, inflxdb: InfluxDb):
        """Send all metrics to influxDB."""
        raise NotImplementedError("Benchmark.save_results_to_influxdb is not defined. Overwrite in child class")


    def __str__(self):
        return self.name

    __repr__ = __str__
