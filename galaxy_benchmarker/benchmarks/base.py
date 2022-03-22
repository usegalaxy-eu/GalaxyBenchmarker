from __future__ import annotations

import json
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Type, TypeVar

from galaxy_benchmarker.bridge.ansible import AnsibleTask

if TYPE_CHECKING:
    from galaxy_benchmarker.benchmarker import Benchmarker

log = logging.getLogger(__name__)


_registered_benchmarks: dict[str, Type["Benchmark"]] = {}


SubClass = TypeVar("SubClass", bound="Type[Benchmark]")


def register_benchmark(cls: SubClass) -> SubClass:
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
            raise ValueError(f"'repetitions' property is missing for '{name}'")

        try:
            self.repetitions = int(repetitions)
        except ValueError as e:
            raise ValueError(f"'repetitions' has to be a number for '{name}'") from e

        self.id = datetime.now().replace(microsecond=0).isoformat()
        self.benchmark_results: dict[str, Any] = {}

        # Parse pre tasks
        self._pre_tasks: list[AnsibleTask] = []
        if "pre_task" in config:
            if "pre_tasks" in config:
                raise ValueError(f"'pre_task' and 'pre_tasks' given for '{name}'")

            pre_task = AnsibleTask.from_config(config["pre_task"], f"{name}_pre_task")
            self._pre_tasks.append(pre_task)
        elif "pre_tasks" in config:
            for i, t_config in enumerate(config.get("pre_tasks", [])):
                pre_task = AnsibleTask.from_config(t_config, f"{name}_pre_task_{i}")
                self._pre_tasks.append(pre_task)

        # Parse post tasks
        self._post_tasks: list[AnsibleTask] = []
        if "post_task" in config:
            if "post_tasks" in config:
                raise ValueError(f"'post_task' and 'post_tasks' given for '{name}'")

            post_task = AnsibleTask.from_config(
                config["post_task"], f"{name}_post_task"
            )
            self._post_tasks.append(post_task)
        elif "post_tasks" in config:
            for i, t_config in enumerate(config.get("post_tasks", [])):
                post_task = AnsibleTask.from_config(t_config, f"{name}_post_task_{i}")
                self._post_tasks.append(post_task)

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

    def run_pre_tasks(self) -> None:
        """Run setup tasks"""
        for task in self._pre_tasks:
            task.run()

    def run_post_tasks(self) -> None:
        """Run clean up task"""
        for task in self._post_tasks:
            task.run()

    def run(self) -> None:
        """Run benchmark"""
        raise NotImplementedError(
            "Benchmark.run is not defined. Overwrite in child class"
        )

    def save_results_to_file(self, directory: Path) -> str:
        """Write all metrics to a file."""
        file = directory / self.get_result_filename()
        results = {"tags": self.get_tags(), "results": self.benchmark_results}
        json_results = json.dumps(results, indent=2)
        file.write_text(json_results)

        return str(file)

    def get_result_filename(self) -> str:
        return f"{self.id}_{self.name}.json"

    def get_tags(self) -> dict[str, str]:
        return {
            "plugin": "benchmarker",
            "benchmark_name": self.name,
            "benchmark_id": self.id,
            "benchmark_type": self.__class__.__name__,
        }

    def __str__(self):
        return self.name

    __repr__ = __str__


@register_benchmark
class SetupTimeBenchmark(Benchmark):
    """Compare the setuptime/ansible connection time between different destinations.

    Useful for basic setup tests and to check if everything is configured correctly
    """

    def __init__(self, name: str, config: dict, benchmarker: Benchmarker):
        super().__init__(name, config, benchmarker)

        if "hosts" not in config:
            raise ValueError(
                f"'hosts' property (type: list[str]) is missing for '{name}'"
            )

        self.hosts: list[str] = config["hosts"]

        if not all(isinstance(value, str) for value in self.hosts):
            raise ValueError("'hosts' property has to be of type list[str]")

        self._run_task = AnsibleTask(playbook="connection_test.yml")

    def run(self):
        """Run the connection_test playbook on each destination"""

        for host in self.hosts:
            log.info("Start %s for %s", self.name, host)
            results = []
            for i in range(self.repetitions):
                log.info("Run %d of %d", i + 1, self.repetitions)
                start_time = time.monotonic()

                self._run_task.run_at(host)

                total_runtime = time.monotonic() - start_time
                results.append({"runtime_in_s": total_runtime})
            self.benchmark_results[host] = results
