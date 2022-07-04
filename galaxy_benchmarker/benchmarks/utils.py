from __future__ import annotations

from galaxy_benchmarker.benchmarks import base
from typing import TYPE_CHECKING
import threading
from pathlib import Path
import json
from functools import wraps
import logging

if TYPE_CHECKING:
    from galaxy_benchmarker.benchmarker import Benchmarker

log = logging.getLogger(__name__)

@base.register_benchmark
class BenchmarkCompare(base.Benchmark):
    """
    Compare two benchmarks side by side.
    """

    def __init__(self, name: str, config: dict, benchmarker: Benchmarker):
        super().__init__(name, config, benchmarker)

        conf_a = config.get("bench_a", {})
        if not conf_a:
            raise ValueError(
                f"Property 'bench_a' (dict) is missing for {name}. Must be a valid benchmark config"
            )
        conf_b = config.get("bench_b", {})
        if not conf_b:
            raise ValueError(
                f"Property 'bench_b' (dict) is missing for {name}. Must be a valid benchmark config"
            )
        self.bench_a = base.Benchmark.create(f"{name}_a", conf_a, benchmarker)
        self.bench_b = base.Benchmark.create(f"{name}_b", conf_b, benchmarker)


    def run_pre_tasks(self) -> None:
        """Run setup tasks"""
        self.bench_a.run_pre_tasks()
        self.bench_b.run_pre_tasks()

    def run_post_tasks(self) -> None:
        """Run clean up task"""
        self.bench_a.run_post_tasks()
        self.bench_b.run_post_tasks()

    def run(self):
        """Run two benchmarks in the following order: a1, b1, a2, b2, ..."""
        sync_lock = threading.Condition()
        sync_required = True

        def run_at_wrapper(method, note):
            """Wrapper for _ran_at

            Wait after each _run_at-call so the other benchmark can run
            """
            @wraps(method)
            def _impl(self, *method_args, **method_kwargs):
                with sync_lock:
                    log.info("Start run in thread %s", note)
                    method_output = method(self, *method_args, **method_kwargs)
                    sync_lock.notify()
                    if sync_required:
                        sync_lock.wait()
                return method_output
            return _impl

        def run_in_thread(method):
            """Wrapper for thread call

            After call to run() has finished notify the other thread so it can
            also finish
            """
            def _impl():
                method_output = method()
                global sync_required
                sync_required = False
                with sync_lock:
                    sync_lock.notify()
                return method_output
            return _impl

        _bench_a_run_at = self.bench_a._run_at
        _bench_b_run_at = self.bench_b._run_at

        try:
            # Inject threading mechanism to interleave execution of both benchmarks
            self.bench_a._run_at = run_at_wrapper(_bench_a_run_at, "BenchA")
            self.bench_b._run_at = run_at_wrapper(_bench_b_run_at, "BenchB")

            # creating threads
            t1 = threading.Thread(target=run_in_thread(self.bench_a.run))
            t2 = threading.Thread(target=run_in_thread(self.bench_b.run))
            t1.start()
            t2.start()

            # wait until threads finish their job
            t1.join()
            t2.join()
        finally:
            # Restore original functions
            self.bench_a._run_at = _bench_a_run_at
            self.bench_b._run_at = _bench_b_run_at


    def save_results_to_file(self, directory: Path) -> str:
        """Write all metrics to a file."""
        file = directory / self.result_file.name
        results = {"tags": self.get_tags()}
        json_results = json.dumps(results, indent=2)
        file.write_text(json_results)

        self.bench_a.save_results_to_file(directory)
        self.bench_b.save_results_to_file(directory)

        return str(file)

    def get_tags(self) -> dict[str, str]:
        return {
            "plugin": "benchmarker",
            "benchmark_name": self.name,
            "benchmark_id": self.id,
            "benchmark_type": self.__class__.__name__,
            "bench_a": self.bench_a.get_tags(),
            "bench_a_results": self.bench_a.result_file.name,
            "bench_b": self.bench_b.get_tags(),
            "bench_b_results": self.bench_b.result_file.name,
        }
