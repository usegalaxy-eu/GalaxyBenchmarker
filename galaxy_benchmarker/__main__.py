import argparse
import logging
import os
import time
from pathlib import Path

from serde.yaml import from_yaml

from galaxy_benchmarker.benchmarker import Benchmarker, BenchmarkerConfig, GlobalConfig
from galaxy_benchmarker.utils import ansible


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--cfgs", nargs="+", default=[], help="Path(s) to config file(s)"
    )
    parser.add_argument("--only-pre-tasks", dest="only_pre_tasks", action="store_true")
    parser.add_argument("--only-benchmark", dest="only_benchmark", action="store_true")
    parser.add_argument(
        "--only-post-tasks", dest="only_post_tasks", action="store_true"
    )
    parser.add_argument("--verbose", dest="verbose", action="store_true")
    parser.set_defaults(
        verbose=False, only_pre_tasks=False, only_benchmark=False, only_post_tasks=False
    )
    parser.add_argument(
        "--benchmarks", nargs="+", default=[], help="List of benchmark(s)"
    )

    args = parser.parse_args()

    log = configure_logger(args.verbose)

    for config_name in args.cfgs:
        cfg_path = Path(config_name)
        if not cfg_path.is_file():
            raise ValueError(f"Path to config '{config_name}' is not a file")

        log.debug(
            "Loading Configuration from file {filename}".format(filename=config_name)
        )

        cfg = from_yaml(GlobalConfig, cfg_path.read_text())

        ansible.AnsibleTask.register(cfg.tasks or {})

        log.info("Initializing Benchmarker.")
        benchmarker = Benchmarker(cfg.config or BenchmarkerConfig(), cfg.benchmarks)

        log.info("Start benchmarker.")
        if args.only_pre_tasks:
            flags = (True, False, False)
        elif args.only_benchmark:
            flags = (False, True, False)
        elif args.only_post_tasks:
            flags = (False, False, True)
        else:
            flags = (True, True, True)

        pre, bench, post = flags
        benchmarker.run(
            run_pretasks=pre,
            run_benchmarks=bench,
            run_posttasks=post,
            filter_benchmarks=args.benchmarks,
        )


def configure_logger(verbose: bool) -> logging.Logger:
    # Formatter
    fmt_with_time = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    fmt_no_time = logging.Formatter("%(name)s - %(levelname)s - %(message)s")

    # Create logger
    log = logging.getLogger("galaxy_benchmarker")
    if verbose:
        log.setLevel(logging.DEBUG)
    else:
        log.setLevel(logging.INFO)

    # Create console handler
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.DEBUG)
    stream_handler.setFormatter(fmt_no_time)
    log.addHandler(stream_handler)

    # Create file handler
    log_filename = r"logs/{filename}.log".format(filename=time.time())
    os.makedirs(os.path.dirname(log_filename), exist_ok=True)
    file_handler = logging.FileHandler(log_filename, mode="w")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(fmt_with_time)
    log.addHandler(file_handler)
    return log


if __name__ == "__main__":
    main()
