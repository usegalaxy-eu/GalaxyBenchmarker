import argparse
import logging
import os
import time
from pathlib import Path

from serde.yaml import from_yaml

from galaxy_benchmarker import config
from galaxy_benchmarker.benchmarker import Benchmarker, BenchmarkerConfig
from galaxy_benchmarker.bridge import ansible


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--cfg", type=str, default="benchmark_config.yml", help="Path to config file"
    )
    parser.add_argument("--verbose", dest="verbose", action="store_true")
    parser.set_defaults(verbose=False)

    args = parser.parse_args()

    log = configure_logger(args.verbose)

    cfg_path = Path(args.cfg)
    if not cfg_path.is_file():
        raise ValueError(f"Path to config '{args.cfg}' is not a file")

    log.debug("Loading Configuration from file {filename}".format(filename=args.cfg))

    cfg = from_yaml(config.GlobalConfig, cfg_path.read_text())

    ansible.AnsibleDestination.register(cfg.destinations or {})
    ansible.AnsibleTask.register(cfg.tasks or {})

    log.info("Initializing Benchmarker.")
    benchmarker = Benchmarker(cfg.config or BenchmarkerConfig(), cfg.benchmarks)

    log.info("Start benchmarker.")
    benchmarker.run()


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
