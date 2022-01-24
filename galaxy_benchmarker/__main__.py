import argparse
from galaxy_benchmarker.benchmarker import Benchmarker
import logging
import time
import os


# Formatter
fmt_with_time = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fmt_no_time = logging.Formatter('%(name)s - %(levelname)s - %(message)s')

# Create logger
log = logging.getLogger("galaxy_benchmarker")
log.setLevel(logging.INFO)

# Create console handler
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.DEBUG)
stream_handler.setFormatter(fmt_no_time)
log.addHandler(stream_handler)

# Create file handler
log_filename = r'logs/{filename}.log'.format(filename=time.time())
os.makedirs(os.path.dirname(log_filename), exist_ok=True)
file_handler = logging.FileHandler(log_filename, mode='w')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(fmt_with_time)
log.addHandler(file_handler)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=str, default="benchmark_config.yml", help="Path to config file")
    args = parser.parse_args()

    log.debug("Loading Configuration from file {filename}".format(filename=args.config))

    log.info("Initializing Benchmarker.")
    benchmarker = Benchmarker.from_config(args.config)

    log.info("Start benchmarker.")
    benchmarker.run()


if __name__ == '__main__':
    main()
