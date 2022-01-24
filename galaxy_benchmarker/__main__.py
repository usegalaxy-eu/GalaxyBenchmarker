import argparse
import sys
from galaxy_benchmarker.benchmarker import Benchmarker
import logging
import time
import os

logging.basicConfig()
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)
log_handler = logging.StreamHandler(sys.stdout)
log_handler.setLevel(logging.DEBUG)

# Log to file
log_filename = r'logs/{filename}.log'.format(filename=time.time())
os.makedirs(os.path.dirname(log_filename), exist_ok=True)
fh = logging.FileHandler(log_filename, mode='w')
log.addHandler(fh)



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
