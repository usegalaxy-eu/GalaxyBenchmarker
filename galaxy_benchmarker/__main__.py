import yaml
import argparse
import sys
import bioblend
from time import sleep
from benchmarker import Benchmarker
import logging
import time
import requests
import os
from requests.adapters import HTTPAdapter

logging.basicConfig()
log = logging.getLogger("GalaxyBenchmarker")
log.setLevel(logging.INFO)
log_handler = logging.StreamHandler(sys.stdout)
log_handler.setLevel(logging.DEBUG)

# Log to file
log_filename = r'logs/{filename}.log'.format(filename=time.time())
os.makedirs(os.path.dirname(log_filename), exist_ok=True)
fh = logging.FileHandler(log_filename, mode='w')
log.addHandler(fh)

s = requests.Session()
s.mount('http://', HTTPAdapter(max_retries=20))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=str, default="benchmark_config.yml", help="Path to config file")
    args = parser.parse_args()

    log.debug("Loading Configuration from file {filename}".format(filename=args.config))
    with open(args.config, "r") as stream:
        try:
            config = yaml.safe_load(stream)

            log.info("Initializing Benchmarker.")
            benchmarker = Benchmarker(config)

            benchmarker.run_pre_tasks()

            log.info("Starting to run benchmarks.")
            try:
                benchmarker.run()
            except bioblend.ConnectionError:
                log.error("There was a problem with the connection. Benchmark canceled.")

            results_filename = "results/results_{time}".format(time=time.time())
            log.info("Saving results to file: '{filename}.json'.".format(filename=results_filename))
            os.makedirs(os.path.dirname(results_filename), exist_ok=True)
            benchmarker.save_results(results_filename)

            if benchmarker.inflx_db is not None:
                log.info("Sending results to influxDB.")
                benchmarker.send_results_to_influxdb()

            benchmarker.run_post_tasks()

        except yaml.YAMLError as exc:
            print(exc)
        except IOError as err:
            print(err)


if __name__ == '__main__':
    main()
