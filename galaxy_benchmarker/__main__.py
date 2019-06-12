import yaml
import argparse
import sys
from benchmarker import Benchmarker
import logging
import time

logging.basicConfig()
log = logging.getLogger("GalaxyBenchmarker")
log.setLevel(logging.INFO)
log_handler = logging.StreamHandler(sys.stdout)
log_handler.setLevel(logging.DEBUG)

logging.getLogger("ephemeris").setLevel(logging.WARNING)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=str, default="benchmark_config.yml", help="Path to config file")
    args = parser.parse_args()

    log.debug("Loading Configuration from file {filename}".format(filename=args.config))
    with open(args.config, "r") as stream:
        try:
            config = yaml.safe_load(stream)

            log.info("Initializing Benchmarker")
            benchmarker = Benchmarker(config)

            log.info("Starting to run benchmarks")
            benchmarker.run()

            benchmarker.save_results("results/results_{time}".format(time=time.time()))
            benchmarker.send_results_to_influxdb()

        except yaml.YAMLError as exc:
            print(exc)
        except IOError as err:
            print(err)


if __name__ == '__main__':
    main()
