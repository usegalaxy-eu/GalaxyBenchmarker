# Galaxy Benchmarker
A tool for benchmarking Galaxy job-destinations. 

The goal is to easily benchmark different job-destinations. All you have to do is to configure the benchmarker itself,
define the destinations you want to benchmark, the workflows you want to run and the benchmarks you want to use. GalaxyBenchmarker
should handle the rest, like configuring Galaxy (to send the jobs to the right destination), submitting workflows and
collecting the metrics.

GalaxyBenchmarker is designed to be easily extendable in terms of destinations-types and benchmark-scenarios.

### Benchmark-Scenarios
Currently there are three types/scenarios of benchmarks available:
#### Cold vs Warm
What is the difference between running a workflow for the first time or for it having been run multiple
times already? What is the overhead for staging time, installing tools, etc?

This benchmark cleans up Pulsar for every cold run, using a Ansible-Playbook ([coldwarm_pretask.yml]). 

#### Destination Comparison
What are the differences between multiple destinations in terms of staging time (sending data
to a remote location might take some time), runtime, etc.

#### Burst
How does a destination handle a big burst of requests?


Every benchmark allows to define its destination and workflows to test, how often workflows should
be run per destination and if some ansible-playbooks should be run before or after the benchmark ran.

## Requirements
* A Galaxy-Instance with admin rights
* InfluxDB for saving the benchmark results
* Some job-destinations to benchmark
* Python 3.7
* Ansible

## Usage
* Configure the benchmarker. Examples can be found in [benchmark_config.yml.example].
* Run 

        python galaxy_benchmarker
    
* Analyze metrics in InfluxDB. Example Dashboards for Grafana can be found at [./grafana_dashboards] 