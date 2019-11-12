# Galaxy Benchmarker
A tool for benchmarking Galaxy job-destinations.

The goal is to easily benchmark different job-destinations. All you have to do is to configure the benchmarker itself,
define the destinations you want to benchmark, the workflows you want to run and the benchmarks you want to use. GalaxyBenchmarker
should handle the rest, like configuring Galaxy (to send the jobs to the right destination), submitting workflows and
collecting the metrics.

GalaxyBenchmarker is designed to be easily extendable in terms of destinations-types and benchmark-scenarios.

### Benchmark-Scenarios
Currently there are three types/scenarios of benchmarks available. Every benchmark allows to define its destination and workflows to test, how often workflows should
be run per destination and if some ansible-playbooks should be run before or after the benchmark ran.
#### Cold vs Warm
What is the difference between running a workflow for the first time or for it having been run multiple
times already? What is the overhead for staging time, installing tools, etc?

This benchmark cleans up Pulsar for every cold run, using a Ansible-Playbook ([coldwarm_pretask.yml]).

#### Destination Comparison
What are the differences between multiple destinations in terms of staging time (sending data
to a remote location might take some time), runtime, etc.

#### Burst
How does a destination handle a big burst of requests?


## Requirements
* A Galaxy-Instance with admin rights
* InfluxDB for saving the benchmark results
* Some job-destinations to benchmark
* Python 3.7
* Ansible

## Usage
### Install all dependencies
Some additional packages are needed in order for GalaxyBenchmarker to function properly. 
To install those, run the following command:
```shell
pip3 install -r requirements.txt
```

### Configure the GalaxyBenchmarker
To use the benchmarker, you first need to create a yaml-configuration file.
We need to have access to a Galaxy instance in order to submit workflows:
```yaml
galaxy:
  url: https://usegalaxy.eu
  user_key: YOUR-KEY
```

Next, we need to define the workflows that should be run. The benchmarker
uses the test functionality of [Planemo](https://github.com/galaxyproject/planemo) to 
submit workflows. Examples can be found at: https://github.com/usegalaxy-eu/workflow-testing. For a start, you
can clone this repository and use those workflows for benchmarking. Workflows are defined in the
configuration as following:
```yaml
workflows:
  - name: ARDWorkflow
    type: Galaxy
    path: /../workflow-testing/sklearn/ard/ard.ga
```

The benchmarker can submit jobs to multiple job destinations to compare the performance of each. For routing
the jobs to the right destination, users can be defined for each. The GalaxyBenchmarker can take 
care of that 
(see the [configuration examples](https://github.com/AndreasSko/Galaxy-Benchmarker/blob/master/benchmark_config.yml.example)) 
if you are an administrator for the Galaxy instance. Otherwise, it is also possible to use different
users for each destination and link them in the configuration of the benchmarker:
```yaml
destinations:
  - name: Destination1
    type: Galaxy
    galaxy_user_name: USERNAME
    galaxy_user_key: USERKEY
```

Now, we just need to define the actual benchmark that we want to perform. To compare different job destinations, 
you can do the following:
```yaml
benchmarks:
  - name: DestinationComparisonBenchmark
    type: DestinationComparison
    destinations:
      - Destination1
    workflows:
      - ARDWorkflow
    runs_per_workflow: 1
```

### Run the benchmarks
The GalaxyBenchmarker can use different configuration files. If none is given, it will look for `benchmark_config.yml`
```shell
python3 galaxy_benchmarker --config benchmark_config.yml
```

## Additional options
All possible options can be found in the [configuration examples](https://github.com/AndreasSko/Galaxy-Benchmarker/blob/master/benchmark_config.yml.example).

### InfluxDB
In normal cases, GalaxyBenchmarker will save the results in a json file under the `results` directory. However, job
metrics can also be submitted to InfluxDB for further analysis.
```yaml
influxdb:
  host: influxdb.example.com
  port: 8086
  username: glx_benchmarker_user
  password: supersecret
  db_name: glx_benchmarker
```
Example Dashboards for Grafana can be found at [grafana_dashboards](https://github.com/AndreasSko/Galaxy-Benchmarker/tree/master/grafana_dashboards)
