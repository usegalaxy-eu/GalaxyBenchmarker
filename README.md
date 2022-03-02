# Galaxy Benchmarker

TODO...

## Basic architecture

You can run GalaxyBenchmarker locally on your maschine or inside a container. It
mainly uses [Ansible](https://docs.ansible.com/) and [Docker](https://www.docker.com/)
to configure the target hosts. This ensures that the target hosts will be in a clean
state after benchmarking.

### Project structure

```
.
├── ansible            -- Ansible playbooks and inventory
├── examples           -- Example configurations
├── galaxy_benchmarker -- Source code
├── logs               -- Generated logs
├── results            -- Generated results
...
```

## Usage

You can use GalaxyBenchmarker with the prepackaged and preconfigured container
environment or as a local project. For the container env it automatically maps
SSH (config, agent, ...) inside the container. With this you can use your local
SSH config and keys within the inventory.

### Inside container


```bash
# Build and prepare the image(s)
docker-compose build

# Change 'command' in docker-compose.yml to point to your config file
nano docker-compose.yml

# Option 1: Run GalaxyBenchmarker and display progress
docker-compose up -d
docker-compose logs -f benchmarker

# Option 2: Run with make commands
make run
## If you require sudo for docker-compose (-E flag is required!)
make run_sudo

# Gracefull shutdown (stores all results before exiting)
make stop
make stop_sudo
```

### Local

This project requires [Poetry](https://python-poetry.org/docs/).

```bash
# Setup project
poetry env use python3.10
poetry install

# Run GalaxyBenchmarker
poetry run python -m galaxy_benchmarker [... optional args]
```

## Inventory / Hosts / Targets

The targets/hosts are defined in [ansible/inventory/](./ansible/inventory/) as
[Ansible inventory](https://docs.ansible.com/ansible/latest/user_guide/intro_inventory.html).
Here, you can also set host specific variables, if necessary.

The defined hosts can then be used throughout the various benchmarker configs.
