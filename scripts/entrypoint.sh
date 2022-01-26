#!/bin/bash

if [ -d "/root/.ssh-host" ]; then
    # Host ssh-config is mounted -> fix permissions
    echo "Fixing permissions for ssh config"

    mkdir -p ~/.ssh
    cp -r /root/.ssh-host/* ~/.ssh/
    chown -R $(id -u):$(id -g) ~/.ssh
fi

# Run benchmarker
python3 -m galaxy_benchmarker