#!/bin/bash

echo "#############################"
echo "########   New run   ########"
echo "#############################"

if [ -d "/root/.ssh-host" ]; then
    # Host ssh-config is mounted -> fix permissions
    echo "Fixing permissions for ssh config"

    mkdir -p ~/.ssh
    cp -r /root/.ssh-host/* ~/.ssh/
    chown -R $(id -u):$(id -g) ~/.ssh
fi

# Send SIGNALS to child processes
trap 'echo entrypoint.sh: signal received, stoping child processes; kill -SIGINT $(jobs -p); wait;' SIGINT SIGTERM

# Run benchmarker
python3 -m galaxy_benchmarker &

wait