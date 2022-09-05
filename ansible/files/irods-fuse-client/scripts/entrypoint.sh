#!/bin/bash

echo Starting fuse-client...

/irodsfs/irodsfs -config /scripts/config.yml /mnt/volume_under_test

sleep infinity
