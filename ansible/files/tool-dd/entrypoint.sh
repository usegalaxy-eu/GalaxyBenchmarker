#!/bin/bash

dd bs=$DD_BS if=$DD_IF of=$DD_OF conv=fsync count=$DD_COUNT &
dd bs=$DD_BS if=$DD_IF of=$DD_OF conv=fsync count=$DD_COUNT seek=$DD_SEEK skip=$DD_SKIP &
wait
