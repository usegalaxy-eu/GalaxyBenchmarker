#!/bin/bash

dd bs=$DD_BS if=$DD_IF of=$DD_OF conv=fsync count=$DD_COUNT &
dd bs=$DD_BS if=$DD_IF of=$DD_OF conv=fsync count=$DD_COUNT seek=$DD_SEEK_1 skip=$DD_SKIP_1 &
dd bs=$DD_BS if=$DD_IF of=$DD_OF conv=fsync count=$DD_COUNT seek=$DD_SEEK_2 skip=$DD_SKIP_2 &
dd bs=$DD_BS if=$DD_IF of=$DD_OF conv=fsync count=$DD_COUNT seek=$DD_SEEK_3 skip=$DD_SKIP_3 &
wait
