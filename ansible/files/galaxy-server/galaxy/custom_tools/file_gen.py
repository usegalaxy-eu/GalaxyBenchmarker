#!/usr/bin/env python
# Generate files with random data

import sys
import io
import os
from pathlib import Path
import random

BUFFER_BLOCK_SIZE = 1024*1024 # 1MiB
BUFFER_NUM_BLOCKS = 1024      # Total size: 1 GiB

def fill_file_from_buffer(file: Path, buffer: io.BytesIO, file_size: int):
    """Fill file with random blocks from buffer until file_size is reached"""

    num_blocks, rest = divmod(file_size, BUFFER_BLOCK_SIZE)
    with file.open("wb") as out:
        # Copy num_blocks random blocks
        for _ in range(num_blocks):
            rand_block = random.randint(0,BUFFER_NUM_BLOCKS-1)
            buffer.seek(rand_block)
            out.write(buffer.read(BUFFER_BLOCK_SIZE))

        # Write the last block
        rand_block = random.randint(0,BUFFER_NUM_BLOCKS-1)
        buffer.seek(rand_block)
        out.write(buffer.read(rest))

def main():

    num_files = int(sys.argv[1])
    file_size_in_bytes = int(sys.argv[2])
    output_dir = Path(sys.argv[3])

    output_dir.mkdir(parents=True, exist_ok=True)

    with io.BytesIO() as buffer:
        if file_size_in_bytes > 0:
            # Create a buffer with random data of size
            # BUFFER_BLOCK_SIZE*BUFFER_NUM_BLOCKS
            for _ in range (BUFFER_NUM_BLOCKS):
                buffer.write(os.urandom(BUFFER_BLOCK_SIZE))
            buffer.seek(0)

        for i in range(num_files):
            file = output_dir / f"data_{i}.data"
            fill_file_from_buffer(file, buffer, file_size_in_bytes)


if __name__ == "__main__":
    main()
