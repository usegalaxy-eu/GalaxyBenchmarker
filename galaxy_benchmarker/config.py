from dataclasses import dataclass
from typing import Optional

from serde import serde

from galaxy_benchmarker.benchmarker import BenchmarkerConfig

# Mapping { name -> configdict }
NamedConfigDicts = dict[str, dict]


@serde
@dataclass
class GlobalConfig:
    config: Optional[BenchmarkerConfig]

    destinations: Optional[NamedConfigDicts]
    tasks: Optional[NamedConfigDicts]
    benchmarks: NamedConfigDicts
