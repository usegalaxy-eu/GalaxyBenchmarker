from dataclasses import dataclass
from typing import Any, Optional

from serde import serde

from galaxy_benchmarker.benchmarker import BenchmarkerConfig

# Mapping { name -> configdict }
NamedConfigDicts = dict[str, dict[str, Any]]


@serde
@dataclass
class GlobalConfig:
    config: Optional[BenchmarkerConfig]

    tasks: Optional[NamedConfigDicts]
    benchmarks: NamedConfigDicts
