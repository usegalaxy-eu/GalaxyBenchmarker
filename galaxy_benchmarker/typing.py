from typing import Any

# Mapping { name -> configdict }
NamedConfigDicts = dict[str, dict[str, Any]]

RunName = str
RunResult = dict[str, str | int | float]
RunResults = list[RunResult]

BenchmarkResults = dict[RunName, RunResults]
