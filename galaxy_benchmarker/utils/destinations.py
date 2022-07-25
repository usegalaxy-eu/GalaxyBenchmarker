import dataclasses


@dataclasses.dataclass
class BenchmarkDestination:
    host: str = ""

    def __post_init__(self):
        if not self.host:
            raise ValueError(f"Property 'host' is missing for BenchmarkDestination")

    @property
    def name(self):
        return f"{self.host}"


@dataclasses.dataclass
class PosixBenchmarkDestination(BenchmarkDestination):
    target_folder: str = ""

    def __post_init__(self):
        super().__post_init__()
        if not self.target_folder:
            raise ValueError(
                f"Property 'target_folder' is missing for host '{self.host}'"
            )

    @property
    def name(self):
        return f"{self.host}_{self.target_folder.replace('/','_')}"
