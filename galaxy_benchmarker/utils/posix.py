import dataclasses

@dataclasses.dataclass
class PosixBenchmarkDestination:
    host: str = ""
    target_folder: str = ""

    def __post_init__(self):
        if not self.host:
            raise ValueError(
                f"Property 'host' is missing for BenchmarkDestination"
            )
        if not self.target_folder:
            raise ValueError(
                f"Property 'target_folder' is missing for host '{self.host}'"
            )

    @property
    def name(self):
        return f"{self.host}_{self.target_folder.replace('/','_')}"

