import novaclient.v2.servers
import novaclient.client
from typing import List
import logging
from dataclasses import dataclass
from serde import serde

log = logging.getLogger("GalaxyBenchmarker")


@serde
@dataclass
class OpenStackComputeConfig:
    auth_url: str
    compute_endpoint_version: str
    password: str
    project_id: str
    region_name: str
    user_domain_name: str
    username: str


class OpenStackCompute:
    def __init__(self, config: OpenStackComputeConfig):
        self.client = novaclient.client.Client(
            config.compute_endpoint_version,
            username=config.username,
            password=config.password,
            project_id=config.project_id,
            auth_url=config.auth_url,
            user_domain_name=config.user_domain_name,
            region_name=config.region_name)

    def get_servers(self, name_contains="") -> List[novaclient.v2.servers.Server]:
        """
        Returns all servers that contain name_contains in their name.
        """
        result = []
        servers = self.client.servers.list()

        for server in servers:
            if server.name.find(name_contains) != -1:
                result.append(server)

        return result

    def reboot_servers(self, servers: List[novaclient.v2.servers.Server], hard=False):
        """
        Reboots the given servers.
        Inspired by: https://gist.github.com/gangwang2/9228989
        """
        reboot_type = "HARD" if hard else "SOFT"
        for server in servers:
            if server.status == 'ACTIVE':
                log.info("Rebooting server {name}".format(name=server.name))
                server.reboot(reboot_type)

    def rebuild_servers(self, servers: List[novaclient.v2.servers.Server]):
        """
        Rebuilds the given servers with the current settings (images, cloud-init, etc).
        """
        for server in servers:
            if server.status == 'ACTIVE':
                log.info("Rebuilding server {name}".format(name=server.name))
                image = server.image["id"]
                server.rebuild(image)
