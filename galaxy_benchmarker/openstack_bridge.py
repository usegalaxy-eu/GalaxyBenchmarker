import novaclient.v2.servers
import novaclient.client
from typing import List
import logging

log = logging.getLogger("GalaxyBenchmarker")


class OpenStackCompute:
    def __init__(self, auth_url, compute_endpoint_version, username, password,
                 project_id, region_name, user_domain_name):
        self.client = novaclient.client.Client(compute_endpoint_version, username=username, password=password,
                                               project_id=project_id, auth_url=auth_url,
                                               user_domain_name=user_domain_name, region_name=region_name)

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
