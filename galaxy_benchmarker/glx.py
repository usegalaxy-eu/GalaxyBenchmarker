from bioblend.galaxy import GalaxyInstance
import logging

log = logging.getLogger(__name__)


class Galaxy:
    def __init__(self, url, admin_key, ssh_key):
        self.url = url
        self.admin_key = admin_key
        self.ssh_key = ssh_key

        self.instance = GalaxyInstance(url, key=admin_key)
