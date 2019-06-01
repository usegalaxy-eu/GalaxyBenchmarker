from bioblend.galaxy import GalaxyInstance
import logging

log = logging.getLogger(__name__)


class Galaxy:
    def __init__(self, url, admin_key, ssh_key):
        self.url = url
        self.admin_key = admin_key
        self.ssh_key = ssh_key

        self.instance = GalaxyInstance(url, key=admin_key)

    def impersonate(self, user=None, user_key=None):
        """
        Returns a GalaxyInstance for the given user_key. If user is provided,
        user_key is fetched from Galaxy.
        """
        if user is not None:
            user_id = self.instance.users.get_users(f_name=user)[0]["id"]
            user_key = self.instance.users.get_user_apikey(user_id)
        return GalaxyInstance(self.url, key=user_key)
