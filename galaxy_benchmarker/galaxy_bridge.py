from bioblend.galaxy import GalaxyInstance
import ansible_bridge
import logging
import string
import random
import re

log = logging.getLogger("GalaxyBenchmarker")


class Galaxy:
    def __init__(self, url, admin_key, ssh_user, ssh_key, galaxy_root_path, galaxy_config_dir, galaxy_user):
        self.url = url
        self.admin_key = admin_key
        self.ssh_user = ssh_user
        self.ssh_key = ssh_key

        self.galaxy_root_path = galaxy_root_path
        self.galaxy_config_dir = galaxy_config_dir
        self.galaxy_user = galaxy_user

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

    def create_user(self, username):
        """
        Creates a new user (if not already created) with username and a random password and returns
        its user_id and api_key
        """
        new_user = False
        if len(self.instance.users.get_users(f_name=username)) == 0:
            password = ''.join([random.choice(string.ascii_letters + string.digits) for _ in range(32)])
            self.instance.users.create_local_user(username,
                                                  "{username}@galaxy.uni.andreas-sk.de".format(username=username),
                                                  password)
            new_user = True

        user_id = self.instance.users.get_users(f_name=username)[0]["id"]
        user_key = self.instance.users.create_user_apikey(user_id) if new_user \
            else self.instance.users.get_user_apikey(user_id)

        return user_id, user_key

    def deploy_job_conf(self):
        host = re.findall("^[a-z][a-z0-9+\-.]*://([a-z0-9\-._~%!$&'()*+,;=]+@)?([a-z0-9\-._~%]+|\[[a-z0-9\-."
                          + "_~%!$&'()*+,;=:]+\])", self.url)[0][1]
        values = {
            "galaxy_root_path": self.galaxy_root_path,
            "galaxy_config_dir": self.galaxy_config_dir,
            "galaxy_user": self.galaxy_user
        }
        ansible_bridge.run_playbook("prepare_galaxy.yml", host, self.ssh_user, self.ssh_key, values)
