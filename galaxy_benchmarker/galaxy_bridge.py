from bioblend.galaxy import GalaxyInstance
from typing import Tuple, List
from workflow import BaseWorkflow, GalaxyWorkflow
import ansible_bridge
import planemo_bridge
import logging
import string
import random
import re

log = logging.getLogger("GalaxyBenchmarker")


class Galaxy:
    def __init__(self, url, admin_key, shed_install, ssh_user, ssh_key, galaxy_root_path, galaxy_config_dir, galaxy_user):
        self.url = url
        self.admin_key = admin_key
        self.shed_install = shed_install
        self.ssh_user = ssh_user
        self.ssh_key = ssh_key

        self.galaxy_root_path = galaxy_root_path
        self.galaxy_config_dir = galaxy_config_dir
        self.galaxy_user = galaxy_user

        self.instance = GalaxyInstance(url, key=admin_key)

    def impersonate(self, user=None, user_key=None) -> GalaxyInstance:
        """
        Returns a GalaxyInstance for the given user_key. If user is provided,
        user_key is fetched from Galaxy.
        """
        if user is not None:
            user_id = self.instance.users.get_users(f_name=user)[0]["id"]
            user_key = self.instance.users.get_user_apikey(user_id)
        return GalaxyInstance(self.url, key=user_key)

    def create_user(self, username) -> Tuple:
        """
        Creates a new user (if not already created) with username and a random password and returns
        its user_id and api_key as a tuple.
        """
        if len(self.instance.users.get_users(f_name=username)) == 0:
            password = ''.join([random.choice(string.ascii_letters + string.digits) for _ in range(32)])
            self.instance.users.create_local_user(username,
                                                  "{username}@galaxy.uni.andreas-sk.de".format(username=username),
                                                  password)

        user_id = self.instance.users.get_users(f_name=username)[0]["id"]
        user_key = self.instance.users.get_user_apikey(user_id)

        if user_key == "Not available.":
            user_key = self.instance.users.create_user_apikey(user_id)

        return user_id, user_key

    def delete_all_histories_for_user(self, user, purge=True):
        """
        Deletes and - if not set otherwise - purges for a given username all its histories.
        """
        impersonated = self.impersonate(user)
        histories = impersonated.histories.get_histories()

        for history in histories:
            impersonated.histories.delete_history(history["id"], purge)

    def install_tools_for_workflows(self, workflows: List[BaseWorkflow]):
        log.info("Installing all necessary workflow-tools on Galaxy.")
        for workflow in workflows:
            if type(workflow) is GalaxyWorkflow:
                log.info("Installing tools for workflow '{workflow}'".format(workflow=workflow.name))
                planemo_bridge.install_workflow([workflow.path], self.instance)

    def deploy_job_conf(self):
        """
        Deploys the job_conf.xml-file to the Galaxy-Server.
        """
        # Hostname parsed from the Galaxy-URL
        host = re.findall("^[a-z][a-z0-9+\-.]*://([a-z0-9\-._~%!$&'()*+,;=]+@)?([a-z0-9\-._~%]+|\[[a-z0-9\-."
                          + "_~%!$&'()*+,;=:]+\])", self.url)[0][1]
        values = {
            "galaxy_root_path": self.galaxy_root_path,
            "galaxy_config_dir": self.galaxy_config_dir,
            "galaxy_user": self.galaxy_user
        }
        ansible_bridge.run_playbook("prepare_galaxy.yml", host, self.ssh_user, self.ssh_key, values)
