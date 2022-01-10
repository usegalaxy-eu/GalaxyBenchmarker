from bioblend.galaxy import GalaxyInstance
from typing import Tuple, List
from galaxy_benchmarker.workflow import BaseWorkflow, GalaxyWorkflow
from galaxy_benchmarker.bridge import ansible, planemo
import logging
import string
import random
import re

log = logging.getLogger("GalaxyBenchmarker")


class Galaxy:
    def __init__(self, url, user_key, shed_install=False,
                 ssh_user=None, ssh_key=None, galaxy_root_path=None,
                 galaxy_config_dir=None, galaxy_user=None):
        self.url = url
        self.user_key = user_key
        self.shed_install = shed_install
        self.ssh_user = ssh_user
        self.ssh_key = ssh_key

        self.galaxy_root_path = galaxy_root_path
        self.galaxy_config_dir = galaxy_config_dir
        self.galaxy_user = galaxy_user

        self.instance = GalaxyInstance(url, key=user_key)

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
                planemo.install_workflow([workflow.path], self.instance)

    def deploy_job_conf(self):
        """
        Deploys the job_conf.xml-file to the Galaxy-Server.
        """
        # Hostname parsed from the Galaxy-URL
        host = re.findall("^[a-z][a-z0-9+\-.]*://([a-z0-9\-._~%!$&'()*+,;=]+@)?([a-z0-9\-._~%]+|\[[a-z0-9\-."
                          + "_~%!$&'()*+,;=:]+\])", self.url)[0][1]

        if None in (self.ssh_user, self.ssh_key, self.galaxy_root_path, self.galaxy_config_dir, self.galaxy_user):
            raise ValueError("ssh_user, ssh_key, galaxy_root_path, galaxy_config_dir, and galaxy_user need "
                             "to be set in order to deploy the job_conf.xml-file!")

        values = {
            "galaxy_root_path": self.galaxy_root_path,
            "galaxy_config_dir": self.galaxy_config_dir,
            "galaxy_user": self.galaxy_user
        }
        ansible.run_playbook("prepare_galaxy.yml", host, self.ssh_user, self.ssh_key, values)


    def get_job_ids_from_history_name(self, history_name: str, user_key: str) -> List:
        """
        For a given history_name return all its associated job-ids. As a history is only accessible from the user it
        was created by, an impersonated_instance is needed.
        """
        impersonated = self.impersonate(user_key=user_key)
        histories = impersonated.histories.get_histories(name=history_name)

        if len(histories) >= 1:
            history_id = histories[0]["id"]
            dataset_ids = impersonated.histories.show_history(history_id)["state_ids"]["ok"]
            job_ids = list()

            for dataset_id in dataset_ids:
                job_ids.append(impersonated.histories.show_dataset(history_id, dataset_id)["creating_job"])

            return job_ids

        return []
