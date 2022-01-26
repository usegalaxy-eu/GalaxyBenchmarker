#     def run(self):
#         if self.name == "delete_old_histories":
#             for destination in self.destinations:
#                 self._delete_old_histories(destination)
#         elif self.name == "reboot_openstack_servers":
#             self._reboot_openstack_servers()
#         elif self.name == "reboot_random_openstack_server":
#             self._reboot_random_openstack_server()
#         elif self.name == "rebuild_random_openstack_server":
#             self._rebuild_random_openstack_server()
#         else:
#             raise ValueError("{name} is not a valid BenchmarkerTask!".format(name=self.name))

#     def _delete_old_histories(self, destination):
#         destination.galaxy.delete_all_histories_for_user(destination.galaxy_user_name, True)

#     def _reboot_openstack_servers(self):
#         if "name_contains" not in self.params:
#             raise ValueError("'name_contains' is needed for rebooting openstack servers")
#         reboot_type = self.params["reboot_type"] if "reboot_type" in self.params else "soft"

#         servers = self.openstack.get_servers(self.params["name_contains"])
#         self.openstack.reboot_servers(servers, reboot_type == "hard")

#     def _reboot_random_openstack_server(self):
#         if "name_contains" not in self.params:
#             raise ValueError("'name_contains' is needed for rebooting openstack servers")
#         reboot_type = self.params["reboot_type"] if "reboot_type" in self.params else "soft"

#         servers = self.openstack.get_servers(self.params["name_contains"])

#         rand_index = randrange(0, len(servers))
#         self.openstack.reboot_servers([servers[rand_index]], reboot_type == "hard")

#     def _rebuild_random_openstack_server(self):
#         if "name_contains" not in self.params:
#             raise ValueError("'name_contains' is needed for rebuilding openstack servers")

#         servers = self.openstack.get_servers(self.params["name_contains"])

#         rand_index = randrange(0, len(servers))
#         self.openstack.rebuild_servers([servers[rand_index]])
