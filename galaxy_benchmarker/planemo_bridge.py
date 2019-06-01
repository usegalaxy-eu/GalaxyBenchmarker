"""
Bridge between Planemo and GalaxyBenchmarker
"""
import logging
import random
import time
from glx import Galaxy
from destination import PulsarMQDestination
from planemo import options
from bioblend.galaxy import GalaxyInstance
from planemo.cli import Context
from planemo.engine import engine_context
from planemo.runnable import for_paths

log = logging.getLogger(__name__)
log.setLevel(logging.ERROR)


def run_planemo(glx: Galaxy, dest: PulsarMQDestination, workflow_path):
    """
    Runs workflow with Planemo and returns the job_ids of the finished workflow.
    """
    return cli(Context(), [workflow_path], glx, dest.galaxy_user_key)


@options.galaxy_target_options()
@options.galaxy_config_options()
@options.test_options()
@options.engine_options()
def cli(ctx, paths, glx, user_key, **kwds):
    """
    Run specified tool's tests within Galaxy.
    See https://github.com/galaxyproject/planemo/blob/master/planemo/commands/cmd_test.py
    """
    kwds["engine"] = "external_galaxy"
    kwds["shed_install"] = True
    kwds["galaxy_url"] = glx.url
    kwds["galaxy_admin_key"] = glx.admin_key
    kwds["history_name"] = "galaxy_benchmarker-" + str(time.time_ns()) + str(random.randrange(0, 99999))

    if user_key is not None:
        kwds["galaxy_user_key"] = user_key

    runnables = for_paths(paths)

    with engine_context(ctx, **kwds) as engine:
        engine.test(runnables)

    job_ids = get_job_ids_from_history_name(kwds["history_name"], glx.impersonate(user_key=user_key))
    print(job_ids)
    return job_ids


def get_job_ids_from_history_name(history_name, glx_instance: GalaxyInstance):
    history_id = glx_instance.histories.get_histories(name=history_name)[0]["id"]
    dataset_ids = glx_instance.histories.show_history(history_id)["state_ids"]["ok"]
    job_ids = list()
    for dataset_id in dataset_ids:
        job_ids.append(glx_instance.histories.show_dataset(history_id, dataset_id)["creating_job"])

    return job_ids
