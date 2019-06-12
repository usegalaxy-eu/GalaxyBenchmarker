"""
Bridge between Planemo and GalaxyBenchmarker
"""
import random
import bioblend
import time
import logging
from galaxy_bridge import Galaxy
from destination import PulsarMQDestination
from planemo import options
from planemo.cli import Context
from planemo.engine import engine_context
from planemo.galaxy.test import handle_reports_and_summary
from planemo.runnable import for_paths

log = logging.getLogger("GalaxyBenchmarker")


def run_planemo(glx: Galaxy, dest: PulsarMQDestination, workflow_path):
    """
    Runs workflow with Planemo and returns a dict of the status and history_name of the finished workflow.
    """
    return cli(Context(), [workflow_path], glx, dest.galaxy_user_key)


@options.galaxy_target_options()
@options.galaxy_config_options()
@options.test_options()
@options.engine_options()
def cli(ctx, paths, glx, user_key, **kwds):
    """
    Run specified tool's tests within Galaxy.
    Returns a dict of the status and history_name of the finished workflow.
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

    try:
        with engine_context(ctx, **kwds) as engine:
            test_data = engine.test(runnables)
            exit_code = handle_reports_and_summary(ctx, test_data.structured_data, kwds=kwds)
            status = "success" if exit_code == 0 else "error"
    except bioblend.ConnectionError as err:
        log.error("There was an error with the connection.")
        status = "error"

    return {"status": status, "history_name": kwds["history_name"]}

