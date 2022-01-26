"""
Bridge between Planemo and GalaxyBenchmarker
"""
from __future__ import annotations
import random
import time
import logging
# from destination import PulsarMQDestination
from planemo import options
from planemo.cli import PlanemoContext
from planemo.engine import engine_context
from planemo.galaxy.test import handle_reports_and_summary
from planemo.runnable import for_paths
from planemo.galaxy.workflows import install_shed_repos
from typing import Dict, TYPE_CHECKING

if TYPE_CHECKING:
    from galaxy_benchmarker.bridge.galaxy import Galaxy

log = logging.getLogger(__name__)


def run_planemo(glx: Galaxy, dest: PulsarMQDestination, workflow_path) -> Dict:
    """
    Runs workflow with Planemo and returns a dict of the status and history_name of the finished workflow.
    """
    return _cli(PlanemoContext(), [workflow_path], glx, dest.galaxy_user_key)


def install_workflow(workflow_path, glx_instance):
    """
    Installs the tools necessary to run a given workflow (given as a path to the workflow).
    """
    runnable = for_paths(workflow_path)[0]
    install_shed_repos(runnable, glx_instance, False)


@options.galaxy_target_options()
@options.galaxy_config_options()
@options.test_options()
@options.engine_options()
def _cli(ctx, paths, glx, user_key, **kwds) -> Dict:
    """
    Run specified tool's tests within Galaxy.
    Returns a dict of the status and history_name of the finished workflow.
    See https://github.com/galaxyproject/planemo/blob/master/planemo/commands/cmd_test.py
    """
    kwds["engine"] = "external_galaxy"
    kwds["shed_install"] = False
    kwds["galaxy_url"] = glx.url
    kwds["galaxy_admin_key"] = glx.user_key
    kwds["history_name"] = "galaxy_benchmarker-" + str(time.time_ns()) + str(random.randrange(0, 99999))

    if user_key is not None:
        kwds["galaxy_user_key"] = user_key

    runnables = for_paths(paths)

    try:
        with engine_context(ctx, **kwds) as engine:
            test_data = engine.test(runnables)
            exit_code = handle_reports_and_summary(ctx, test_data.structured_data, kwds=kwds)
            status = "success" if exit_code == 0 else "error"
    except Exception as e:
        log.error("There was an error: {e}".format(e=e))
        status = "error"

    return {"status": status, "history_name": kwds["history_name"]}
