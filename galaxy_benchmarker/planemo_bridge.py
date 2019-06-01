"""
Bridge between Planemo and GalaxyBenchmarker
"""
import logging
from glx import Galaxy
from planemo import options
from planemo.cli import Context
from planemo.engine import engine_context
from planemo.galaxy.test import handle_reports_and_summary
from planemo.runnable import for_paths

log = logging.getLogger(__name__)
log.setLevel(logging.ERROR)


def run_planemo(glx: Galaxy, workflow_path, user_key=None):
    return cli(Context(), [workflow_path], glx, user_key)


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
    kwds["history_name"] = "bridge-history"  # TODO

    if user_key is not None:
        kwds["galaxy_user_key"] = user_key

    runnables = for_paths(paths)
    print(kwds)

    with engine_context(ctx, **kwds) as engine:
        test_data = engine.test(runnables)
        return_value = handle_reports_and_summary(ctx, test_data.structured_data, kwds=kwds)
