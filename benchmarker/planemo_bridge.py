"""
Bridge between Planemo and GalaxyBenchmarker
"""

from planemo import options
from planemo.cli import Context
from planemo.engine import engine_context
from planemo.galaxy.test import handle_reports_and_summary
from planemo.runnable import for_paths


def run(paths):
    ctx = Context()
    return cli(ctx, paths)


@options.galaxy_target_options()
@options.galaxy_config_options()
@options.test_options()
@options.engine_options()
def cli(ctx, paths, **kwds):
    """
    Run specified tool's tests within Galaxy.
    See https://github.com/galaxyproject/planemo/blob/master/planemo/commands/cmd_test.py
    """
    runnables = for_paths(paths)

    kwds["engine"] = "external_galaxy"
    kwds["shed_install"] = True
    kwds["galaxy_url"] = "http://galaxy.uni.andreas-sk.de"
    kwds["galaxy_admin_key"] = "a4d46fe353a30230b9ade20417b08e54"
    kwds["galaxy_user_key"] = "b459d705181051406c0d95e41dd05783"
    kwds["history_name"] = "bridge-history"

    with engine_context(ctx, **kwds) as engine:
        test_data = engine.test(runnables)
        return_value = handle_reports_and_summary(ctx, test_data.structured_data, kwds=kwds)

    ctx.exit(return_value)
