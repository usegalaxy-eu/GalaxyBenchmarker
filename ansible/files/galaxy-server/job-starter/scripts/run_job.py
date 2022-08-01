import os
import json
import logging
import shlex
from bioblend.galaxy import GalaxyInstance

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

GLX_HISTORY_NAME = os.getenv("GLX_HISTORY_NAME", "galaxy_benchmarker")
GLX_TOOL_ID = os.getenv("GLX_TOOL_ID", "")
GLX_TOOL_INPUT = os.getenv("GLX_TOOL_INPUT", "")

def main():
    """Connect to the local galaxy and trigger a job"""
    if not GLX_TOOL_ID:
        raise ValueError("Env var 'GLX_TOOL_ID' is required. Expected: Tool id which should be executed")
    if not GLX_TOOL_INPUT:
        raise ValueError("Env var 'GLX_TOOL_INPUT' is required. Expected: String encoded json input")
    tool_input = json.loads(shlex.split(GLX_TOOL_INPUT)[0])

    glx =  GalaxyInstance(url="http://galaxy", key="fakekey")

    history_id = get_history_id(glx, GLX_HISTORY_NAME)

    log.info("Initiate tool run")
    result = glx.tools.run_tool(
        tool_id=GLX_TOOL_ID,
        tool_inputs=tool_input,
        history_id=history_id
    )
    log.info(result)

def get_history_id(glx: GalaxyInstance, history_name: str) -> str:
    """Get history id by history name"""
    histories = glx.histories.get_histories()
    for history in histories:
        if history["name"] == history_name:
            return history["id"]

    result = glx.histories.create_history(history_name)
    return result["id"]


if __name__ == '__main__':
    main()