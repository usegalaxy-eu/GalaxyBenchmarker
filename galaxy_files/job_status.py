import logging
from sqlalchemy import create_engine
from sqlalchemy.sql import text

from . import InstrumentPlugin
from .. import formatting


log = logging.getLogger(__name__)


class StatusFormatter(formatting.JobMetricFormatter):

    def format(self, key, value):
        return key, value


class JobStatusPlugin(InstrumentPlugin):
    """ Gather status
    """
    plugin_type = "jobstatus"
    formatter = StatusFormatter()

    def __init__(self, **kwargs):
        pass

    def job_properties(self, job_id, job_directory):
        return self._get_job_state_history(job_id)

    def _get_job_state_history(self, job_id):
        engine = create_engine('postgresql:///galaxy?host=/var/run/postgresql')  # TODO: Get it from Galaxy.yml
        with engine.connect() as con:
            query = text("SELECT update_time, state FROM job_state_history WHERE job_id = :job_id order by create_time")
            query_res = con.execute(query, {"job_id": job_id})

        job_state_history = dict()
        for res in query_res:
            job_state_history[res[1]] = res[0]

        return job_state_history


__all__ = ('JobStatusPlugin', )