from ..models.url import URL
from apscheduler.schedulers.base import BaseScheduler
from apscheduler.triggers.interval import IntervalTrigger
import requests

# # Register all jobs
# for (key, value) in jobs.items():
#
#     value['id'] = key
#     scheduler.add_job(**value)
#
# scheduler.add_job(
#     func=register_request,
#     trigger=IntervalTrigger(seconds=3),
#     replace_existing=True,
#     id="register_request",
#     name="Registers station at station office")
#
#
#


class StationClient:
    """
    Client for the station route of the PHT service
    """

    def __init__(self, uri: URL):
        self.uri = uri
        self.register_id = "StationClient-send_station_ping_{}".format(uri)
        self.uri_payload = {'uri': self.uri}

    def register_station_ping(self, uri: URL, scheduler: BaseScheduler):
        """
        Registers the station Client for sending regular pings to the /station PHT service

        """
        scheduler.add_job(
            func=lambda: requests.post(self.uri, data=self.uri_payload),
            trigger=IntervalTrigger(seconds=3),
            id=self.register_id,
            name="Send Station Ping",
            max_instances=1,
            replace_existing=True)

    def remove_station_ping(self, scheduler: BaseScheduler):
        scheduler.remove_job(job_id=self.register_id)
