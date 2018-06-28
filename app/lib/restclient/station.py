from ..models.url import URL
from apscheduler.schedulers.base import BaseScheduler
from apscheduler.triggers.interval import IntervalTrigger
import requests
from ..functions import pprint


class StationClient:
    """
    Client for the station route of the PHT service
    """

    def __init__(self, uri: URL):
        self.uri = uri
        self.register_id = "StationClient-send_station_ping_{}".format(uri)

    def register_station_ping(self, advertized_uri: URL, scheduler: BaseScheduler):
        """
        Registers the station Client for sending regular pings to the /station PHT service

        """
        def send_ping():
            payload = {'uri': advertized_uri.geturl()}
            # pprint("Sending Payload {} to {}".format(payload, self.uri.geturl()))
            requests.post(self.uri.geturl(), json=payload)

        scheduler.add_job(
            func=send_ping,
            trigger=IntervalTrigger(seconds=3),
            id=self.register_id,
            name="Send Station Ping",
            max_instances=1,
            replace_existing=True)

    def remove_station_ping(self, scheduler: BaseScheduler):
        scheduler.remove_job(job_id=self.register_id)
