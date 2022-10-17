import time

from pyipv8.ipv8.requestcache import NumberCache


class HIDRAEventCache(NumberCache):
    """
    Cache for HIDRA events
    """

    def __init__(self,
                 request_cache,
                 prefix: str,
                 event_id: int,
                 applicant_peer_id: str,
                 container_id: int,
                 applicant_usage: int):
        super().__init__(request_cache, prefix, event_id)

        # Attributes
        self.applicant = applicant_peer_id
        self.start_time = time.time_ns()
        self.container_id = container_id
        self.usages = {applicant_peer_id: applicant_usage}
        self.votes = {}
        self.solver = None
        self.end_time = None
