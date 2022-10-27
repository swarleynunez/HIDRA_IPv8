import time
from typing import Any


class HIDRAPeer:
    """
    HIDRA peer information
    """

    def __init__(self, max_usage: int):
        self.max_usage = max_usage
        # self.current_usage = current_usage


class HIDRAEvent:
    """
    HIDRA orchestration events
    """

    def __init__(self, applicant_peer_id: str, container_id: int, applicant_usage: int):
        self.applicant = applicant_peer_id
        self.start_time = time.time_ns()
        self.container_id = container_id
        self.usages = {applicant_peer_id: applicant_usage}
        self.votes = {}
        self.solver = None
        self.end_time = None


class HIDRAContainer:
    """
    HIDRA containers executed by peers
    """

    def __init__(self, image_tag: str):
        self.image_tag = image_tag
        # self.required_usage = required_usage
        self.host = None
        # Testing
        self.migrated = False


class IPv8PendingMessage:
    """
    IPv8 messages pending to be sent
    """

    def __init__(self, sender_peer_id: str, payload: Any):
        self.sender = sender_peer_id
        self.payload = payload
