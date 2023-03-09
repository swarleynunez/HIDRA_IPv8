import time
from typing import Any

from pyipv8.ipv8.keyvault.keys import Key


class HIDRAPeer:
    """
    HIDRA peer information
    """

    def __init__(self, balance: int, resource_offer: int):
        self.balance = balance
        self.resource_offer = resource_offer
        self.free_resources = resource_offer


class HIDRAEvent:
    """
    HIDRA offloading event
    """

    def __init__(self, applicant_peer_id: str, container_id: int):
        self.applicant = applicant_peer_id
        self.start_time = time.time_ns()
        self.container_id = container_id
        self.usage_offers = {}
        self.reputation_offers = {}
        self.ack_signatures = {}
        self.votes = {}
        self.credit_signatures = {}
        self.execution_results = {}
        self.end_time = None


class HIDRAContainer:
    """
    HIDRA containers executed by peers
    """

    def __init__(self, image_tag: str):
        self.image_tag = image_tag
        self.host = None


class IPv8PendingMessage:
    """
    IPv8 messages pending to be delivered
    """

    def __init__(self, sender_peer_id: str, payload: Any):
        self.sender = sender_peer_id
        self.payload = payload
