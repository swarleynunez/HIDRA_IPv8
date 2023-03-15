from enum import Enum
from typing import Any

from pyipv8.ipv8.peer import Peer

# Enumerations (SECONDS, MINUTES, HOURS, WEEKS)
TimeUnit = Enum('TimeUnit', ['S', 'M', 'H', 'W'])


class HIDRAPeerInfo:
    """
    HIDRA peer (shared information)
    """

    def __init__(self, sn_e: int, balance: int, r_max: int, r_free: int):
        self.sn_e = sn_e
        self.balance = balance
        self.r_max = r_max
        self.r_free = r_free


class HIDRAPeer:
    """
    HIDRA peer (local information)
    """

    def __init__(self):
        self.peer_info: HIDRAPeerInfo = None
        self.resource_replies = {}
        self.deposits = {}


class HIDRAWorkload:
    """
    HIDRA workloads executed by Solver peers
    """

    def __init__(self, image: str, resource_limit: int, port: int):
        self.image = image
        self.resource_limit = resource_limit
        self.port = port


class HIDRAEventInfo:
    """
    HIDRA offloading event (shared information)
    """

    def __init__(self, workload: HIDRAWorkload, t_exec_value: int, p_ratio_value: int, ts_start: int):
        self.workload = workload
        self.t_exec_value = t_exec_value
        # self.t_exec_unit = TimeUnit.S.value
        self.p_ratio_value = p_ratio_value
        # self.p_ratio_unit = TimeUnit.S.value
        self.ts_start = ts_start


class HIDRAEvent:
    """
    HIDRA offloading event (local information)
    """

    def __init__(self, event_info: HIDRAEventInfo, domain_id: int):
        self.event_info = event_info
        self.domain_id = domain_id
        self.solver_id = None
        self.available_peers = []


class IPv8PendingMessage:
    """
    IPv8 messages pending to be delivered
    """

    def __init__(self, sender: Peer, payload: Any):
        self.sender = sender
        self.payload = payload
