from enum import Enum
from typing import Any

from pyipv8.ipv8.peer import Peer

# Enumerations (SECONDS, MINUTES, HOURS, WEEKS)
TimeUnit = Enum('TimeUnit', ['S', 'M', 'H', 'W'])


class HIDRAPeerInfo:
    """
    HIDRA peer information
    """

    def __init__(self, sn_e: int, balance: int, r_max: int, r_free: int):
        self.sn_e = sn_e
        self.balance = balance
        self.r_max = r_max
        self.r_free = r_free


class HIDRAPeer:
    """
    HIDRA peer
    """

    def __init__(self):
        self.peer_info: HIDRAPeerInfo
        self.resource_replies = {}
        self.balance_locks = {}


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
    HIDRA offloading event information
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
    HIDRA offloading event
    """

    def __init__(self, event_info: HIDRAEventInfo, domain_index: int):
        self.event_info = event_info
        self.domain_index = domain_index
        self.available_peers = []
        self.solver_id = None


class IPv8PendingMessage:
    """
    IPv8 messages pending to be delivered
    """

    def __init__(self, sender: Peer, payload: Any):
        self.sender = sender
        self.payload = payload
