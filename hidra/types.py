from enum import Enum
from typing import Any

from pyipv8.ipv8.peer import Peer

# Enumerations (SECONDS, MINUTES, HOURS, WEEKS)
TimeUnit = Enum('TimeUnit', ['S', 'M', 'H', 'W'])


class HIDRAPeerInfo:
    """
    HIDRA peer (shared information)
    """

    def __init__(self, balance: int, r_max: int, r_free: int, sn_e: int, sn_r: int):
        self.balance = balance
        self.r_max = r_max
        self.r_free = r_free
        self.sn_e = sn_e
        self.sn_r = sn_r

    def __str__(self):
        return str(self.balance) + ":" + \
            str(self.r_max) + ":" + \
            str(self.r_free) + ":" + \
            str(self.sn_e) + ":" + \
            str(self.sn_r)


class HIDRAPeer:
    """
    HIDRA peer (local information)
    """

    def __init__(self):
        self.peer_info: HIDRAPeerInfo = None
        self.resource_replies = {}
        self.deposits = {}
        self.reservations = {}


class HIDRAWorkload:
    """
    HIDRA workloads executed by Solver peers
    """

    def __init__(self, image: str, resource_limit: int, port: int):
        self.image = image
        self.resource_limit = resource_limit
        self.port = port

    def __str__(self):
        return self.image + ":" + \
            str(self.resource_limit) + ":" + \
            str(self.port)


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

    def __str__(self):
        return str(self.workload) + ":" + \
            str(self.t_exec_value) + ":" + \
            str(self.p_ratio_value) + ":" + \
            str(self.ts_start)


class HIDRAEvent:
    """
    HIDRA offloading event (local information)
    """

    def __init__(self, event_info: HIDRAEventInfo, to_domain_id: int):
        # Global info
        self.event_info = event_info
        self.to_domain_id = to_domain_id
        self.to_solver_id = None
        self.locking_qc = {}  # Quorum certificate
        self.reservation_qc = {}  # Quorum certificate

        # Local info
        self.available_peers = []
        self.sn_r = 0
        self.sent_echo = False
        self.reservation_qc_echos = {}
        self.reservation_qc_readys = {}
