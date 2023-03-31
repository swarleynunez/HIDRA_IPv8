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
        self.info: HIDRAPeerInfo = None
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

    def __init__(self, to_domain_id: int, to_solver_id: str, workload: HIDRAWorkload,
                 t_exec_value: int, p_ratio_value: int, ts_start: int):
        self.to_domain_id = to_domain_id
        self.to_solver_id = to_solver_id
        self.workload = workload
        self.t_exec_value = t_exec_value
        # self.t_exec_unit = TimeUnit.S.value
        self.p_ratio_value = p_ratio_value
        # self.p_ratio_unit = TimeUnit.S.value
        self.ts_start = ts_start

    def __str__(self):
        return str(self.to_domain_id) + ":" + \
            self.to_solver_id + ":" + \
            str(self.workload) + ":" + \
            str(self.t_exec_value) + ":" + \
            str(self.p_ratio_value) + ":" + \
            str(self.ts_start)


class HIDRAEvent:
    """
    HIDRA offloading event (local information)
    """

    def __init__(self):
        # Shared info
        self.info: HIDRAEventInfo = None

        # Local info
        self.available_peers = []
        self.locking_echo_sent = False
        self.locking_echos = {}
        self.locking_readys = {}
        self.locking_credits = {}

        self.sn_r = 0
        self.reservation_echos = {}
        self.reservation_readys = {}


class IPv8PendingMessage:
    """
    IPv8 messages pending to be delivered
    """

    def __init__(self, sender: Peer, payload: Any):
        self.sender = sender
        self.payload = payload
