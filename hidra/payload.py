import json
from dataclasses import dataclass

from pyipv8.ipv8.messaging.payload_dataclass import overwrite_dataclass

from hidra.types import HIDRAEventInfo, HIDRAPeerInfo, HIDRAWorkload

# Enhance normal dataclasses for IPv8
dataclass = overwrite_dataclass(dataclass)

# SSP
REQUEST_RESOURCE_INFO = 1
RESOURCE_INFO = 2

# WRP: locking
LOCKING_SEND = 3
LOCKING_ECHO = 4
LOCKING_READY = 5
LOCKING_CREDIT = 6

# WRP: reservation
RESERVATION_ECHO = 7
RESERVATION_DENY = 8
RESERVATION_READY = 9
RESERVATION_CREDIT = 10


# WEP

@dataclass(msg_id=REQUEST_RESOURCE_INFO)
class RequestResourceInfoPayload:
    """
    Payload for HIDRA's 'RequestResourceInfo' messages
    """

    sn_e: int
    event_info: bytes

    @staticmethod
    def fix_pack_event_info(obj: HIDRAEventInfo) -> bytes:
        return json.dumps(obj, default=lambda o: o.__dict__).encode("utf-8")

    @classmethod
    def fix_unpack_event_info(cls, serialized_obj: bytes) -> HIDRAEventInfo:
        return HIDRAEventInfo(**json.loads(serialized_obj.decode("utf-8")))


@dataclass(msg_id=RESOURCE_INFO)
class ResourceInfoPayload:
    """
    Payload for HIDRA's 'ResourceInfo' messages
    """

    sn_e: int
    available: bool
    resource_replies: bytes

    @staticmethod
    def fix_pack_resource_replies(obj: [HIDRAPeerInfo]) -> bytes:
        return json.dumps(obj, default=lambda o: o.__dict__).encode("utf-8")

    @classmethod
    def fix_unpack_resource_replies(cls, serialized_obj: bytes) -> [HIDRAPeerInfo]:
        d = json.loads(serialized_obj.decode("utf-8"))
        for k, v in d.items():
            d[k] = HIDRAPeerInfo(**v)
        return d


@dataclass(msg_id=LOCKING_SEND)
class LockingSendPayload:
    """
    Payload for HIDRA's 'LockingSend' messages
    """

    sn_e: int
    to_domain_id: int
    to_solver_id: str
    event_info: bytes

    @staticmethod
    def fix_pack_event_info(obj: HIDRAEventInfo) -> bytes:
        return json.dumps(obj, default=lambda o: o.__dict__).encode("utf-8")

    @classmethod
    def fix_unpack_event_info(cls, serialized_obj: bytes) -> HIDRAEventInfo:
        d = json.loads(serialized_obj.decode("utf-8"))
        event_info = HIDRAEventInfo(**d)
        event_info.workload = HIDRAWorkload(**d["workload"])
        return event_info


@dataclass(msg_id=LOCKING_ECHO)
class LockingEchoPayload:
    """
    Payload for HIDRA's 'LockingEcho' messages
    """

    applicant_id: int
    sn_e: int
    event_info: bytes

    @staticmethod
    def fix_pack_event_info(obj: HIDRAEventInfo) -> bytes:
        return json.dumps(obj, default=lambda o: o.__dict__).encode("utf-8")

    @classmethod
    def fix_unpack_event_info(cls, serialized_obj: bytes) -> HIDRAEventInfo:
        d = json.loads(serialized_obj.decode("utf-8"))
        event_info = HIDRAEventInfo(**d)
        event_info.workload = HIDRAWorkload(**d["workload"])
        return event_info


@dataclass(msg_id=LOCKING_READY)
class LockingReadyPayload:
    """
    Payload for HIDRA's 'LockingReady' messages
    """

    sn_e: int
    event_info: bytes

    @staticmethod
    def fix_pack_event_info(obj: HIDRAEventInfo) -> bytes:
        return json.dumps(obj, default=lambda o: o.__dict__).encode("utf-8")

    @classmethod
    def fix_unpack_event_info(cls, serialized_obj: bytes) -> HIDRAEventInfo:
        d = json.loads(serialized_obj.decode("utf-8"))
        event_info = HIDRAEventInfo(**d)
        event_info.workload = HIDRAWorkload(**d["workload"])
        return event_info


@dataclass(msg_id=LOCKING_CREDIT)
class LockingCreditPayload:
    """
    Payload for HIDRA's 'LockingCredit' messages
    """

    sn_e: int
    event_info: bytes

    @staticmethod
    def fix_pack_event_info(obj: HIDRAEventInfo) -> bytes:
        return json.dumps(obj, default=lambda o: o.__dict__).encode("utf-8")

    @classmethod
    def fix_unpack_event_info(cls, serialized_obj: bytes) -> HIDRAEventInfo:
        d = json.loads(serialized_obj.decode("utf-8"))
        event_info = HIDRAEventInfo(**d)
        event_info.workload = HIDRAWorkload(**d["workload"])
        return event_info


@dataclass(msg_id=RESERVATION_ECHO)
class NewReservationPayload:
    """
    Payload for HIDRA's 'NewReservation' messages
    """

    applicant_id: str
    sn_e: int
    sn_r: int
    event_info: bytes
    locking_qc: bytes

    @staticmethod
    def fix_pack_event_info(obj: HIDRAEventInfo) -> bytes:
        return json.dumps(obj, default=lambda o: o.__dict__).encode("utf-8")

    @classmethod
    def fix_unpack_event_info(cls, serialized_obj: bytes) -> HIDRAEventInfo:
        d = json.loads(serialized_obj.decode("utf-8"))
        event_info = HIDRAEventInfo(**d)
        event_info.workload = HIDRAWorkload(**d["workload"])
        return event_info

    @staticmethod
    def fix_pack_locking_qc(dictionary: dict) -> bytes:
        return json.dumps(dictionary).encode("utf-8")

    @classmethod
    def fix_unpack_locking_qc(cls, serialized_dictionary: bytes) -> dict:
        return json.loads(serialized_dictionary.decode("utf-8"))


@dataclass(msg_id=RESERVATION_READY)
class ReservationReplyPayload:
    """
    Payload for HIDRA's 'ReservationReply' messages
    """

    applicant_id: str
    sn_e: int
    signature: bytes


@dataclass(msg_id=RESERVATION_CREDIT)
class ReservationCommitPayload:
    """
    Payload for HIDRA's 'ReservationCommit' messages
    """

    sn_e: int
    sn_r: int
    reservation_qc: bytes

    @staticmethod
    def fix_pack_reservation_qc(dictionary: dict) -> bytes:
        return json.dumps(dictionary).encode("utf-8")

    @classmethod
    def fix_unpack_reservation_qc(cls, serialized_dictionary: bytes) -> dict:
        return json.loads(serialized_dictionary.decode("utf-8"))
