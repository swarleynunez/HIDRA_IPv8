import json
from dataclasses import dataclass

from pyipv8.ipv8.messaging.payload_dataclass import overwrite_dataclass

from hidra.types import HIDRAEventInfo, HIDRAPeerInfo, HIDRAWorkload

# Enhance normal dataclasses for IPv8
dataclass = overwrite_dataclass(dataclass)

# SSP Identifiers
REQUEST_RESOURCE_INFO = 1
RESOURCE_INFO = 2

# WRP Identifiers
NEW_EVENT = 3
EVENT_REPLY = 4
EVENT_COMMIT = 5

# WEP Identifiers
NEW_RESERVATION = 6
RESERVATION_REPLY = 7
RESERVATION_COMMIT = 8
RESERVATION_COMMIT_2 = 9
RESERVATION_COMMIT_3 = 10


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


@dataclass(msg_id=NEW_EVENT)
class NewEventPayload:
    """
    Payload for HIDRA's 'NewEvent' messages
    """

    sn_e: int
    to_domain_id: int
    solver_id: str
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


@dataclass(msg_id=EVENT_REPLY)
class EventReplyPayload:
    """
    Payload for HIDRA's 'EventReply' messages
    """

    sn_e: int
    signature: bytes


@dataclass(msg_id=EVENT_COMMIT)
class EventCommitPayload:
    """
    Payload for HIDRA's 'EventCommit' messages
    """

    sn_e: int
    from_domain_id: int
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


@dataclass(msg_id=NEW_RESERVATION)
class NewReservationPayload:
    """
    Payload for HIDRA's 'NewReservation' messages
    """

    sn_e: int


@dataclass(msg_id=RESERVATION_REPLY)
class ReservationReplyPayload:
    """
    Payload for HIDRA's 'ReservationReply' messages
    """

    sn_e: int


@dataclass(msg_id=RESERVATION_COMMIT)
class ReservationCommitPayload:
    """
    Payload for HIDRA's 'ReservationCommit' messages
    """

    sn_e: int
