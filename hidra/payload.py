import json
from dataclasses import dataclass

from pyipv8.ipv8.messaging.payload_dataclass import overwrite_dataclass

from hidra.types import HIDRAEventInfo, HIDRAPeerInfo

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
RESERVATION_COMMIT_1 = 8
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
        return json.loads(serialized_obj.decode("utf-8"))


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
        return json.loads(serialized_obj.decode("utf-8"))


@dataclass(msg_id=NEW_EVENT)
class NewEventPayload:
    """
    Payload for HIDRA's 'NewEvent' messages
    """

    sn_e: int
    solver_id: str
    event_info: bytes

    @staticmethod
    def fix_pack_event_info(obj: HIDRAEventInfo) -> bytes:
        return json.dumps(obj, default=lambda o: o.__dict__).encode("utf-8")

    @classmethod
    def fix_unpack_event_info(cls, serialized_obj: bytes) -> HIDRAEventInfo:
        return json.loads(serialized_obj.decode("utf-8"))


@dataclass(msg_id=EVENT_REPLY)
class EventReplyPayload:
    """
    Payload for HIDRA's 'EventReply' messages
    """

    sn_e: int


@dataclass(msg_id=EVENT_COMMIT)
class EventCommitPayload:
    """
    Payload for HIDRA's 'EventCommit' messages
    """

    sn_e: int
