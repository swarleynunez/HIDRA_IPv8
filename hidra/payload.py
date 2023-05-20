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
RESERVATION_CANCEL = 8
RESERVATION_READY = 9
RESERVATION_CREDIT = 10

# WRP: confirmation
EVENT_CONFIRM = 11
EVENT_CANCEL = 12

# WEP
MONITORING_REQUEST = 13
MONITORING_RESPONSE = 14
MONITORING_RESULT = 15
MONITORING_SEND = 16
MONITORING_ECHO = 17
MONITORING_CREDIT = 18


@dataclass(msg_id=REQUEST_RESOURCE_INFO)
class RequestResourceInfoPayload:
    """
    Payload for HIDRA's 'RequestResourceInfo' messages
    """

    applicant_id: str
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


@dataclass(msg_id=RESOURCE_INFO)
class ResourceInfoPayload:
    """
    Payload for HIDRA's 'ResourceInfo' messages
    """

    applicant_id: str
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

    applicant_id: str
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


@dataclass(msg_id=LOCKING_ECHO)
class LockingEchoPayload:
    """
    Payload for HIDRA's 'LockingEcho' messages
    """

    applicant_id: str
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

    applicant_id: str
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

    applicant_id: str
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
class ReservationEchoPayload:
    """
    Payload for HIDRA's 'ReservationEcho' messages
    """

    applicant_id: str
    sn_e: int
    sn_r: int
    vote: bool
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


@dataclass(msg_id=RESERVATION_READY)
class ReservationReadyPayload:
    """
    Payload for HIDRA's 'ReservationReady' messages
    """

    applicant_id: str
    sn_e: int
    sn_r: int
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


@dataclass(msg_id=RESERVATION_CANCEL)
class ReservationCancelPayload:
    """
    Payload for HIDRA's 'ReservationCancel' messages
    """

    applicant_id: str
    sn_e: int


@dataclass(msg_id=RESERVATION_CREDIT)
class ReservationCreditPayload:
    """
    Payload for HIDRA's 'ReservationCredit' messages
    """

    applicant_id: str
    sn_e: int
    sn_r: int
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


@dataclass(msg_id=EVENT_CONFIRM)
class EventConfirmPayload:
    """
    Payload for HIDRA's 'EventConfirm' messages
    """

    applicant_id: str
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


@dataclass(msg_id=EVENT_CANCEL)
class EventCancelPayload:
    """
    Payload for HIDRA's 'EventCancel' messages
    """

    applicant_id: str
    sn_e: int
    solver_id: str
    sn_r: int


@dataclass(msg_id=MONITORING_REQUEST)
class MonitoringRequestPayload:
    """
    Payload for HIDRA's 'MonitoringRequest' messages
    """

    applicant_id: str
    sn_e: int


@dataclass(msg_id=MONITORING_RESPONSE)
class MonitoringResponsePayload:
    """
    Payload for HIDRA's 'MonitoringResponse' messages
    """

    applicant_id: str
    sn_e: int
    response: bool


@dataclass(msg_id=MONITORING_RESULT)
class MonitoringResultPayload:
    """
    Payload for HIDRA's 'MonitoringResult' messages
    """

    applicant_id: str
    sn_e: int
    ts_end: str


@dataclass(msg_id=MONITORING_SEND)
class MonitoringSendPayload:
    """
    Payload for HIDRA's 'MonitoringSend' messages
    """

    applicant_id: str
    sn_e: int


@dataclass(msg_id=MONITORING_ECHO)
class MonitoringEchoPayload:
    """
    Payload for HIDRA's 'MonitoringEcho' messages
    """

    applicant_id: str
    sn_e: int


@dataclass(msg_id=MONITORING_CREDIT)
class MonitoringCreditPayload:
    """
    Payload for HIDRA's 'MonitoringCredit' messages
    """

    applicant_id: str
    sn_e: int
