import json
from dataclasses import dataclass

from pyipv8.ipv8.messaging.payload_dataclass import overwrite_dataclass

from hidra.types import HIDRAEvent

# Enhance normal dataclasses for IPv8
dataclass = overwrite_dataclass(dataclass)

# Identifiers
PEER_INIT_MESSAGE = 1
NEW_EVENT_MESSAGE = 2
EVENT_REPLY_MESSAGE = 3
EVENT_COMMIT_MESSAGE = 4
EVENT_CREDIT_MESSAGE = 5
EVENT_DISCOVERY_MESSAGE = 6


@dataclass(msg_id=PEER_INIT_MESSAGE)
class PeerInitPayload:
    """
    Payload for HIDRA's 'PeerInit' messages
    """

    public_key: bytes
    max_usage: int


@dataclass(msg_id=NEW_EVENT_MESSAGE)
class NewEventPayload:
    """
    Payload for HIDRA's 'NewEvent' messages
    """

    event_id: int
    container_id: int
    container_image_tag: str


@dataclass(msg_id=EVENT_REPLY_MESSAGE)
class EventReplyPayload:
    """
    Payload for HIDRA's 'EventReply' messages
    """

    event_id: int
    usage_offer: int
    reputation_offer: int
    signature: bytes


@dataclass(msg_id=EVENT_COMMIT_MESSAGE)
class EventCommitPayload:
    """
    Payload for HIDRA's 'EventCommit' messages
    """

    event_id: int
    usage_offers: bytes
    reputation_offers: bytes
    ack_signatures: bytes

    @staticmethod
    def fix_pack_usage_offers(dictionary: dict) -> bytes:
        return json.dumps(dictionary).encode("utf-8")

    @classmethod
    def fix_unpack_usage_offers(cls, serialized_dictionary: bytes) -> dict:
        return json.loads(serialized_dictionary.decode("utf-8"))

    @staticmethod
    def fix_pack_reputation_offers(dictionary: dict) -> bytes:
        return json.dumps(dictionary).encode("utf-8")

    @classmethod
    def fix_unpack_reputation_offers(cls, serialized_dictionary: bytes) -> dict:
        return json.loads(serialized_dictionary.decode("utf-8"))

    @staticmethod
    def fix_pack_ack_signatures(dictionary: dict) -> bytes:
        return json.dumps(dictionary).encode("utf-8")

    @classmethod
    def fix_unpack_ack_signatures(cls, serialized_dictionary: bytes) -> dict:
        return json.loads(serialized_dictionary.decode("utf-8"))


@dataclass(msg_id=EVENT_CREDIT_MESSAGE)
class EventCreditPayload:
    """
    Payload for HIDRA's 'EventCredit' messages
    """

    event_id: int
    solver: str
    execution_result: int
    signature: bytes


# Optional. To send/retrieve event data
@dataclass(msg_id=EVENT_DISCOVERY_MESSAGE)
class EventDiscoveryPayload:
    """
    Payload for HIDRA's 'EventDiscovery' messages
    """

    event_id: int
    event: bytes

    @staticmethod
    def fix_pack_event(event: HIDRAEvent) -> bytes:
        return json.dumps(event, default=lambda o: o.__dict__).encode("utf-8")

    @classmethod
    def fix_unpack_event(cls, serialized_event: bytes) -> dict:
        return json.loads(serialized_event.decode("utf-8"))
