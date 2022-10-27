from dataclasses import dataclass

from pyipv8.ipv8.messaging.payload_dataclass import overwrite_dataclass

# Enhance normal dataclasses for IPv8
dataclass = overwrite_dataclass(dataclass)

# Identifiers
PEER_OFFER_MESSAGE = 1
NEW_EVENT_MESSAGE = 2
EVENT_REPLY_MESSAGE = 3
VOTE_SOLVER_MESSAGE = 4
EVENT_SOLVER_MESSAGE = 5


@dataclass(msg_id=PEER_OFFER_MESSAGE)
class PeerOfferPayload:
    """
    Payload for HIDRA's 'PeerOffer' messages
    """

    peer: str
    max_usage: int


@dataclass(msg_id=NEW_EVENT_MESSAGE)
class NewEventPayload:
    """
    Payload for HIDRA's 'NewEvent' messages
    """

    event_id: int
    # task_type: int
    container_id: int
    container_image_tag: str
    # container_required_usage: int
    usage: int


@dataclass(msg_id=EVENT_REPLY_MESSAGE)
class EventReplyPayload:
    """
    Payload for HIDRA's 'EventReply' messages
    """

    event_id: int
    usage: int


@dataclass(msg_id=VOTE_SOLVER_MESSAGE)
class VoteSolverPayload:
    """
    Payload for HIDRA's 'VoteSolver' messages
    """

    event_id: int
    solver: str


@dataclass(msg_id=EVENT_SOLVER_MESSAGE)
class EventSolvedPayload:
    """
    Payload for HIDRA's 'EventSolved' messages
    """

    event_id: int
