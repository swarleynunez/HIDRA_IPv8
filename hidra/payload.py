from dataclasses import dataclass

from pyipv8.ipv8.messaging.payload_dataclass import overwrite_dataclass

# Enhance normal dataclasses for IPv8
dataclass = overwrite_dataclass(dataclass)


@dataclass(msg_id=1)
class NewEventPayload:
    """
    Payload for HIDRA's 'NewEvent' messages
    """

    event_id: int
    # task_type: int
    container_id: int  # = 0
    usage: int


@dataclass(msg_id=2)
class EventReplyPayload:
    """
    Payload for HIDRA's 'EventReply' messages
    """

    event_id: int
    usage: int


@dataclass(msg_id=3)
class VoteSolverPayload:
    """
    Payload for HIDRA's 'VoteSolver' messages
    """

    event_id: int
    solver: str


@dataclass(msg_id=4)
class EventSolvedPayload:
    """
    Payload for HIDRA's 'EventSolved' messages
    """

    event_id: int
