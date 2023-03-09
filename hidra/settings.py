from dataclasses import dataclass

from bami.settings import SimulationSettings


@dataclass
class HIDRASettings:
    # Identifier size of HIDRA peers (in number of hexadecimal characters)
    peer_id_size: int = 4

    # Identifier size of HIDRA objects (in number of hexadecimal characters)
    object_id_size: int = 4

    # Initial balance per peer
    initial_balance = 100

    # Initial resource offer per peer
    initial_resource_offer = 10240

    # TODO
    # Number of containers initially executed by each peer
    initial_containers: int = 0

    # Weighting factors to select event solvers (between 0 and 1)
    usage_factor = 0.1
    reputation_factor = 1 - usage_factor

    # Execute a free-rider peer
    enable_free_rider: bool = False

    # Free-riding type: "over" or "under"
    free_riding_type: str = "over"
