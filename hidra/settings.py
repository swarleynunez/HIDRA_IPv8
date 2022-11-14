from dataclasses import dataclass

from bami.settings import SimulationSettings


@dataclass
class HIDRASettings:
    # HIDRA peer identifier size (in number of hexadecimal characters)
    peer_id_size: int = 6

    # HIDRA objects identifier size (in number of hexadecimal characters)
    object_id_size: int = 6

    # HIDRA resource usage size (maximum digits)
    usage_size: int = 6

    # Maximum number of peers in each HIDRA event
    max_fanout = 15

    # Number of faulty peers in each HIDRA event
    if max_fanout <= SimulationSettings.peers:
        faulty_peers = max_fanout // 3
    else:
        faulty_peers = SimulationSettings.peers // 3

    # Number of containers initially executed by each peer
    initial_containers: int = 0

    # Weighting factors to select event solvers (between 0 and 1)
    usage_factor = 0.1
    reputation_factor = 1 - usage_factor

    # Execute a free-rider peer
    enable_free_rider: bool = False

    # Free-riding type: "over" or "under"
    free_riding_type: str = "over"
