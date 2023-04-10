from dataclasses import dataclass


@dataclass
class HIDRASettings:
    # Identifier size of HIDRA peers (in number of hexadecimal characters)
    peer_id_size: int = 4

    # Identifier size of HIDRA objects (in number of hexadecimal characters)
    object_id_size: int = 8

    # Initial balance per peer
    initial_balance = 3600

    # Initial resource offer per peer
    max_resources = 10240

    # Peers send offloading events to their parent domains (intra) or to other domains (inter)
    domain_selection_policy = "inter"

    # Timeout for resource replies to select Solvers
    ssp_timeout = 3

    # Timeout for balance locking and resource reservation
    wrp_timeout = 3

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
