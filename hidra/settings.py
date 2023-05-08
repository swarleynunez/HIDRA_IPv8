from dataclasses import dataclass


@dataclass
class HIDRASettings:
    # Identifier size of HIDRA peers (in number of hexadecimal characters)
    peer_id_size: int = 4

    # Identifier size of HIDRA objects (in number of hexadecimal characters)
    object_id_size: int = 8

    # Initial balance units per peer
    initial_balance = 1000000

    # Initial resource units offer per peer
    max_resources = 1000000

    # Peers send offloading events to their parent domains (intra) or to other domains (inter)
    domain_selection_policy = "intra"

    # Offloading events sent per peer
    events_per_peer = 1

    # Delay between offloading events (in milliseconds)
    event_sending_delay = 1000

    # Timeout for resource replies to select Solvers
    ssp_timeout = 5

    # Timeout for balance locking and resource reservation
    wrp_timeout = 5
