from dataclasses import dataclass


@dataclass
class HIDRASettings:
    # Peer identifier size (in number of hexadecimal characters)
    peer_id_size: int = 8

    # Object identifier size (in number of hexadecimal characters)
    object_id_size: int = 16

    # Initial balance units per peer
    initial_balance: int = 1000000

    # Initial resource units offer per peer
    max_resources: int = 1000000

    # Peers send offloading events to their parent domains (intra) or to other domains (inter)
    domain_selection_policy: str = "inter"

    # Offloading events sent per peer
    events_per_peer: int = 50

    # Delay between offloading events (in milliseconds)
    event_sending_delay: int = 0

    # Timeout for resource replies to select Solvers (in seconds)
    ssp_timeout: int = 5

    # Timeout for balance locking and resource reservation (in seconds)
    wrp_timeout: int = 5

    # Display messages exchanged during a run
    message_debug: bool = True
