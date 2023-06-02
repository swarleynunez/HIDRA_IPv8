from dataclasses import dataclass


@dataclass
class HIDRASettings:
    # Peer identifier size (in number of hexadecimal characters)
    peer_id_size: int = 8

    # Object identifier size (in number of hexadecimal characters)
    object_id_size: int = 16

    # Peers send offloading events to their parent domains (intra) or to other domains (inter)
    domain_selection_policy: str = "inter"

    # Offloading events sent per peer
    events_per_peer: int = 5

    # Delay between offloading events (in milliseconds)
    event_sending_delay: int = 0

    # Initial credits per peer
    initial_balance: int = 10 * events_per_peer

    # Initial resource units offer per peer
    max_resources: int = 1024 * events_per_peer

    # Timeout for resource replies to select Solvers (in seconds)
    ssp_timeout: int = 5

    # Timeout for credit locking and resource reservation (in seconds)
    wrp_timeout: int = 5

    # Execution time of workloads (in seconds)
    t_exec_value: int = 10

    # Payment (number of credits) per unit of time (second)
    p_ratio_value: int = 1

    # Monitoring requests sent per epoch
    requests_per_epoch: int = 0

    # Failed monitoring requests threshold
    failed_requests_threshold: int = 5

    # Display messages exchanged during a run
    message_debug: bool = False
