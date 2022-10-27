from dataclasses import dataclass


@dataclass
class HIDRASettings:
    # HIDRA peer identifier size (in number of hexadecimal characters)
    peer_id_size: int = 4

    # HIDRA objects identifier size (maximum digits)
    object_id_size: int = 4

    # HIDRA resource usage size (maximum digits)
    usage_size: int = 5

    # Number of containers initially executed by each peer
    initial_container_count: int = 10

    # Execute a free-rider peer
    enable_free_rider: bool = True

    # Free-riding type: "over" or "under"
    free_riding_type: str = "over"
