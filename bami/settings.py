from dataclasses import dataclass
from typing import Optional


@dataclass
class SimulationSettings:
    # Number of HIDRA domains
    domains: int = 1

    # Number of IPv8 peers per HIDRA domain
    peers_per_domain: int = 10

    # Number of IPv8 peers
    peers: int = domains * peers_per_domain

    # Maximum number of faulty peers per domain
    faulty_peers: int = peers_per_domain // 3

    # The name of the experiment
    name: str = ""

    # Whether to run the Yappi profiler
    profile: bool = False

    # An optional identifier for the experiment, appended to the working directory name
    identifier: Optional[str] = None

    # The duration of the simulation in seconds
    duration: int = 180

    # The logging level during the experiment
    logging_level: str = "INFO"

    # Whether we enable statistics like message sizes and frequencies
    enable_community_statistics: bool = True

    # Optional CSV file with a latency matrix (space-separated)
    latencies_file: Optional[str] = "data/latencies.txt"

    # The IPv8 ticker is responsible for community walking and discovering other peers, but can significantly limit
    # performance. Setting this option to False cancels the IPv8 ticker, improving performance
    enable_ipv8_ticker: bool = False
