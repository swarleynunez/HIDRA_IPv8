from dataclasses import dataclass


@dataclass
class DeploymentSettings:
    # Number of HIDRA domains
    domains: int = 1

    # Number of IPv8 peers per HIDRA domain
    peers_per_domain: int = 10

    # Number of IPv8 peers
    peers: int = domains * peers_per_domain

    # Maximum number of faulty peers per domain
    faulty_peers: int = peers_per_domain // 3

    # The duration of the execution in seconds
    duration: int = 10

    # The logging level during the deployment
    logging_level: str = "INFO"

    # Whether we enable statistics like message sizes and frequencies
    enable_community_statistics: bool = True

    # The IPv8 ticker is responsible for community walking and discovering other peers, but can significantly limit
    # performance. Setting this option to False cancels the IPv8 ticker, improving performance
    enable_ipv8_ticker: bool = False
