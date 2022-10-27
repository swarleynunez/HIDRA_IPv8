from binascii import hexlify
from collections import Counter

from pyipv8.ipv8.peer import Peer

from hidra.settings import HIDRASettings


def get_peer_id(peer: Peer) -> str:
    return hexlify(peer.mid).decode()[:HIDRASettings.peer_id_size:]


def select_own_solver(usages: dict) -> str:
    minor_usage = float('inf')
    solver = ""

    # Iterate over all EventReply/usages
    for k, v in usages.items():
        if v < minor_usage:
            minor_usage = v
            solver = k

    return solver


def get_event_solver(votes: dict, required_votes: int) -> str:
    # Count votes and get the most voted
    count = Counter(votes.values()).most_common(1)

    return count[0][0] if count[0][1] == required_votes else None
