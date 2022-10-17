from binascii import hexlify
from collections import Counter

from pyipv8.ipv8.peer import Peer

# Peer identifier size (in number of hexadecimal characters)
PEER_ID_SIZE = 4


def get_peer_id(peer: Peer) -> str:
    return hexlify(peer.mid).decode()[:PEER_ID_SIZE:]


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
