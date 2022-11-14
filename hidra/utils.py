from collections import Counter
from hashlib import sha1

from pyipv8.ipv8.keyvault.crypto import default_eccrypto
from pyipv8.ipv8.keyvault.keys import PrivateKey, PublicKey
from pyipv8.ipv8.peer import Peer

from hidra.settings import HIDRASettings


def get_peer_id(peer: Peer) -> str:
    return peer.mid.hex()[:HIDRASettings.peer_id_size:]


def get_object_id(peer_id: str, object_sn: int) -> int:
    object_id = (peer_id + str(object_sn)).encode("utf-8")
    return int(sha1(object_id).hexdigest()[:HIDRASettings.object_id_size:], base=16)


def sign_data(private_key: PrivateKey, data: str) -> bytes:
    data_hash = sha1(data.encode("utf-8")).digest()
    return default_eccrypto.create_signature(private_key, data_hash)


def verify_sign(public_key: PublicKey, data: str, signature: bytes) -> bool:
    data_hash = sha1(data.encode("utf-8")).digest()
    return default_eccrypto.is_valid_signature(public_key, data_hash, signature)


def select_own_solver(usage_offers: dict, reputation_offers: dict) -> str:
    higher_score = 0
    solver = ""

    # Iterate over all usage/reputation offers (ties depend on peer IDs)
    for k, v in usage_offers.items():
        peer_score = get_peer_score(v, reputation_offers[k])
        if peer_score > higher_score:
            higher_score = peer_score
            solver = k
        elif peer_score == higher_score and k < solver:
            solver = k

    return solver


def get_peer_score(usage_offer: int, reputation_offer: int) -> float:
    return (usage_offer * HIDRASettings.usage_factor) + (reputation_offer * HIDRASettings.reputation_factor)


def get_event_solver(votes: dict, required_votes: int) -> str:
    # Count votes and get the most voted
    counter = Counter(votes.values()).most_common(1)

    if counter and counter[0][1] >= required_votes:
        return counter[0][0]
    else:
        return ""
