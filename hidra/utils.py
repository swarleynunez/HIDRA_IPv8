import json
from hashlib import sha1

from pyipv8.ipv8.keyvault.crypto import default_eccrypto
from pyipv8.ipv8.keyvault.keys import PrivateKey, PublicKey
from pyipv8.ipv8.peer import Peer

from hidra.settings import HIDRASettings


def get_peer_id(peer: Peer) -> str:
    return peer.mid.hex()[:HIDRASettings.peer_id_size:]


def get_object_id(peer_id: str, object_sn: int) -> str:
    object_id = (peer_id + str(object_sn)).encode("utf-8")
    return sha1(object_id).hexdigest()[:HIDRASettings.object_id_size:]


def sign_data(private_key: PrivateKey, data: str) -> bytes:
    data_hash = sha1(data.encode("utf-8")).digest()
    return default_eccrypto.create_signature(private_key, data_hash)


def verify_sign(public_key: PublicKey, data: str, signature: bytes) -> bool:
    data_hash = sha1(data.encode("utf-8")).digest()
    return default_eccrypto.is_valid_signature(public_key, data_hash, signature)


def hash_dict(dictionary: dict) -> str:
    return sha1(json.dumps(dictionary).encode("utf-8")).digest()
