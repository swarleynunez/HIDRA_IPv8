import json
import random
import time
from asyncio import get_event_loop
from binascii import unhexlify
from collections import Counter

from bami.settings import SimulationSettings
from hidra.caches import HIDRANumberCache
from hidra.payload import REQUEST_RESOURCE_INFO, RESOURCE_INFO, RequestResourceInfoPayload, ResourceInfoPayload, \
    NewEventPayload, NEW_EVENT, EVENT_REPLY, EVENT_COMMIT, EventReplyPayload, EventCommitPayload, NEW_RESERVATION, \
    RESERVATION_REPLY, RESERVATION_COMMIT, NewReservationPayload, ReservationReplyPayload, ReservationCommitPayload
from hidra.settings import HIDRASettings
from hidra.types import HIDRAPeerInfo, IPv8PendingMessage, HIDRAEventInfo, HIDRAWorkload, HIDRAPeer, HIDRAEvent
from hidra.utils import get_peer_id, get_object_id, sign_data, verify_sign
from pyipv8.ipv8.community import Community
from pyipv8.ipv8.lazy_community import lazy_wrapper
from pyipv8.ipv8.peer import Peer
from pyipv8.ipv8.requestcache import RequestCache

# TODO. Cache prefixes
EVENT_PREFIX = "HIDRA_events"
MESSAGES_PREFIX = "IPv8_messages"

# Debug
APPLICANT = None


class HIDRACommunity(Community):
    """
    HIDRA community
    """

    community_id = unhexlify("2d606de41ee6595b2d3d5c57065b78bf17870f32")

    def __init__(self, my_peer, endpoint, network) -> None:
        super().__init__(my_peer, endpoint, network)

        # Domains
        self.next_sn_d = 0
        self.domains = {}
        self.parent_domain_id = None

        # Peers
        self.peers = {}
        self.my_peer_id = get_peer_id(self.my_peer)

        # Events
        self.next_sn_e = 0
        self.events = {}

        # TODO. IPv8 Messages
        self.messages = {}
        self.message_sn = 0

        # TODO. Overlay cache
        self.cache = RequestCache()

        # Debug
        global APPLICANT
        if not APPLICANT:
            APPLICANT = self.my_peer_id

        # Message handlers
        self.add_message_handler(REQUEST_RESOURCE_INFO, self.on_request_resource_info)
        self.add_message_handler(RESOURCE_INFO, self.on_resource_info)
        self.add_message_handler(NEW_EVENT, self.on_new_event)
        self.add_message_handler(EVENT_REPLY, self.on_event_reply)
        self.add_message_handler(EVENT_COMMIT, self.on_event_commit)
        self.add_message_handler(NEW_RESERVATION, self.on_new_reservation)
        self.add_message_handler(RESERVATION_REPLY, self.on_reservation_reply)
        self.add_message_handler(RESERVATION_COMMIT, self.on_reservation_commit)

    ########
    # Peer #
    ########
    async def start(self):
        # Register asyncio tasks with the community
        # This ensures that tasks end when the community is unloaded
        self.register_task("initialize_peer", self.initialize_peer)
        await self.wait_for_tasks()

        # Debug
        if self.my_peer_id == APPLICANT:
            for _ in range(1):
                # Solver Selection Phase (SSP)
                self.register_task("ssp_" + str(self.next_sn_e), self.ssp, self.next_sn_e)

                # Workload Reservation Phase (WRP)
                self.register_task("wrp_" + str(self.next_sn_e), self.wrp, self.next_sn_e,
                                   delay=HIDRASettings.ssp_timeout)

                # Workload Execution Phase (WEP)
                self.register_task("wep_" + str(self.next_sn_e), self.wep, self.next_sn_e,
                                   delay=HIDRASettings.ssp_timeout + HIDRASettings.wrp_timeout)

                # Update storage
                self.next_sn_e += 1

    def initialize_peer(self) -> None:
        # Initialize system domains deterministically
        self.set_domains()

        # Initialize parent domain peers information
        balance = HIDRASettings.initial_balance
        r_max = HIDRASettings.max_resources
        for peer in self.domains[self.parent_domain_id]:
            # Update storage
            p = HIDRAPeer()
            p.peer_info = HIDRAPeerInfo(0, balance, r_max, r_max)
            self.peers[get_peer_id(peer)] = p

    def set_domains(self) -> None:
        # Sort array of IPv8 peers
        peers = self.get_peers()
        peers.append(self.my_peer)
        peers.sort(key=lambda o: o.mid)

        # Set system domains
        n = SimulationSettings.peers_per_domain
        for i in range(0, len(peers), n):
            domain_peers = peers[i:i + n]

            # Set parent domain
            if not self.parent_domain_id:
                for peer in domain_peers:
                    if self.my_peer.mid == peer.mid:
                        self.parent_domain_id = self.next_sn_d
                        break

            # Update storage
            self.domains[self.next_sn_d] = domain_peers
            self.next_sn_d += 1

    # TODO
    def add_pending_message(self, sender, payload) -> None:
        # Get the next message ID
        message_id = get_object_id(self.my_peer_id, self.message_sn)
        self.message_sn += 1

        # Update storage and cache
        self.messages[message_id] = IPv8PendingMessage(sender, payload)
        self.cache.add(HIDRANumberCache(self.cache, MESSAGES_PREFIX, message_id))

    # TODO
    def process_pending_messages(self):
        for k, v in list(self.messages.items()):
            # Update storage and cache
            self.messages.pop(k)
            self.cache.pop(MESSAGES_PREFIX, k)

            if v.payload.msg_id == REQUEST_RESOURCE_INFO:
                # Process pending 'RequestResourceInfo' message
                self.process_request_resource_info_message(v.sender, v.payload)
            elif v.payload.msg_id == RESOURCE_INFO:
                # Process pending 'ResourceInfo' message
                self.process_resource_info_message(v.sender, v.payload)

    # TODO
    async def unload(self) -> None:
        await self.cache.shutdown()
        await super().unload()

    #############
    # Callbacks #
    #############
    @lazy_wrapper(RequestResourceInfoPayload)
    def on_request_resource_info(self, sender, payload) -> None:
        self.process_request_resource_info_message(sender, payload)

    @lazy_wrapper(ResourceInfoPayload)
    def on_resource_info(self, sender, payload) -> None:
        self.process_resource_info_message(sender, payload)

    @lazy_wrapper(NewEventPayload)
    def on_new_event(self, sender, payload) -> None:
        self.process_new_event_message(sender, payload)

    @lazy_wrapper(EventReplyPayload)
    def on_event_reply(self, sender, payload) -> None:
        self.process_event_reply_message(sender, payload)

    @lazy_wrapper(EventCommitPayload)
    def on_event_commit(self, sender, payload) -> None:
        self.process_event_commit_message(sender, payload)

    @lazy_wrapper(NewReservationPayload)
    def on_new_reservation(self, sender, payload) -> None:
        self.process_new_reservation_message(sender, payload)

    @lazy_wrapper(ReservationReplyPayload)
    def on_reservation_reply(self, sender, payload) -> None:
        self.process_reservation_reply_message(sender, payload)

    @lazy_wrapper(ReservationCommitPayload)
    def on_reservation_commit(self, sender, payload) -> None:
        self.process_reservation_commit_message(sender, payload)

    #########
    # Tasks #
    #########
    def ssp(self, sn_e) -> None:
        # Payload data
        workload = HIDRAWorkload("nginx", 1024, 8888)
        event_info = HIDRAEventInfo(workload, 1800, 1, int(time.time()) + 180)

        # Select a domain
        domain_id = self.select_domain()

        # Update storage
        self.events[get_object_id(self.my_peer_id, sn_e)] = HIDRAEvent(event_info, domain_id)

        # Debug
        print("[Time:" + str(get_event_loop().time()) +
              "][Domain:" + str(self.parent_domain_id) +
              "][Peer:" + self.my_peer_id +
              "] Sending RequestResourceInfo ---> " +
              "Domain:" + str(domain_id) +
              ", Event:" + str(sn_e))

        # Applicant sends RequestResourceInfo messages to the selected domain
        for peer in self.domains[domain_id]:
            self.ez_send(peer, RequestResourceInfoPayload(sn_e, event_info))

    def process_request_resource_info_message(self, sender, payload) -> None:
        sender_id = get_peer_id(sender)

        # Payload data
        # TODO. Check event info to make a decision
        available = random.choice([True, False])
        domain_info = {}
        for peer in self.domains[self.parent_domain_id]:
            peer_id = get_peer_id(peer)
            domain_info[peer_id] = self.peers[peer_id].peer_info

        # Debug
        print("[Time:" + str(get_event_loop().time()) +
              "][Domain:" + str(self.parent_domain_id) +
              "][Peer:" + self.my_peer_id +
              "] Sending ResourceInfo --->" +
              " Peer:" + sender_id +
              ", Event:" + str(payload.sn_e) +
              ", Available:" + ("T" if available else "F"))

        # Selected domain sends ResourceInfo messages to the Applicant
        self.ez_send(sender, ResourceInfoPayload(payload.sn_e, available, domain_info))

    def process_resource_info_message(self, sender, payload) -> None:
        sender_id = get_peer_id(sender)

        # Is the sender peer available to manage the offloading event?
        if payload.available:
            self.events[get_object_id(self.my_peer_id, payload.sn_e)].available_peers.append(sender_id)

        # Deliver resource replies
        for k, v in payload.resource_replies.items():
            # Update storage
            if k not in self.peers:
                p = HIDRAPeer()
                p.resource_replies[sender_id] = v
                self.peers[k] = p
            else:
                self.peers[k].resource_replies[sender_id] = v

            # Select peer info from resource replies
            peer_info = self.select_peer_info(self.peers[k].resource_replies)
            if peer_info:
                # Update storage
                self.peers[k].peer_info = peer_info

    def wrp(self, sn_e):
        event = self.events[get_object_id(self.my_peer_id, sn_e)]

        # Requirements
        if len(event.available_peers) == 0:
            # TODO. Select another domain
            print("INFO ---> Domain:" + str(event.domain_id) + " not available for Event:" + str(sn_e))
            return

        # Payload data
        solver_id = random.choice(event.available_peers)

        # Update storage
        event.solver_id = solver_id

        # Debug
        print("\n[Time:" + str(get_event_loop().time()) +
              "][Domain:" + str(self.parent_domain_id) +
              "][Peer:" + self.my_peer_id +
              "] Sending NewEvent ---> " +
              "Domain:" + str(self.parent_domain_id) +
              ", Event:" + str(sn_e) +
              ", Solver:" + str(solver_id) +
              ", Timeout:" + str(event.event_info.ts_start))

        # Applicant sends NewEvent messages to its parent domain
        for peer in self.domains[self.parent_domain_id]:
            self.ez_send(peer, NewEventPayload(sn_e, event.domain_id, solver_id, event.event_info))

    def process_new_event_message(self, sender, payload) -> None:
        sender_id = get_peer_id(sender)
        peer = self.peers[sender_id]
        deposit = payload.event_info.t_exec_value * payload.event_info.p_ratio_value
        event_id = get_object_id(sender_id, payload.sn_e)

        # Requirements
        if payload.sn_e != peer.peer_info.sn_e:
            print("INFO ---> Missing past offloading events for Peer:" + sender_id)
            return
        if deposit > self.get_available_balance(peer):
            print("INFO ---> Peer:" + sender_id + " does not have enough balance for Event:" + str(payload.sn_e))
            return

        # Update storage
        peer.peer_info.sn_e += 1
        peer.deposits[payload.sn_e] = deposit
        self.events[event_id] = HIDRAEvent(payload.event_info, payload.to_domain_id)
        self.events[event_id].solver_id = payload.solver_id

        # Format and sign event data
        data = sender_id + ":" + \
               str(payload.sn_e) + ":" + \
               str(payload.to_domain_id) + ":" + \
               payload.solver_id + ":" + \
               str(payload.event_info)
        signature = sign_data(self.my_peer.key, data)

        # Debug
        print("[Time:" + str(get_event_loop().time()) +
              "][Domain:" + str(self.parent_domain_id) +
              "][Peer:" + self.my_peer_id +
              "] Sending EventReply ---> " +
              " Peer:" + sender_id +
              ", Event:" + str(payload.sn_e) +
              ", Deposit:" + str(deposit))

        # Parent domain sends EventReply messages to the Applicant
        self.ez_send(sender, EventReplyPayload(payload.sn_e, signature))

    def process_event_reply_message(self, sender, payload) -> None:
        sender_id = get_peer_id(sender)
        event = self.events[get_object_id(self.my_peer_id, payload.sn_e)]

        # Requirements
        if len(event.locking_qc) == self.required_replies_for_quorum():
            # Dismiss the message...
            return
        data = self.my_peer_id + ":" + \
               str(payload.sn_e) + ":" + \
               str(event.domain_id) + ":" + \
               event.solver_id + ":" + \
               str(event.event_info)
        if not verify_sign(sender.public_key, data, payload.signature):
            print("INFO ---> Peer:" + sender_id + " sent an invalid signature of Event:" + str(payload.sn_e))
            return

        # Update storage
        event.locking_qc[sender_id] = str(payload.signature, "latin1")  # Due to JSON bytes serialization

        # Received required replies?
        if len(event.locking_qc) == self.required_replies_for_quorum():
            peer = self.get_peer_from_id(event.domain_id, event.solver_id)
            if peer:
                # Debug
                print("[Time:" + str(get_event_loop().time()) +
                      "][Domain:" + str(self.parent_domain_id) +
                      "][Peer:" + self.my_peer_id +
                      "] Sending NewCommit ---> " +
                      "Domain:" + str(event.domain_id) +
                      ", Peer:" + event.solver_id +
                      ", Event:" + str(payload.sn_e))

                # Applicant sends NewCommit message to the Solver
                self.ez_send(peer, EventCommitPayload(payload.sn_e,
                                                      event.domain_id,
                                                      event.event_info,
                                                      event.locking_qc))
            else:
                print("INFO ---> Peer:" + event.solver_id + " not in Domain:" + str(event.domain_id))

    def process_event_commit_message(self, sender, payload) -> None:
        sender_id = get_peer_id(sender)

        # Requirements
        if len(payload.locking_qc) != self.required_replies_for_quorum():
            print("INFO ---> Invalid locking quorum certificate for Event:" + str(payload.sn_e))
            return
        for k, v in payload.locking_qc.items():
            data = sender_id + ":" + \
                   str(payload.sn_e) + ":" + \
                   str(self.parent_domain_id) + ":" + \
                   self.my_peer_id + ":" + \
                   str(payload.event_info)
            if not verify_sign(self.get_peer_from_id(payload.from_domain_id, k).public_key, data, bytes(v, "latin1")):
                print("INFO ---> Invalid locking quorum certificate for Event:" + str(payload.sn_e))
                return

        # Update storage

    def process_new_reservation_message(self, sender, payload) -> None:
        pass

    def process_reservation_reply_message(self, sender, payload) -> None:
        pass

    def process_reservation_commit_message(self, sender, payload) -> None:
        pass

    def wep(self, sn_e):
        pass

    #########
    # Utils #
    #########
    def select_domain(self) -> int:
        if len(self.domains) > 1 and HIDRASettings.domain_selection_policy == "inter":
            while True:
                i = random.randint(0, len(self.domains) - 1)
                if i != self.parent_domain_id:
                    return i
        else:
            return self.parent_domain_id

    def select_peer_info(self, resource_replies: dict) -> HIDRAPeerInfo:
        # Count equal resource replies
        rr = (json.dumps(v, default=lambda o: o.__dict__).encode("utf-8") for v in resource_replies.values())
        counter = Counter(rr).most_common(1)

        # f + 1 equal resource replies?
        if counter[0][1] == self.required_replies_for_proof():
            return HIDRAPeerInfo(**json.loads(counter[0][0]))

    @staticmethod
    def get_available_balance(peer: HIDRAPeer) -> int:
        # Count total deposited/locked
        deposited = 0
        for _, v in peer.deposits.items():
            deposited += v

        return peer.peer_info.balance - deposited

    @staticmethod
    def required_replies_for_quorum() -> int:
        """
        Number of required replies to get a quorum certificate on a message
        """

        return 2 * SimulationSettings.faulty_peers + 1

    @staticmethod
    def required_replies_for_proof() -> int:
        """
        Number of required replies to prove a message or quorum certificate
        """

        return SimulationSettings.faulty_peers + 1

    def get_peer_from_id(self, domain_id, peer_id) -> Peer:
        for peer in self.domains[domain_id]:
            if get_peer_id(peer) == peer_id:
                return peer
