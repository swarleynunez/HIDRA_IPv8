import json
import random
import time
from asyncio import get_event_loop
from binascii import unhexlify
from collections import Counter

from bami.settings import SimulationSettings
from hidra.payload import REQUEST_RESOURCE_INFO, RESOURCE_INFO, RequestResourceInfoPayload, ResourceInfoPayload, \
    LockingSendPayload, LOCKING_SEND, LOCKING_ECHO, LOCKING_READY, LockingEchoPayload, LockingCreditPayload, \
    RESERVATION_ECHO, RESERVATION_READY, RESERVATION_CREDIT, LOCKING_CREDIT, RESERVATION_DENY, LockingReadyPayload, \
    ReservationEchoPayload, ReservationDenyPayload, ReservationCreditPayload, ReservationReadyPayload
from hidra.settings import HIDRASettings
from hidra.types import HIDRAPeerInfo, HIDRAEventInfo, HIDRAWorkload, HIDRAPeer, HIDRAEvent, IPv8PendingMessage
from hidra.utils import get_peer_id, get_object_id, verify_sign, hash_data
from pyipv8.ipv8.community import Community
from pyipv8.ipv8.lazy_community import lazy_wrapper
from pyipv8.ipv8.peer import Peer

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
        self.domains = {}
        self.parent_domain_id = None

        # Peers
        self.peers = {}
        self.my_peer_id = get_peer_id(self.my_peer)

        # Events
        self.events = {}

        # IPv8 Messages
        self.messages = {}

        # Counters
        self.next_sn_d = 0
        self.next_sn_e = 0
        self.next_sn_m = 0

        # Debug
        global APPLICANT
        if not APPLICANT:
            APPLICANT = self.my_peer_id

        # Message handlers
        self.add_message_handler(REQUEST_RESOURCE_INFO, self.on_request_resource_info)
        self.add_message_handler(RESOURCE_INFO, self.on_resource_info)
        self.add_message_handler(LOCKING_SEND, self.on_locking_send)
        self.add_message_handler(LOCKING_ECHO, self.on_locking_echo)
        self.add_message_handler(LOCKING_READY, self.on_locking_ready)
        self.add_message_handler(LOCKING_CREDIT, self.on_locking_credit)
        self.add_message_handler(RESERVATION_ECHO, self.on_reservation_echo)
        self.add_message_handler(RESERVATION_DENY, self.on_reservation_deny)
        self.add_message_handler(RESERVATION_READY, self.on_reservation_ready)
        self.add_message_handler(RESERVATION_CREDIT, self.on_reservation_credit)

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
                                   delay=HIDRASettings.ssp_timeout + HIDRASettings.wrp_start_timestamp)

                # Update storage
                self.next_sn_e += 1

    def initialize_peer(self) -> None:
        # Initialize system domains deterministically
        self.set_domains()

        # Initialize peers information per domain
        balance = HIDRASettings.initial_balance
        r_max = HIDRASettings.max_resources
        for domain in self.domains.values():
            for peer in domain:
                # Update storage
                p = HIDRAPeer()
                p.info = HIDRAPeerInfo(balance, r_max, 0, 0)
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

    def add_pending_message(self, sender, payload) -> None:
        # Get the next message ID
        message_id = get_object_id(self.my_peer_id, self.next_sn_m)
        self.next_sn_m += 1

        # Update storage and cache
        self.messages[message_id] = IPv8PendingMessage(sender, payload)

    def process_pending_messages(self):
        for k, v in list(self.messages.items()):
            # Update storage and cache
            self.messages.pop(k)

            if v.payload.msg_id == LOCKING_ECHO:
                # Process pending 'LockingEcho' message
                self.process_locking_echo_message(v.sender, v.payload)
            # elif v.payload.msg_id == LOCKING_READY:
            #     # Process pending 'LockingReady' message
            #     self.process_locking_ready_message(v.sender, v.payload)
            elif v.payload.msg_id == RESERVATION_ECHO:
                # Process pending 'ReservationEcho' message
                self.process_reservation_echo_message(v.sender, v.payload)
            # elif v.payload.msg_id == RESERVATION_READY:
            #     # Process pending 'ReservationReady' message
            #     self.process_reservation_ready_message(v.sender, v.payload)

    #############
    # Callbacks #
    #############
    @lazy_wrapper(RequestResourceInfoPayload)
    def on_request_resource_info(self, sender, payload) -> None:
        self.process_request_resource_info_message(sender, payload)

    @lazy_wrapper(ResourceInfoPayload)
    def on_resource_info(self, sender, payload) -> None:
        self.process_resource_info_message(sender, payload)

    @lazy_wrapper(LockingSendPayload)
    def on_locking_send(self, sender, payload) -> None:
        self.process_locking_send_message(sender, payload)

    @lazy_wrapper(LockingEchoPayload)
    def on_locking_echo(self, sender, payload) -> None:
        self.process_pending_messages()
        self.process_locking_echo_message(sender, payload)

    @lazy_wrapper(LockingReadyPayload)
    def on_locking_ready(self, sender, payload) -> None:
        # self.process_pending_messages()
        self.process_locking_ready_message(sender, payload)

    @lazy_wrapper(LockingCreditPayload)
    def on_locking_credit(self, sender, payload) -> None:
        self.process_locking_credit_message(sender, payload)

    @lazy_wrapper(ReservationEchoPayload)
    def on_reservation_echo(self, sender, payload) -> None:
        self.process_pending_messages()
        self.process_reservation_echo_message(sender, payload)

    @lazy_wrapper(ReservationDenyPayload)
    def on_reservation_deny(self, sender, payload) -> None:
        self.process_reservation_deny_message(sender, payload)

    @lazy_wrapper(ReservationReadyPayload)
    def on_reservation_ready(self, sender, payload) -> None:
        # self.process_pending_messages()
        self.process_reservation_ready_message(sender, payload)

    @lazy_wrapper(ReservationCreditPayload)
    def on_reservation_credit(self, sender, payload) -> None:
        self.process_reservation_credit_message(sender, payload)

    #########
    # Tasks #
    #########
    # APPLICANT
    def ssp(self, sn_e) -> None:
        event_id = get_object_id(self.my_peer_id, sn_e)

        # Select a domain
        to_domain_id = self.select_domain()

        # Payload data
        workload = HIDRAWorkload("nginx", 1024, 8888)
        event_info = HIDRAEventInfo(self.parent_domain_id, to_domain_id, "", workload, 1800, 1, int(time.time()) + 180)

        # Update storage
        self.events[event_id] = HIDRAEvent()
        self.events[event_id].info = event_info

        # Debug
        print("SSP:"
              "\n[Time:" + format(get_event_loop().time(), ".3f") +
              "][Domain:" + str(self.parent_domain_id) +
              "][Peer:" + self.my_peer_id +
              "] Sending RequestResourceInfo ---> " +
              "Domain:" + str(to_domain_id) +
              ", Event:" + str(sn_e))

        # Applicant sends RequestResourceInfo messages to the selected domain
        for peer in self.domains[to_domain_id]:
            self.ez_send(peer, RequestResourceInfoPayload(sn_e, event_info))

    # SELECTED DOMAIN
    def process_request_resource_info_message(self, sender, payload) -> None:
        sender_id = get_peer_id(sender)

        # Payload data
        # TODO. Check event info to make a decision
        available = random.choice([True, False])
        domain_info = {}
        for peer in self.domains[self.parent_domain_id]:
            peer_id = get_peer_id(peer)
            domain_info[peer_id] = self.peers[peer_id].info

        # Debug
        print("[Time:" + format(get_event_loop().time(), ".3f") +
              "][Domain:" + str(self.parent_domain_id) +
              "][Peer:" + self.my_peer_id +
              "] Sending ResourceInfo --->" +
              " Peer:" + sender_id +
              ", Event:" + str(payload.sn_e) +
              ", Available:" + ("T" if available else "F"))

        # Selected domain sends ResourceInfo messages to the Applicant
        self.ez_send(sender, ResourceInfoPayload(payload.sn_e, available, domain_info))

    # APPLICANT
    def process_resource_info_message(self, sender, payload) -> None:
        sender_id = get_peer_id(sender)

        # Is the sender peer available to manage the offloading event?
        if payload.available:
            self.events[get_object_id(self.my_peer_id, payload.sn_e)].available_peers.append(sender_id)

        # Deliver resource replies
        for k, v in payload.resource_replies.items():
            # Update storage
            self.peers[k].resource_replies[sender_id] = v

            # Select peer info from resource replies
            peer_info = self.select_peer_info(self.peers[k].resource_replies)
            if peer_info:
                # Update storage
                self.peers[k].info = peer_info

    # APPLICANT
    def wrp(self, sn_e):
        event = self.events[get_object_id(self.my_peer_id, sn_e)]

        # Requirements
        if len(event.available_peers) == 0:
            # TODO. Select another domain
            print("INFO ---> Domain:" + str(event.info.to_domain_id) + " not available for Event:" + str(sn_e))
            return

        # Payload data
        to_solver_id = random.choice(event.available_peers)

        # Update storage
        event.info.to_solver_id = to_solver_id

        # Debug
        print("\nWRP:"
              "\n[Time:" + format(get_event_loop().time(), ".3f") +
              "][Domain:" + str(self.parent_domain_id) +
              "][Peer:" + self.my_peer_id +
              "] Sending LockingSend ---> " +
              "Domain:" + str(self.parent_domain_id) +
              ", Event:" + str(sn_e) +
              ", Solver:" + str(to_solver_id))

        # Applicant sends LockingSend messages to its parent domain
        for peer in self.domains[self.parent_domain_id]:
            self.ez_send(peer, LockingSendPayload(sn_e, event.info))

    # APPLICANT PARENT DOMAIN
    def process_locking_send_message(self, sender, payload) -> None:
        sender_id = get_peer_id(sender)
        peer = self.peers[sender_id]
        deposit = payload.event_info.t_exec_value * payload.event_info.p_ratio_value
        event_id = get_object_id(sender_id, payload.sn_e)

        # Requirements
        if self.exist_event(event_id) and self.events[event_id].locking_echo_sent:
            # Dismiss the message...
            return
        if payload.sn_e != peer.info.sn_e:
            # TODO: wait for or request previous sn_e
            print("INFO ---> Missing past events for Peer:" + sender_id)
            return
        if deposit > self.get_available_balance(peer):
            print("INFO ---> Peer:" + sender_id + " does not have enough balance for Event:" + str(payload.sn_e))
            return

        # Update storage
        peer.info.sn_e += 1
        peer.deposits[payload.sn_e] = deposit
        self.events[event_id] = HIDRAEvent()
        self.events[event_id].info = payload.event_info
        self.events[event_id].locking_echo_sent = True

        # Debug
        print("[Time:" + format(get_event_loop().time(), ".3f") +
              "][Domain:" + str(self.parent_domain_id) +
              "][Peer:" + self.my_peer_id +
              "] Sending LockingEcho ---> " +
              "Domain:" + str(self.parent_domain_id) +
              ", Event:" + str(payload.sn_e) +
              ", Applicant:" + sender_id +
              ", Deposit:" + str(deposit))

        # Applicant parent domain sends LockingEcho messages to itself
        for peer in self.domains[self.parent_domain_id]:
            self.ez_send(peer, LockingEchoPayload(sender_id, payload.sn_e, payload.event_info))

    # APPLICANT PARENT DOMAIN
    def process_locking_echo_message(self, sender, payload) -> None:
        sender_id = get_peer_id(sender)
        event_id = get_object_id(payload.applicant_id, payload.sn_e)

        # Requirements
        if not self.exist_event(event_id):
            self.add_pending_message(sender, payload)
            return
        event = self.events[event_id]
        if sender_id in event.locking_echos:
            # Dismiss the message...
            return
        if self.has_required_replies(event.locking_echos, self.required_replies_for_quorum()):
            # Dismiss the message...
            return

        # Format and hash event data
        data = payload.applicant_id + ":" + \
               str(payload.sn_e) + ":" + \
               str(payload.event_info)

        # Update storage
        event.locking_echos[sender_id] = hash_data(data)

        # Required equal echos for a locking?
        if self.has_required_replies(event.locking_echos, self.required_replies_for_quorum()):
            # Debug
            print("[Time:" + format(get_event_loop().time(), ".3f") +
                  "][Domain:" + str(self.parent_domain_id) +
                  "][Peer:" + self.my_peer_id +
                  "] Sending LockingReady ---> " +
                  "Domain:" + str(self.parent_domain_id) +
                  ", Event:" + str(payload.sn_e))

            # Applicant parent domain sends LockingReady messages to itself
            for peer in self.domains[self.parent_domain_id]:
                self.ez_send(peer, LockingReadyPayload(payload.applicant_id, payload.sn_e, payload.event_info))

    # APPLICANT PARENT DOMAIN
    def process_locking_ready_message(self, sender, payload) -> None:
        sender_id = get_peer_id(sender)
        event_id = get_object_id(payload.applicant_id, payload.sn_e)

        # Requirements
        # if not self.exist_event(event_id):
        #     self.add_pending_message(sender, payload)
        #     return
        event = self.events[event_id]
        if sender_id in event.locking_readys:
            # Dismiss the message...
            return
        if self.has_required_replies(event.locking_readys, self.required_replies_for_proof()):
            # Dismiss the message...
            return

        # Format and hash event data
        data = payload.applicant_id + ":" + \
               str(payload.sn_e) + ":" + \
               str(payload.event_info)

        # Update storage
        event.locking_readys[sender_id] = hash_data(data)

        # Required equal readys for a locking?
        if self.has_required_replies(event.locking_readys, self.required_replies_for_proof()):
            # Debug
            print("[Time:" + format(get_event_loop().time(), ".3f") +
                  "][Domain:" + str(self.parent_domain_id) +
                  "][Peer:" + self.my_peer_id +
                  "] Sending LockingCredit ---> " +
                  "Domain:" + str(payload.event_info.to_domain_id) +
                  ", Event:" + str(payload.sn_e))

            # Applicant parent domain sends LockingCredit messages to Solver parent domain
            for peer in self.domains[payload.event_info.to_domain_id]:
                self.ez_send(peer, LockingCreditPayload(payload.applicant_id, payload.sn_e, payload.event_info))

    # SOLVER PARENT DOMAIN
    def process_locking_credit_message(self, sender, payload) -> None:
        sender_id = get_peer_id(sender)
        event_id = get_object_id(payload.applicant_id, payload.sn_e)

        # Requirements
        if not self.exist_event(event_id):
            self.events[event_id] = HIDRAEvent()
        event = self.events[event_id]
        if sender_id in event.locking_credits:
            # Dismiss the message...
            return
        if self.has_required_replies(event.locking_credits, self.required_replies_for_proof()):
            # Dismiss the message...
            return

        # Format and hash event data
        data = payload.applicant_id + ":" + \
               str(payload.sn_e) + ":" + \
               str(payload.event_info)

        # Update storage
        event.locking_credits[sender_id] = hash_data(data)

        # Required equal credits for a locking?
        if self.has_required_replies(event.locking_credits, self.required_replies_for_proof()):
            solver_id = payload.event_info.to_solver_id

            # Payload data
            sn_r = self.peers[solver_id].info.sn_r

            # Update storage
            self.peers[payload.applicant_id].info.sn_e += 1
            event.info = payload.event_info
            event.sn_r = sn_r

            # Debug
            print("[Time:" + format(get_event_loop().time(), ".3f") +
                  "][Domain:" + str(self.parent_domain_id) +
                  "][Peer:" + self.my_peer_id +
                  "] Sending ReservationEcho ---> " +
                  "Domain:" + str(self.parent_domain_id) +
                  ", Event:" + str(payload.sn_e) +
                  ", Solver:" + solver_id +
                  ", Reservation:" + str(sn_r))

            # Solver parent domain sends ReservationEcho messages to itself
            for peer in self.domains[self.parent_domain_id]:
                self.ez_send(peer, ReservationEchoPayload(payload.applicant_id, payload.sn_e, sn_r))

    # SOLVER PARENT DOMAIN
    def process_reservation_echo_message(self, sender, payload) -> None:
        sender_id = get_peer_id(sender)
        event_id = get_object_id(payload.applicant_id, payload.sn_e)

        # Requirements
        if not self.exist_event(event_id):
            self.add_pending_message(sender, payload)
            return
        event = self.events[event_id]
        if sender_id in event.reservation_echos:
            # Dismiss the message...
            return
        if self.has_required_replies(event.reservation_echos, self.required_replies_for_quorum()):
            # Dismiss the message...
            return

        # Format and hash reservation data
        data = payload.applicant_id + ":" + str(payload.sn_e) + ":" + str(payload.sn_r)

        # Update storage
        event.reservation_echos[sender_id] = hash_data(data)

        # Required equal echos for a reservation?
        if self.has_required_replies(event.reservation_echos, self.required_replies_for_quorum()):
            # Requirements
            solver_id = event.info.to_solver_id
            solver = self.peers[solver_id]
            if payload.sn_r != solver.info.sn_r:
                # TODO: f + 1 largest sn_r?
                # TODO: wait for or request previous sn_r
                print("INFO ---> Missing past reservations for Peer:" + solver_id)
                return

            # Update storage
            solver.info.sn_r += 1

            # Requirements
            resource_limit = event.info.workload.resource_limit
            if resource_limit > self.get_free_resources(solver):
                # Debug
                print("[Time:" + format(get_event_loop().time(), ".3f") +
                      "][Domain:" + str(self.parent_domain_id) +
                      "][Peer:" + self.my_peer_id +
                      "] Sending ReservationDeny ---> " +
                      "Domain:" + str(event.info.from_domain_id) +
                      ", Event:" + str(payload.sn_e))

                # Solver parent domain sends ReservationDeny messages to Applicant parent domain
                for peer in self.domains[event.info.from_domain_id]:
                    self.ez_send(peer, ReservationDenyPayload(payload.applicant_id, payload.sn_e, payload.sn_r))
                return

            # Debug
            print("[Time:" + format(get_event_loop().time(), ".3f") +
                  "][Domain:" + str(self.parent_domain_id) +
                  "][Peer:" + self.my_peer_id +
                  "] Sending ReservationReady ---> " +
                  "Domain:" + str(self.parent_domain_id) +
                  ", Event:" + str(payload.sn_e))

            # Solver parent domain sends ReservationReady messages to itself
            for peer in self.domains[self.parent_domain_id]:
                self.ez_send(peer, ReservationReadyPayload(payload.applicant_id, payload.sn_e, payload.sn_r))

    # APPLICANT PARENT DOMAIN
    def process_reservation_deny_message(self, sender, payload) -> None:
        sender_id = get_peer_id(sender)
        event = self.events[get_object_id(payload.applicant_id, payload.sn_e)]

        # Requirements
        if sender_id in event.reservation_denys:
            # Dismiss the message...
            return
        if self.has_required_replies(event.reservation_denys, self.required_replies_for_proof()):
            # Dismiss the message...
            return

        # Format and hash reservation data
        data = payload.applicant_id + ":" + str(payload.sn_e) + ":" + str(payload.sn_r)

        # Update storage
        event.reservation_denys[sender_id] = hash_data(data)

        # Required equal denys for a reservation?
        if self.has_required_replies(event.reservation_denys, self.required_replies_for_proof()):
            # Update storage
            del self.peers[payload.applicant_id].deposits[payload.sn_e]
            self.peers[event.info.to_solver_id].info.sn_r += 1

    # SOLVER PARENT DOMAIN
    def process_reservation_ready_message(self, sender, payload) -> None:
        sender_id = get_peer_id(sender)
        event_id = get_object_id(payload.applicant_id, payload.sn_e)

        # Requirements
        # if not self.exist_event(event_id):
        #     self.add_pending_message(sender, payload)
        #     return
        event = self.events[event_id]
        if sender_id in event.reservation_readys:
            # Dismiss the message...
            return
        if self.has_required_replies(event.reservation_readys, self.required_replies_for_proof()):
            # Dismiss the message...
            return

        # Format and hash reservation data
        data = payload.applicant_id + ":" + str(payload.sn_e) + ":" + str(payload.sn_r)

        # Update storage
        event.reservation_readys[sender_id] = hash_data(data)

        # Required equal readys for a reservation?
        if self.has_required_replies(event.reservation_readys, self.required_replies_for_proof()):
            # Debug
            print("[Time:" + format(get_event_loop().time(), ".3f") +
                  "][Domain:" + str(self.parent_domain_id) +
                  "][Peer:" + self.my_peer_id +
                  "] Sending ReservationCredit ---> " +
                  "Domain:" + str(event.info.from_domain_id) +
                  ", Event:" + str(payload.sn_e))

            # Solver parent domain sends ReservationCredit messages to Applicant parent domain
            for peer in self.domains[event.info.from_domain_id]:
                self.ez_send(peer, ReservationCreditPayload(payload.applicant_id, payload.sn_e, payload.sn_r))

    # APPLICANT PARENT DOMAIN
    def process_reservation_credit_message(self, sender, payload) -> None:
        pass

    # APPLICANT PARENT DOMAIN
    def wep(self, sn_e):
        # peer.peer_info.sn_e += 1
        # peer.peer_info.sn_r += 1
        pass

    ############
    # Checkers #
    ############
    def exist_event(self, event_id) -> bool:
        return event_id in self.events

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
        for v in peer.deposits.values():
            deposited += v

        return peer.info.balance - deposited

    @staticmethod
    def get_free_resources(peer: HIDRAPeer) -> int:
        # Count total reserved
        reserved = 0
        for v in peer.reservations.values():
            reserved += v

        return peer.info.r_max - reserved

    @staticmethod
    def required_replies_for_quorum() -> int:
        """
        Number of required replies to get a quorum certificate on a message
        """

        return 2 * SimulationSettings.faulty_peers + 1

    @staticmethod
    def required_replies_for_proof() -> int:
        """
        Number of required replies to prove a message
        """

        return SimulationSettings.faulty_peers + 1

    def get_peer_from_id(self, peer_id) -> Peer:
        for domain in self.domains.values():
            for peer in domain:
                if get_peer_id(peer) == peer_id:
                    return peer

    @staticmethod
    def has_required_replies(replies: dict, required_replies: int) -> bool:
        # Count equal replies
        counter = Counter(replies.values()).most_common(1)

        if counter and counter[0][1] == required_replies:
            return True
        else:
            return False
