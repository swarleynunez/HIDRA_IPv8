import asyncio
import json
import random
import time
from asyncio import get_event_loop
from binascii import unhexlify
from collections import Counter

from bami.settings import SimulationSettings
from hidra.payload import REQUEST_RESOURCE_INFO, RESOURCE_INFO, RequestResourceInfoPayload, ResourceInfoPayload, \
    LockingSendPayload, LOCKING_SEND, LOCKING_ECHO, LOCKING_READY, LockingEchoPayload, LockingCreditPayload, \
    RESERVATION_ECHO, RESERVATION_READY, RESERVATION_CREDIT, LOCKING_CREDIT, RESERVATION_CANCEL, LockingReadyPayload, \
    ReservationEchoPayload, ReservationCancelPayload, ReservationCreditPayload, ReservationReadyPayload, \
    EVENT_CONFIRM, EVENT_CANCEL, EventConfirmPayload, EventCancelPayload
from hidra.settings import HIDRASettings
from hidra.types import HIDRAPeerInfo, HIDRAEventInfo, HIDRAWorkload, HIDRAPeer, HIDRAEvent, IPv8PendingMessage
from hidra.utils import get_peer_id, get_object_id
from pyipv8.ipv8.community import Community
from pyipv8.ipv8.lazy_community import lazy_wrapper
from pyipv8.ipv8.peer import Peer


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

        # Other
        self.reservation_lock = asyncio.Lock()

        # Message handlers
        self.add_message_handler(REQUEST_RESOURCE_INFO, self.on_request_resource_info)
        self.add_message_handler(RESOURCE_INFO, self.on_resource_info)
        self.add_message_handler(LOCKING_SEND, self.on_locking_send)
        self.add_message_handler(LOCKING_ECHO, self.on_locking_echo)
        self.add_message_handler(LOCKING_READY, self.on_locking_ready)
        self.add_message_handler(LOCKING_CREDIT, self.on_locking_credit)
        self.add_message_handler(RESERVATION_ECHO, self.on_reservation_echo)
        self.add_message_handler(RESERVATION_CANCEL, self.on_reservation_cancel)
        self.add_message_handler(RESERVATION_READY, self.on_reservation_ready)
        self.add_message_handler(RESERVATION_CREDIT, self.on_reservation_credit)
        self.add_message_handler(EVENT_CONFIRM, self.on_event_confirm)
        self.add_message_handler(EVENT_CANCEL, self.on_event_cancel)

    ########
    # Peer #
    ########
    async def start(self):
        # Register asyncio tasks with the community
        # This ensures that tasks end when the community is unloaded
        self.register_task("initialize_peer", self.initialize_peer)
        await self.wait_for_tasks()

        # To avoid missing IPv8 messages due to an early simulation end
        self.register_task("process_pending_messages", self.process_pending_messages, interval=1)

        # Sending offloading events
        delay = 0
        for _ in range(HIDRASettings.events_per_peer):
            # Next event sequence number
            sn_e = self.next_sn_e
            self.next_sn_e += 1

            # Solver Selection Phase (SSP)
            self.register_task("ssp_" + str(sn_e), self.ssp, sn_e, delay=delay)

            # Workload Reservation Phase (WRP)
            self.register_task("wrp_" + str(sn_e), self.wrp, sn_e, delay=HIDRASettings.ssp_timeout + delay)

            delay += HIDRASettings.event_sending_delay / 1000

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

    async def process_pending_messages(self):
        for k, v in list(self.messages.items()):
            # Update storage and cache
            self.messages.pop(k)

            if v.payload.msg_id == LOCKING_SEND:
                self.process_locking_send_message(v.sender, v.payload)
            elif v.payload.msg_id == LOCKING_ECHO:
                self.process_locking_echo_message(v.sender, v.payload)
            elif v.payload.msg_id == LOCKING_READY:
                self.process_locking_ready_message(v.sender, v.payload)
            elif v.payload.msg_id == LOCKING_CREDIT:
                await self.process_locking_credit_message(v.sender, v.payload)
            elif v.payload.msg_id == RESERVATION_ECHO:
                self.process_reservation_echo_message(v.sender, v.payload)
            elif v.payload.msg_id == RESERVATION_READY:
                self.process_reservation_ready_message(v.sender, v.payload)
            elif v.payload.msg_id == RESERVATION_CREDIT:
                self.process_reservation_credit_message(v.sender, v.payload)

    #############
    # Callbacks #
    #############
    @lazy_wrapper(RequestResourceInfoPayload)
    async def on_request_resource_info(self, sender, payload) -> None:
        await self.process_pending_messages()
        self.process_request_resource_info_message(sender, payload)

    @lazy_wrapper(ResourceInfoPayload)
    async def on_resource_info(self, sender, payload) -> None:
        await self.process_pending_messages()
        self.process_resource_info_message(sender, payload)

    @lazy_wrapper(LockingSendPayload)
    async def on_locking_send(self, sender, payload) -> None:
        await self.process_pending_messages()
        self.process_locking_send_message(sender, payload)

    @lazy_wrapper(LockingEchoPayload)
    async def on_locking_echo(self, sender, payload) -> None:
        await self.process_pending_messages()
        self.process_locking_echo_message(sender, payload)

    @lazy_wrapper(LockingReadyPayload)
    async def on_locking_ready(self, sender, payload) -> None:
        await self.process_pending_messages()
        self.process_locking_ready_message(sender, payload)

    @lazy_wrapper(LockingCreditPayload)
    async def on_locking_credit(self, sender, payload) -> None:
        await self.process_pending_messages()
        await self.process_locking_credit_message(sender, payload)

    @lazy_wrapper(ReservationEchoPayload)
    async def on_reservation_echo(self, sender, payload) -> None:
        await self.process_pending_messages()
        self.process_reservation_echo_message(sender, payload)

    @lazy_wrapper(ReservationCancelPayload)
    async def on_reservation_cancel(self, sender, payload) -> None:
        await self.process_pending_messages()
        self.process_reservation_cancel_message(sender, payload)

    @lazy_wrapper(ReservationReadyPayload)
    async def on_reservation_ready(self, sender, payload) -> None:
        await self.process_pending_messages()
        self.process_reservation_ready_message(sender, payload)

    @lazy_wrapper(ReservationCreditPayload)
    async def on_reservation_credit(self, sender, payload) -> None:
        await self.process_pending_messages()
        self.process_reservation_credit_message(sender, payload)

    @lazy_wrapper(EventConfirmPayload)
    async def on_event_confirm(self, sender, payload) -> None:
        await self.process_pending_messages()
        self.process_event_confirm_message(sender, payload)

    @lazy_wrapper(EventCancelPayload)
    async def on_event_cancel(self, sender, payload) -> None:
        await self.process_pending_messages()
        self.process_event_cancel_message(sender, payload)

    #########
    # Tasks #
    #########
    # APPLICANT
    def ssp(self, sn_e) -> None:
        event_id = get_object_id(self.my_peer_id, sn_e)

        # Select a domain
        to_domain_id = self.select_domain()

        # Payload data
        workload = HIDRAWorkload("nginx", random.randint(1, 1024), 8888)
        event_info = HIDRAEventInfo(self.parent_domain_id,
                                    to_domain_id,
                                    "",
                                    workload,
                                    random.randint(1, 1800),
                                    1,
                                    int(time.time()) + 180)

        # Update storage
        self.events[event_id] = HIDRAEvent()
        self.events[event_id].info = event_info

        # Debug
        print("[" + format(get_event_loop().time(), ".3f") +
              "][Domain:" + str(self.parent_domain_id) +
              "][Peer:" + self.my_peer_id +
              "] Sending RequestResourceInfo ---> " +
              "Domain:" + str(to_domain_id) +
              ", Applicant:" + self.my_peer_id +
              ", Event:" + str(sn_e))

        # Applicant sends RequestResourceInfo messages to the selected domain
        for peer in self.domains[to_domain_id]:
            self.ez_send(peer, RequestResourceInfoPayload(sn_e, event_info))

    # SELECTED DOMAIN
    def process_request_resource_info_message(self, sender, payload) -> None:
        # Payload data
        # TODO. Check event info to make a decision
        available = random.choice([True, False])
        domain_info = {}
        for peer in self.domains[self.parent_domain_id]:
            peer_id = get_peer_id(peer)
            domain_info[peer_id] = self.peers[peer_id].info

        # Debug
        print("[" + format(get_event_loop().time(), ".3f") +
              "][Domain:" + str(self.parent_domain_id) +
              "][Peer:" + self.my_peer_id +
              "] Sending ResourceInfo ---> " +
              "Applicant:" + get_peer_id(sender) +
              ", Event:" + str(payload.sn_e) +
              ", Available:" + ("T" if available else "F"))

        # Selected domain sends ResourceInfo messages to the Applicant
        self.ez_send(sender, ResourceInfoPayload(payload.sn_e, available, domain_info))

    # APPLICANT
    def process_resource_info_message(self, sender, payload) -> None:
        sender_id = get_peer_id(sender)

        # Is the sender peer available to manage the event?
        if payload.available and sender_id != self.my_peer_id:
            self.events[get_object_id(self.my_peer_id, payload.sn_e)].available_peers.append(sender_id)

        # Deliver resource replies
        for k, v in payload.resource_replies.items():
            # Update storage
            self.peers[k].resource_replies[sender_id] = v

            # TODO: Select peer info from resource replies
            # peer_info = self.select_peer_info(self.peers[k].resource_replies)
            # if peer_info:
            #     # Update storage
            #     self.peers[k].info = peer_info

    # APPLICANT
    def wrp(self, sn_e):
        event = self.events[get_object_id(self.my_peer_id, sn_e)]

        # Requirements
        if len(event.available_peers) == 0:
            print(" - INFO ---> Domain:" + str(event.info.to_domain_id) +
                  " not available for Applicant:" + self.my_peer_id + " Event:" + str(sn_e))
            self.replace_task("ssp_" + str(sn_e), self.ssp, sn_e)
            self.replace_task("wrp_" + str(sn_e), self.wrp, sn_e, delay=HIDRASettings.ssp_timeout)
            return

        # Payload data
        # TODO. Check event resource_replies to make a decision
        solver_id = random.choice(event.available_peers)

        # Update storage
        event.info.solver_id = solver_id

        # Debug
        print("[" + format(get_event_loop().time(), ".3f") +
              "][Domain:" + str(self.parent_domain_id) +
              "][Peer:" + self.my_peer_id +
              "] Sending LockingSend ---> " +
              "Domain:" + str(self.parent_domain_id) +
              ", Applicant:" + self.my_peer_id +
              ", Event:" + str(sn_e) +
              ", Solver:" + str(solver_id))

        # Applicant sends LockingSend messages to its parent domain
        for peer in self.domains[self.parent_domain_id]:
            self.ez_send(peer, LockingSendPayload(sn_e, event.info))

    # APPLICANT PARENT DOMAIN
    def process_locking_send_message(self, sender, payload) -> None:
        sender_id = get_peer_id(sender)
        event_id = get_object_id(sender_id, payload.sn_e)
        peer = self.peers[sender_id]
        deposit = payload.event_info.t_exec_value * payload.event_info.p_ratio_value

        # Requirements
        if self.exist_event(event_id) and self.events[event_id].locking_echo_ok:
            # Dismiss the message...
            return
        if payload.sn_e > peer.info.sn_e:
            self.add_pending_message(sender, payload)
            return
        if deposit > self.get_available_balance(peer):
            print(" - INFO ---> Applicant:" + sender_id +
                  " does not have enough balance for Event:" + str(payload.sn_e))
            self.add_pending_message(sender, payload)
            return

        # Update storage
        peer.info.sn_e += 1
        peer.deposits[payload.sn_e] = deposit
        if not self.exist_event(event_id):
            self.events[event_id] = HIDRAEvent()
            self.events[event_id].info = payload.event_info

        # Debug
        print("[" + format(get_event_loop().time(), ".3f") +
              "][Domain:" + str(self.parent_domain_id) +
              "][Peer:" + self.my_peer_id +
              "] Sending LockingEcho ---> " +
              "Domain:" + str(self.parent_domain_id) +
              ", Applicant:" + sender_id +
              ", Event:" + str(payload.sn_e) +
              ", Deposit:" + str(deposit))

        # Applicant parent domain sends LockingEcho messages to itself
        self.events[event_id].locking_echo_ok = True
        for peer in self.domains[self.parent_domain_id]:
            self.ez_send(peer, LockingEchoPayload(sender_id, payload.sn_e, payload.event_info))

    # APPLICANT PARENT DOMAIN
    def process_locking_echo_message(self, sender, payload) -> None:
        event_id = get_object_id(payload.applicant_id, payload.sn_e)
        sender_id = get_peer_id(sender)

        # Requirements
        if not self.exist_event(event_id):
            self.add_pending_message(sender, payload)
            return
        event = self.events[event_id]
        if sender_id in event.locking_echos or event.locking_ready_ok:
            # Dismiss the message...
            return

        # Format event data
        data = payload.applicant_id + ":" + str(payload.sn_e) + ":" + str(payload.event_info)

        # Update storage
        event.locking_echos[sender_id] = data

        # Required equal echos for an event/locking?
        if self.has_required_replies(event.locking_echos, self.required_replies_for_quorum()):
            # Debug
            print("[" + format(get_event_loop().time(), ".3f") +
                  "][Domain:" + str(self.parent_domain_id) +
                  "][Peer:" + self.my_peer_id +
                  "] Sending LockingReady ---> " +
                  "Domain:" + str(self.parent_domain_id) +
                  ", Applicant:" + payload.applicant_id +
                  ", Event:" + str(payload.sn_e))

            # Applicant parent domain sends LockingReady messages to itself
            event.locking_ready_ok = True
            for peer in self.domains[self.parent_domain_id]:
                self.ez_send(peer, LockingReadyPayload(payload.applicant_id, payload.sn_e, payload.event_info))

    # APPLICANT PARENT DOMAIN
    def process_locking_ready_message(self, sender, payload) -> None:
        event_id = get_object_id(payload.applicant_id, payload.sn_e)
        sender_id = get_peer_id(sender)

        # Requirements
        if not self.exist_event(event_id):
            self.add_pending_message(sender, payload)
            return
        event = self.events[event_id]
        if sender_id in event.locking_readys or event.locking_credit_ok:
            # Dismiss the message...
            return

        # Format event data
        data = payload.applicant_id + ":" + str(payload.sn_e) + ":" + str(payload.event_info)

        # Update storage
        event.locking_readys[sender_id] = data

        # TODO: amplification step

        # Required equal readys for event/locking delivery?
        if self.has_required_replies(event.locking_readys, self.required_replies_for_quorum()):
            # Workload Execution Phase (WEP)
            self.register_task("wep_" + get_object_id(payload.applicant_id, payload.sn_e),
                               self.wep, payload.applicant_id, payload.sn_e, delay=HIDRASettings.wrp_timeout)

            # Debug
            print("[" + format(get_event_loop().time(), ".3f") +
                  "][Domain:" + str(self.parent_domain_id) +
                  "][Peer:" + self.my_peer_id +
                  "] Sending LockingCredit ---> " +
                  "Domain:" + str(payload.event_info.to_domain_id) +
                  ", Applicant:" + payload.applicant_id +
                  ", Event:" + str(payload.sn_e))

            # Applicant parent domain sends LockingCredit messages to Solver parent domain
            event.locking_credit_ok = True
            for peer in self.domains[payload.event_info.to_domain_id]:
                self.ez_send(peer, LockingCreditPayload(payload.applicant_id, payload.sn_e, payload.event_info))

    # SOLVER PARENT DOMAIN
    async def process_locking_credit_message(self, sender, payload) -> None:
        event_id = get_object_id(payload.applicant_id, payload.sn_e)
        sender_id = get_peer_id(sender)
        applicant = self.peers[payload.applicant_id]

        # Requirements
        if not self.exist_event(event_id):
            self.events[event_id] = HIDRAEvent()
        event = self.events[event_id]
        if sender_id in event.locking_credits or event.reservation_echo_ok:
            # Dismiss the message...
            return
        if payload.sn_e > applicant.next_sn_e:
            self.add_pending_message(sender, payload)
            return

        # Format event data
        data = payload.applicant_id + ":" + str(payload.sn_e) + ":" + str(payload.event_info)

        # Update storage
        event.locking_credits[sender_id] = data

        # Required equal credits for an event/locking?
        if self.has_required_replies(event.locking_credits, self.required_replies_for_proof()):
            solver_id = payload.event_info.solver_id

            # Update storage
            event.info = payload.event_info

            # Am I the event Solver?
            if self.my_peer_id == solver_id:
                solver = self.peers[solver_id]

                # Update storage
                applicant.next_sn_e += 1
                if self.parent_domain_id != payload.event_info.from_domain_id:  # Inter-domain events
                    applicant.info.sn_e += 1

                # Next reservation sequence number (via mutex)
                async with self.reservation_lock:
                    sn_r = solver.info.sn_r
                    solver.info.sn_r += 1

                # Debug
                print("[" + format(get_event_loop().time(), ".3f") +
                      "][Domain:" + str(self.parent_domain_id) +
                      "][Peer:" + self.my_peer_id +
                      "] Sending ReservationEcho ---> " +
                      "Domain:" + str(self.parent_domain_id) +
                      ", Applicant:" + payload.applicant_id +
                      ", Event:" + str(payload.sn_e) +
                      ", Solver:" + solver_id +
                      ", Reservation:" + str(sn_r) +
                      ", Vote:T")

                # Solver parent domain sends ReservationEcho messages to itself
                event.reservation_echo_ok = True
                for peer in self.domains[self.parent_domain_id]:
                    print("ENVÍO", self.my_peer_id, event.info.solver_id, payload.applicant_id, str(payload.sn_e))
                    self.ez_send(peer, ReservationEchoPayload(payload.applicant_id, payload.sn_e, sn_r, True, sn_r))
            else:
                if solver_id in event.reservation_echos:
                    solver = self.peers[solver_id]

                    # Update storage
                    applicant.next_sn_e += 1
                    if self.parent_domain_id != payload.event_info.from_domain_id:  # Inter-domain events
                        applicant.info.sn_e += 1

                    # Payload data
                    sn_r = int(event.reservation_echos[solver_id].split(":")[2])
                    vote = sn_r == solver.next_sn_r

                    # Debug
                    print("[" + format(get_event_loop().time(), ".3f") +
                          "][Domain:" + str(self.parent_domain_id) +
                          "][Peer:" + self.my_peer_id +
                          "] Sending ReservationEcho ---> " +
                          "Domain:" + str(self.parent_domain_id) +
                          ", Applicant:" + payload.applicant_id +
                          ", Event:" + str(payload.sn_e) +
                          ", Solver:" + solver_id +
                          ", Reservation:" + str(sn_r) +
                          ", Vote:" + ("T" if vote else "F"))

                    # Solver parent domain sends ReservationEcho messages to itself
                    event.reservation_echo_ok = True
                    for peer in self.domains[self.parent_domain_id]:
                        self.ez_send(peer, ReservationEchoPayload(payload.applicant_id, payload.sn_e, sn_r, vote,
                                                                  solver.next_sn_r))
                else:
                    del event.locking_credits[sender_id]
                    self.add_pending_message(sender, payload)

    # SOLVER PARENT DOMAIN
    def process_reservation_echo_message(self, sender, payload) -> None:
        event_id = get_object_id(payload.applicant_id, payload.sn_e)
        sender_id = get_peer_id(sender)

        # Requirements
        if not self.exist_event(event_id) or not self.events[event_id].info:
            self.add_pending_message(sender, payload)
            return
        event = self.events[event_id]
        if sender_id in event.reservation_echos or event.reservation_ready_ok:
            # Dismiss the message...
            return
        solver_id = event.info.solver_id
        if len(event.reservation_echos) == 0 and sender_id != solver_id:
            self.add_pending_message(sender, payload)
            return

        # Format message data
        data = payload.applicant_id + ":" + str(payload.sn_e) + ":" + str(payload.sn_r) + ":" + str(payload.vote)

        # Update storage
        event.reservation_echos[sender_id] = data

        # Debug
        data2 = payload.applicant_id + ":" + str(payload.sn_e) + ":" + str(payload.sn_r) + ":" + str(
            payload.vote) + ":" + str(payload.number)
        event.reservation_echos2[sender_id] = data2

        print("LLEGO", self.my_peer_id, sender_id, event.info.solver_id, payload.applicant_id, str(payload.sn_e))

        # Required echos for a reservation?
        if self.has_required_replies(event.reservation_echos, self.required_replies_for_quorum()):
            solver = self.peers[solver_id]

            print("ENTRO", self.my_peer_id, sender_id, event.info.solver_id, payload.applicant_id, str(payload.sn_e),
                  event.reservation_echos2)

            # Waiting for the next reservation
            if payload.sn_r > solver.next_sn_r:
                del event.reservation_echos[sender_id]
                self.add_pending_message(sender, payload)
                return

            # Update storage
            solver.next_sn_r += 1
            if self.my_peer_id != solver_id:
                solver.info.sn_r += 1

            # Correct reservation sequence number?
            data = self.get_most_common_reply(event.reservation_echos)
            if not data.split(":")[3]:
                # Debug
                print("[" + format(get_event_loop().time(), ".3f") +
                      "][Domain:" + str(self.parent_domain_id) +
                      "][Peer:" + self.my_peer_id +
                      "] Sending ReservationCancel ---> " +
                      "Domain:" + str(event.info.from_domain_id) +
                      ", Applicant:" + payload.applicant_id +
                      ", Event:" + str(payload.sn_e))

                # Solver parent domain sends ReservationCancel messages to Applicant parent domain
                event.reservation_ready_ok = True
                for peer in self.domains[event.info.from_domain_id]:
                    self.ez_send(peer, ReservationCancelPayload(payload.applicant_id, payload.sn_e))
                return

            # Enough resources?
            resource_limit = event.info.workload.resource_limit
            if resource_limit > self.get_free_resources(solver):
                print(" - INFO ---> Solver:" + solver_id + " does not have enough resources for Applicant:" +
                      payload.applicant_id + " Event:" + str(payload.sn_e))

                # Debug
                print("[" + format(get_event_loop().time(), ".3f") +
                      "][Domain:" + str(self.parent_domain_id) +
                      "][Peer:" + self.my_peer_id +
                      "] Sending ReservationCancel ---> " +
                      "Domain:" + str(event.info.from_domain_id) +
                      ", Applicant:" + payload.applicant_id +
                      ", Event:" + str(payload.sn_e))

                # Solver parent domain sends ReservationCancel messages to Applicant parent domain
                event.reservation_ready_ok = True
                for peer in self.domains[event.info.from_domain_id]:
                    self.ez_send(peer, ReservationCancelPayload(payload.applicant_id, payload.sn_e))
                return

            # Update storage
            solver.reservations[payload.sn_r] = resource_limit

            # Debug
            print("[" + format(get_event_loop().time(), ".3f") +
                  "][Domain:" + str(self.parent_domain_id) +
                  "][Peer:" + self.my_peer_id +
                  "] Sending ReservationReady ---> " +
                  "Domain:" + str(self.parent_domain_id) +
                  ", Applicant:" + payload.applicant_id +
                  ", Event:" + str(payload.sn_e))

            # Solver parent domain sends ReservationReady messages to itself
            event.reservation_ready_ok = True
            for peer in self.domains[self.parent_domain_id]:
                self.ez_send(peer, ReservationReadyPayload(payload.applicant_id, payload.sn_e, payload.sn_r))
        else:
            print("ECHOES", event.info.solver_id, payload.applicant_id, str(payload.sn_e), event.reservation_echos2)

    # APPLICANT PARENT DOMAIN
    def process_reservation_cancel_message(self, sender, payload) -> None:
        sender_id = get_peer_id(sender)
        event = self.events[get_object_id(payload.applicant_id, payload.sn_e)]

        # Requirements
        if sender_id in event.reservation_cancels or event.reservation_cancel_ok:
            # Dismiss the message...
            return

        # Format reservation data
        data = payload.applicant_id + ":" + str(payload.sn_e)

        # Update storage
        event.reservation_cancels[sender_id] = data

        # Required equal cancels for a reservation?
        if self.has_required_replies(event.reservation_cancels, self.required_replies_for_proof()):
            # Update storage
            event.reservation_cancel_ok = True
            del self.peers[payload.applicant_id].deposits[payload.sn_e]
            if self.parent_domain_id != event.info.to_domain_id:  # Inter-domain events
                self.peers[event.info.solver_id].info.sn_r += 1

    # SOLVER PARENT DOMAIN
    def process_reservation_ready_message(self, sender, payload) -> None:
        event_id = get_object_id(payload.applicant_id, payload.sn_e)
        sender_id = get_peer_id(sender)

        # Requirements
        if not self.exist_event(event_id) or not self.events[event_id].info:
            self.add_pending_message(sender, payload)
            return
        event = self.events[event_id]
        if sender_id in event.reservation_readys or event.reservation_credit_ok:
            # Dismiss the message...
            return

        # Format reservation data
        data = payload.applicant_id + ":" + str(payload.sn_e) + ":" + str(payload.sn_r)

        # Update storage
        event.reservation_readys[sender_id] = data

        # TODO: amplification step

        # Required equal readys for reservation delivery?
        if self.has_required_replies(event.reservation_readys, self.required_replies_for_quorum()):
            # TODO: execute workload

            # Debug
            print("[" + format(get_event_loop().time(), ".3f") +
                  "][Domain:" + str(self.parent_domain_id) +
                  "][Peer:" + self.my_peer_id +
                  "] Sending ReservationCredit ---> " +
                  "Domain:" + str(event.info.from_domain_id) +
                  ", Applicant:" + payload.applicant_id +
                  ", Event:" + str(payload.sn_e))

            # Solver parent domain sends ReservationCredit messages to Applicant parent domain
            event.reservation_credit_ok = True
            for peer in self.domains[event.info.from_domain_id]:
                self.ez_send(peer, ReservationCreditPayload(payload.applicant_id, payload.sn_e, payload.sn_r))

    # APPLICANT PARENT DOMAIN
    def process_reservation_credit_message(self, sender, payload) -> None:
        event_id = get_object_id(payload.applicant_id, payload.sn_e)
        sender_id = get_peer_id(sender)

        # Requirements
        if not self.exist_event(event_id) or not self.events[event_id].info:
            self.add_pending_message(sender, payload)
            return
        event = self.events[event_id]
        if sender_id in event.reservation_credits or event.confirm_ok:
            # Dismiss the message...
            return

        # Format reservation data
        data = payload.applicant_id + ":" + str(payload.sn_e) + ":" + str(payload.sn_r)

        # Update storage
        event.reservation_credits[sender_id] = data

        # Required equal credits for a reservation?
        if self.has_required_replies(event.reservation_credits, self.required_replies_for_proof()):
            # Update storage
            if self.parent_domain_id != event.info.to_domain_id:  # Inter-domain events
                self.peers[event.info.solver_id].info.sn_r += 1

            # Debug
            print("[" + format(get_event_loop().time(), ".3f") +
                  "][Domain:" + str(self.parent_domain_id) +
                  "][Peer:" + self.my_peer_id +
                  "] Sending EventConfirm ---> " +
                  "Domain:" + str(self.parent_domain_id) +
                  ", Applicant:" + payload.applicant_id +
                  ", Event:" + str(payload.sn_e))

            # Applicant parent domain sends EventConfirm messages to itself
            event.confirm_ok = True
            for peer in self.domains[self.parent_domain_id]:
                self.ez_send(peer, EventConfirmPayload(payload.applicant_id, payload.sn_e))

    # APPLICANT PARENT DOMAIN
    def process_event_confirm_message(self, sender, payload) -> None:
        sender_id = get_peer_id(sender)
        event = self.events[get_object_id(payload.applicant_id, payload.sn_e)]

        # Requirements
        if sender_id in event.confirms:
            # Dismiss the message...
            return

        # Format event confirmation data
        data = payload.applicant_id + ":" + str(payload.sn_e)

        # Update storage
        event.confirms[sender_id] = data

        # ...

    # APPLICANT PARENT DOMAIN
    def wep(self, applicant_id, sn_e):
        pass

    # SOLVER PARENT DOMAIN
    def process_event_cancel_message(self, sender, payload) -> None:
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

    @staticmethod
    def get_most_common_reply(replies: dict) -> str:
        # Count equal replies
        counter = Counter(replies.values()).most_common(1)

        if counter:
            return counter[0][0]
        else:
            return ""
