import asyncio
import json
import os
import random
import time
from asyncio import get_event_loop
from collections import Counter

from bami.settings import SimulationSettings
from deployment.settings import DeploymentSettings
from hidra.payload import REQUEST_RESOURCE_INFO, RESOURCE_INFO, RequestResourceInfoPayload, ResourceInfoPayload, \
    LockingSendPayload, LOCKING_SEND, LOCKING_ECHO, LOCKING_READY, LockingEchoPayload, LockingCreditPayload, \
    RESERVATION_ECHO, RESERVATION_READY, RESERVATION_CREDIT, LOCKING_CREDIT, RESERVATION_CANCEL, LockingReadyPayload, \
    ReservationEchoPayload, ReservationCancelPayload, ReservationCreditPayload, ReservationReadyPayload, \
    EVENT_CONFIRM, EVENT_CANCEL, EventConfirmPayload, EventCancelPayload, MonitoringRequestPayload, \
    MonitoringResponsePayload, MONITORING_REQUEST, MONITORING_RESPONSE, MonitoringResultPayload, MonitoringSendPayload, \
    MonitoringEchoPayload, MonitoringCreditPayload, MONITORING_RESULT, MONITORING_SEND, MONITORING_ECHO, \
    MONITORING_CREDIT
from hidra.settings import HIDRASettings
from hidra.types import HIDRAPeerInfo, HIDRAEventInfo, HIDRAWorkload, HIDRAPeer, HIDRAEvent, IPv8PendingMessage
from hidra.utils import get_peer_id, get_object_id, sign_data, verify_sign, hash_dict
from pyipv8.ipv8.community import Community
from pyipv8.ipv8.lazy_community import lazy_wrapper
from pyipv8.ipv8.peer import Peer

# Debug
APPLICANT = None

# Experiments
FAULTY_PEER = "ANY"


class HIDRACommunity(Community):
    """
    HIDRA community
    """

    community_id = os.urandom(20)

    def __init__(self, my_peer, endpoint, network) -> None:
        super().__init__(my_peer, endpoint, network)
        self.settings = HIDRASettings()

        # Domains
        self.domains = {}
        self.parent_domain_id = None

        # Peers
        self.peers = {}
        self.my_peer_id = get_peer_id(self.my_peer)

        # Debug
        global APPLICANT
        if not APPLICANT:
            APPLICANT = self.my_peer_id

        # Events
        self.events = {}
        self.pending_events = []  # Events waiting for balance
        self.reservations_received = []  # To avoid repeated reservations

        # IPv8 Messages
        self.messages = {}

        # Counters
        self.next_sn_d = 0
        self.next_sn_e = 0
        self.next_sn_r = 0
        self.next_sn_m = 0

        # Other
        self.sn_m_lock = asyncio.Lock()
        self.sn_r_lock_1 = asyncio.Lock()
        self.sn_r_lock_2 = asyncio.Lock()

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
        self.add_message_handler(MONITORING_REQUEST, self.on_monitoring_request)
        self.add_message_handler(MONITORING_RESPONSE, self.on_monitoring_response)
        self.add_message_handler(MONITORING_RESULT, self.on_monitoring_result)
        self.add_message_handler(MONITORING_SEND, self.on_monitoring_send)
        self.add_message_handler(MONITORING_ECHO, self.on_monitoring_echo)
        self.add_message_handler(MONITORING_CREDIT, self.on_monitoring_credit)

    ########
    # Peer #
    ########
    def start(self) -> None:
        # To avoid missing IPv8 messages due to an early simulation end
        self.register_task("process_pending_messages", self.process_pending_messages, interval=1)

        # Experiments
        self.set_faulty_peer(self.my_peer_id)
        if self.is_faulty_peer(self.my_peer_id):
            # Dismiss the message...
            return

        # Debug
        if self.my_peer_id == APPLICANT:
            # Sending offloading events
            delay = 0
            for _ in range(self.settings.events_per_peer):
                # Next event sequence number
                sn_e = self.next_sn_e
                self.next_sn_e += 1

                print("Starting at %d" % time.time_ns())

                # Solver Selection Phase (SSP)
                self.register_task("ssp_" + str(sn_e), self.ssp, sn_e, delay=delay)

                # Workload Reservation Phase (WRP)
                self.register_task("wrp_" + str(sn_e), self.wrp, sn_e, delay=self.settings.ssp_timeout + delay)

                delay += self.settings.event_sending_delay / 1000

    def initialize_peer(self) -> None:
        # Initialize system domains deterministically
        self.set_domains()

        # Initialize peers information per domain
        balance = self.settings.initial_balance
        r_max = self.settings.max_resources
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
        if self.settings.deployment_mode:
            n = DeploymentSettings.peers_per_domain
        else:
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
        await self.process_locking_send_message(sender, payload)

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
        await self.process_reservation_echo_message(sender, payload)

    @lazy_wrapper(ReservationCancelPayload)
    async def on_reservation_cancel(self, sender, payload) -> None:
        await self.process_pending_messages()
        self.process_reservation_cancel_message(sender, payload)

    @lazy_wrapper(ReservationReadyPayload)
    async def on_reservation_ready(self, sender, payload) -> None:
        await self.process_pending_messages()
        await self.process_reservation_ready_message(sender, payload)

    @lazy_wrapper(ReservationCreditPayload)
    async def on_reservation_credit(self, sender, payload) -> None:
        await self.process_pending_messages()
        await self.process_reservation_credit_message(sender, payload)

    @lazy_wrapper(EventConfirmPayload)
    async def on_event_confirm(self, sender, payload) -> None:
        await self.process_pending_messages()
        await self.process_event_confirm_message(sender, payload)

    @lazy_wrapper(EventCancelPayload)
    async def on_event_cancel(self, sender, payload) -> None:
        await self.process_pending_messages()
        await self.process_event_cancel_message(sender, payload)

    @lazy_wrapper(MonitoringRequestPayload)
    async def on_monitoring_request(self, sender, payload) -> None:
        await self.process_pending_messages()
        self.process_monitoring_request_message(sender, payload)

    @lazy_wrapper(MonitoringResponsePayload)
    async def on_monitoring_response(self, sender, payload) -> None:
        await self.process_pending_messages()
        self.process_monitoring_response_message(sender, payload)

    @lazy_wrapper(MonitoringResultPayload)
    async def on_monitoring_result(self, sender, payload) -> None:
        await self.process_pending_messages()
        self.process_monitoring_result_message(sender, payload)

    @lazy_wrapper(MonitoringSendPayload)
    async def on_monitoring_send(self, sender, payload) -> None:
        await self.process_pending_messages()
        self.process_monitoring_send_message(sender, payload)

    @lazy_wrapper(MonitoringEchoPayload)
    async def on_monitoring_echo(self, sender, payload) -> None:
        await self.process_pending_messages()
        await self.process_monitoring_echo_message(sender, payload)

    @lazy_wrapper(MonitoringCreditPayload)
    async def on_monitoring_credit(self, sender, payload) -> None:
        await self.process_pending_messages()
        await self.process_monitoring_credit_message(sender, payload)

    #########
    # Tasks #
    #########
    # APPLICANT
    def ssp(self, sn_e) -> None:
        event_id = get_object_id(self.my_peer_id, sn_e)

        # Select a domain
        to_domain_id = self.select_domain()

        # Payload data
        workload = HIDRAWorkload("nginx", 1024, 80)
        event_info = HIDRAEventInfo(self.parent_domain_id, to_domain_id, "", workload,
                                    self.settings.t_exec_value, self.settings.p_ratio_value, 0)

        # Update storage
        self.events[event_id] = HIDRAEvent()
        self.events[event_id].info = event_info

        # Debug
        if self.settings.message_debug:
            print("[" + str(self.get_current_time()) +
                  "][Domain:" + str(self.parent_domain_id) +
                  "][Peer:" + self.my_peer_id +
                  "] Sending RequestResourceInfo ---> " +
                  "Domain:" + str(to_domain_id) +
                  ", Applicant:" + self.my_peer_id +
                  ", Event:" + str(sn_e))

        # Applicant sends RequestResourceInfo messages to the selected domain
        for peer in self.domains[to_domain_id]:
            self.ez_send(peer, RequestResourceInfoPayload(self.my_peer_id, sn_e))

    # SELECTED DOMAIN
    def process_request_resource_info_message(self, sender, payload) -> None:
        # Experiments
        if self.is_faulty_peer(self.my_peer_id):
            # Dismiss the message...
            return

        # Payload data
        # TODO: Check event info to make a decision
        available = random.choice([True, False])
        # resource_replies = {}
        # for peer in self.domains[self.parent_domain_id]:
        #     peer_id = get_peer_id(peer)
        #     resource_replies[peer_id] = self.peers[peer_id].info

        # Debug
        if self.settings.message_debug:
            print("[" + str(self.get_current_time()) +
                  "][Domain:" + str(self.parent_domain_id) +
                  "][Peer:" + self.my_peer_id +
                  "] Sending ResourceInfo ---> " +
                  "Domain:" + str(self.get_peer_domain(payload.applicant_id)) +
                  ", Applicant:" + payload.applicant_id +
                  ", Event:" + str(payload.sn_e) +
                  ", Available:" + ("T" if available else "F"))

        # Selected domain sends ResourceInfo messages to the Applicant
        self.ez_send(sender, ResourceInfoPayload(payload.applicant_id, payload.sn_e, available))

    # APPLICANT
    def process_resource_info_message(self, sender, payload) -> None:
        # Experiments
        if self.is_faulty_peer(self.my_peer_id):
            # Dismiss the message...
            return

        sender_id = get_peer_id(sender)

        # Is the sender peer available to manage the event?
        if payload.available and sender_id != self.my_peer_id:
            self.events[get_object_id(self.my_peer_id, payload.sn_e)].available_peers.append(sender_id)

        # Deliver resource replies
        # for k, v in payload.resource_replies.items():
        #     # Update storage
        #     self.peers[k].resource_replies[sender_id] = v

        # TODO: Select peer info from resource replies
        # peer_info = self.select_peer_info(self.peers[k].resource_replies)
        # if peer_info:
        #     # Update storage
        #     self.peers[k].info = peer_info

    # APPLICANT
    def wrp(self, sn_e) -> None:
        event = self.events[get_object_id(self.my_peer_id, sn_e)]

        # Requirements
        if len(event.available_peers) == 0:
            print("[" + str(self.get_current_time()) + "] INFO ---> Domain:" + str(event.info.to_domain_id) +
                  " not available for Applicant:" + self.my_peer_id + ", Event:" + str(sn_e) + " ---> Retrying...")
            self.replace_task("ssp_" + str(sn_e), self.ssp, sn_e)
            self.replace_task("wrp_" + str(sn_e), self.wrp, sn_e, delay=self.settings.ssp_timeout)
            return

        # TODO: Check event resource_replies to make a decision
        solver_id = random.choice(event.available_peers)

        # Update storage
        event.info.solver_id = solver_id
        event.info.ts_start = self.get_current_time() + self.settings.wrp_timeout

        # Debug
        if self.settings.message_debug:
            print("[" + str(self.get_current_time()) +
                  "][Domain:" + str(self.parent_domain_id) +
                  "][Peer:" + self.my_peer_id +
                  "] Sending LockingSend ---> " +
                  "Domain:" + str(self.parent_domain_id) +
                  ", Applicant:" + self.my_peer_id +
                  ", Event:" + str(sn_e) +
                  ", Solver:" + str(solver_id))

        # Applicant sends LockingSend messages to its parent domain
        for peer in self.domains[self.parent_domain_id]:
            self.ez_send(peer, LockingSendPayload(self.my_peer_id, sn_e, event.info))

    # APPLICANT PARENT DOMAIN
    async def process_locking_send_message(self, sender, payload) -> None:
        # Experiments
        if self.is_faulty_peer(self.my_peer_id):
            # Dismiss the message...
            return

        event_id = get_object_id(payload.applicant_id, payload.sn_e)
        applicant = self.peers[payload.applicant_id]
        deposit = payload.event_info.t_exec_value * payload.event_info.p_ratio_value

        # Requirements
        if self.exist_event(event_id) and self.events[event_id].locking_echo_ok:
            # Dismiss the message...
            return
        if payload.sn_e > applicant.info.sn_e:
            await self.add_pending_message(sender, payload)
            return
        elif payload.sn_e < applicant.info.sn_e:
            # Dismiss the message...
            return
        if deposit > self.get_available_balance(applicant):
            if not event_id in self.pending_events:
                self.pending_events.append(event_id)
                print("[" + str(self.get_current_time()) + "] INFO ---> Applicant:" + payload.applicant_id +
                      " does not have enough balance for Event:" + str(payload.sn_e) + " ---> Waiting...")
            await self.add_pending_message(sender, payload)
            return
        else:
            if event_id in self.pending_events:
                self.pending_events.remove(event_id)
                print("[" + str(self.get_current_time()) + "] INFO ---> Applicant:" + payload.applicant_id +
                      " has obtained balance for Event:" + str(payload.sn_e) + " ---> Retrying...")

        # Update storage
        applicant.info.sn_e += 1
        applicant.deposits[payload.sn_e] = deposit
        if not self.exist_event(event_id):
            self.events[event_id] = HIDRAEvent()

        # Debug
        if self.settings.message_debug:
            print("[" + str(self.get_current_time()) +
                  "][Domain:" + str(self.parent_domain_id) +
                  "][Peer:" + self.my_peer_id +
                  "] Sending LockingEcho ---> " +
                  "Domain:" + str(self.parent_domain_id) +
                  ", Applicant:" + payload.applicant_id +
                  ", Event:" + str(payload.sn_e) +
                  ", Deposit:" + str(deposit))

        # Applicant parent domain sends LockingEcho messages to itself
        self.events[event_id].locking_echo_ok = True
        for peer in self.domains[self.parent_domain_id]:
            self.ez_send(peer, LockingEchoPayload(payload.applicant_id, payload.sn_e, payload.event_info))

    # APPLICANT PARENT DOMAIN
    def process_locking_echo_message(self, sender, payload) -> None:
        # Experiments
        if self.is_faulty_peer(self.my_peer_id):
            # Dismiss the message...
            return

        event_id = get_object_id(payload.applicant_id, payload.sn_e)
        sender_id = get_peer_id(sender)

        # Requirements
        if not self.exist_event(event_id):
            self.events[event_id] = HIDRAEvent()
        event = self.events[event_id]
        if sender_id in event.locking_echos or event.locking_ready_ok:
            # Dismiss the message...
            return

        # Format data
        data = payload.applicant_id + ":" + str(payload.sn_e) + ":" + str(payload.event_info)

        # Update storage
        event.locking_echos[sender_id] = data

        # Required equal echos for an event/locking?
        if self.has_required_replies(event.locking_echos, self.required_replies_for_quorum()):
            # Update storage
            event.info = payload.event_info

            # Debug
            if self.settings.message_debug:
                print("[" + str(self.get_current_time()) +
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
        # Experiments
        if self.is_faulty_peer(self.my_peer_id):
            # Dismiss the message...
            return

        event_id = get_object_id(payload.applicant_id, payload.sn_e)
        sender_id = get_peer_id(sender)

        # Requirements
        if not self.exist_event(event_id):
            self.events[event_id] = HIDRAEvent()
        event = self.events[event_id]
        if sender_id in event.locking_readys or event.locking_credit_ok:
            # Dismiss the message...
            return

        # Format data
        data = payload.applicant_id + ":" + str(payload.sn_e) + ":" + str(payload.event_info)

        # Update storage
        event.locking_readys[sender_id] = data

        # Amplification step (equal readys?)
        if self.has_required_replies(event.locking_readys, self.required_replies_for_proof()) and \
                not event.locking_ready_ok:
            # Update storage
            event.info = payload.event_info

            # Debug
            if self.settings.message_debug:
                print("[" + str(self.get_current_time()) +
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

        # Required equal readys for event/locking delivery?
        if self.has_required_replies(event.locking_readys, self.required_replies_for_quorum()):
            # Debug
            if self.settings.message_debug:
                print("[" + str(self.get_current_time()) +
                      "][Domain:" + str(self.parent_domain_id) +
                      "][Peer:" + self.my_peer_id +
                      "] Sending LockingCredit ---> " +
                      "Domain:" + str(payload.event_info.to_domain_id) +
                      ", Applicant:" + payload.applicant_id +
                      ", Event:" + str(payload.sn_e))

            # Applicant parent domain sends LockingCredit messages to the Solver parent domain
            event.locking_credit_ok = True
            for peer in self.domains[payload.event_info.to_domain_id]:
                self.ez_send(peer, LockingCreditPayload(payload.applicant_id, payload.sn_e, payload.event_info))

    # SOLVER PARENT DOMAIN
    async def process_locking_credit_message(self, sender, payload) -> None:
        # Experiments
        if self.is_faulty_peer(self.my_peer_id):
            # Dismiss the message...
            return

        event_id = get_object_id(payload.applicant_id, payload.sn_e)
        sender_id = get_peer_id(sender)

        # Requirements
        if not self.exist_event(event_id):  # Inter
            self.events[event_id] = HIDRAEvent()
        event = self.events[event_id]
        if sender_id in event.locking_credits or event.reservation_echo_ok:
            # Dismiss the message...
            return

        # Format data
        data = payload.applicant_id + ":" + str(payload.sn_e) + ":" + str(payload.event_info)

        # Update storage
        event.locking_credits[sender_id] = data

        # Required equal credits for an event/locking?
        if self.has_required_replies(event.locking_credits, self.required_replies_for_proof()):
            # Update storage
            if not event.info:  # Inter
                event.info = payload.event_info

            # Am I the event Solver?
            solver_id = event.info.solver_id
            if self.my_peer_id == solver_id:
                # Next reservation sequence number (accessed via mutex)
                async with self.sn_r_lock_1:
                    sn_r = self.next_sn_r
                    self.next_sn_r += 1

                # Debug
                if self.settings.message_debug:
                    print("[" + str(self.get_current_time()) +
                          "][Domain:" + str(self.parent_domain_id) +
                          "][Peer:" + self.my_peer_id +
                          "] Sending ReservationEcho ---> " +
                          "Domain:" + str(self.parent_domain_id) +
                          ", Applicant:" + payload.applicant_id +
                          ", Event:" + str(payload.sn_e) +
                          ", Solver:" + solver_id +
                          ", Reservation:" + str(sn_r) +
                          ", Vote:T")

                # Experiments
                # sn_r = self.set_repeated_sn_r(sn_r, 1)

                # Solver parent domain sends ReservationEcho messages to itself
                event.reservation_echo_ok = True
                for peer in self.domains[self.parent_domain_id]:
                    self.ez_send(peer, ReservationEchoPayload(payload.applicant_id, payload.sn_e, sn_r, True))
            else:
                if solver_id in event.reservation_echos:
                    # Get reservation sequence number chosen by the Solver
                    sn_r = int(event.reservation_echos[solver_id].split(":")[2])

                    # Peers vote YES/NO on the reservation sequence number chosen by the Solver
                    async with self.sn_r_lock_2:
                        reservation_id = get_object_id(solver_id, sn_r)
                        if not reservation_id in self.reservations_received:
                            self.reservations_received.append(reservation_id)
                            vote = True
                        else:
                            vote = False

                    # Debug
                    if self.settings.message_debug:
                        print("[" + str(self.get_current_time()) +
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
                        self.ez_send(peer, ReservationEchoPayload(payload.applicant_id, payload.sn_e, sn_r, vote))
                else:
                    # Cleaning of stuck pending messages due to the amplification step
                    if not event.reservation_credit_ok:
                        # Waiting for previous messages
                        del event.locking_credits[sender_id]
                        await self.add_pending_message(sender, payload)

    # SOLVER PARENT DOMAIN
    async def process_reservation_echo_message(self, sender, payload) -> None:
        # Experiments
        if self.is_faulty_peer(self.my_peer_id):
            # Dismiss the message...
            return

        event_id = get_object_id(payload.applicant_id, payload.sn_e)
        sender_id = get_peer_id(sender)

        # Requirements
        if not self.exist_event(event_id) or not self.events[event_id].info:
            await self.add_pending_message(sender, payload)
            return
        event = self.events[event_id]
        if sender_id in event.reservation_echos or event.reservation_ready_ok:
            # Dismiss the message...
            return
        solver_id = event.info.solver_id
        if len(event.reservation_echos) == 0 and sender_id != solver_id:
            await self.add_pending_message(sender, payload)
            return
        solver = self.peers[solver_id]
        if payload.sn_r > solver.info.sn_r:
            await self.add_pending_message(sender, payload)
            return

        # Format data
        data = payload.applicant_id + ":" + str(payload.sn_e) + ":" + str(payload.sn_r) + ":" + str(payload.vote)

        # Update storage
        event.reservation_echos[sender_id] = data

        # Required echos for a reservation?
        if self.has_required_negative_replies(event.reservation_echos, self.required_replies_for_proof()):
            # In case of negative equal echos (i.e. repeated sn_r)
            # Debug
            if self.settings.message_debug:
                print("[" + str(self.get_current_time()) +
                      "][Domain:" + str(self.parent_domain_id) +
                      "][Peer:" + self.my_peer_id +
                      "] Sending ReservationCancel (REPEATED RESERVATION) ---> " +
                      "Domain:" + str(event.info.from_domain_id) +
                      ", Applicant:" + payload.applicant_id +
                      ", Event:" + str(payload.sn_e))

            # Solver parent domain sends ReservationCancel messages to the Applicant parent domain
            event.reservation_ready_ok = True
            for peer in self.domains[event.info.from_domain_id]:
                self.ez_send(peer, ReservationCancelPayload(payload.applicant_id, payload.sn_e))
            return
        elif self.has_required_replies(event.reservation_echos, self.required_replies_for_quorum()):
            # In case of positive equal echos
            # Update storage
            solver.info.sn_r += 1

            # Enough resources?
            resource_limit = event.info.workload.resource_limit
            if resource_limit > self.get_free_resources(solver):
                print("[" + str(self.get_current_time()) +
                      "] INFO ---> Solver:" + solver_id + " does not have enough resources for Applicant:" +
                      payload.applicant_id + ", Event:" + str(payload.sn_e) + ", Reservation:" + str(payload.sn_r) +
                      " ---> Cancelling...")

                # Debug
                if self.settings.message_debug:
                    print("[" + str(self.get_current_time()) +
                          "][Domain:" + str(self.parent_domain_id) +
                          "][Peer:" + self.my_peer_id +
                          "] Sending ReservationCancel (NOT ENOUGH RESOURCES) ---> " +
                          "Domain:" + str(event.info.from_domain_id) +
                          ", Applicant:" + payload.applicant_id +
                          ", Event:" + str(payload.sn_e))

                # Solver parent domain sends ReservationCancel messages to the Applicant parent domain
                event.reservation_ready_ok = True
                for peer in self.domains[event.info.from_domain_id]:
                    self.ez_send(peer, ReservationCancelPayload(payload.applicant_id, payload.sn_e))
                return

            # Update storage
            solver.reservations[event_id] = resource_limit

            # Debug
            if self.settings.message_debug:
                print("[" + str(self.get_current_time()) +
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

    # SOLVER PARENT DOMAIN
    async def process_reservation_ready_message(self, sender, payload) -> None:
        # Experiments
        if self.is_faulty_peer(self.my_peer_id):
            # Dismiss the message...
            return

        event_id = get_object_id(payload.applicant_id, payload.sn_e)
        sender_id = get_peer_id(sender)

        # Requirements
        if not self.exist_event(event_id) or not self.events[event_id].info:
            await self.add_pending_message(sender, payload)
            return
        event = self.events[event_id]
        if sender_id in event.reservation_readys or event.reservation_credit_ok:
            # Dismiss the message...
            return

        # Format data
        data = payload.applicant_id + ":" + str(payload.sn_e) + ":" + str(payload.sn_r)

        # Update storage
        event.reservation_readys[sender_id] = data

        # Amplification step (equal readys?)
        if self.has_required_replies(event.reservation_readys, self.required_replies_for_proof()) and \
                not event.reservation_ready_ok:
            solver_id = event.info.solver_id
            solver = self.peers[solver_id]

            # Update storage
            solver.info.sn_r += 1

            # Enough resources?
            resource_limit = event.info.workload.resource_limit
            if resource_limit > self.get_free_resources(solver):
                print("[" + str(self.get_current_time()) +
                      "] INFO ---> Solver:" + solver_id + " does not have enough resources for Applicant:" +
                      payload.applicant_id + ", Event:" + str(payload.sn_e) + ", Reservation:" + str(payload.sn_r) +
                      " ---> Cancelling...")

                # Debug
                if self.settings.message_debug:
                    print("[" + str(self.get_current_time()) +
                          "][Domain:" + str(self.parent_domain_id) +
                          "][Peer:" + self.my_peer_id +
                          "] Sending ReservationCancel (NOT ENOUGH RESOURCES) ---> " +
                          "Domain:" + str(event.info.from_domain_id) +
                          ", Applicant:" + payload.applicant_id +
                          ", Event:" + str(payload.sn_e))

                # Solver parent domain sends ReservationCancel messages to the Applicant parent domain
                event.reservation_ready_ok = True
                for peer in self.domains[event.info.from_domain_id]:
                    self.ez_send(peer, ReservationCancelPayload(payload.applicant_id, payload.sn_e))
                return

            # Update storage
            solver.reservations[event_id] = resource_limit

            # Debug
            if self.settings.message_debug:
                print("[" + str(self.get_current_time()) +
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

        # Required equal readys for reservation delivery?
        if self.has_required_replies(event.reservation_readys, self.required_replies_for_quorum()):
            # Start workload (simulated)
            solver_id = event.info.solver_id
            if self.my_peer_id == solver_id:
                print("[" + str(self.get_current_time()) + "] INFO ---> Solver:" + solver_id +
                      " is now executing the workload for Applicant:" + payload.applicant_id +
                      ", Event:" + str(payload.sn_e) + ", Reservation:" + str(payload.sn_r))

            # Debug
            if self.settings.message_debug:
                print("[" + str(self.get_current_time()) +
                      "][Domain:" + str(self.parent_domain_id) +
                      "][Peer:" + self.my_peer_id +
                      "] Sending ReservationCredit ---> " +
                      "Domain:" + str(event.info.from_domain_id) +
                      ", Applicant:" + payload.applicant_id +
                      ", Event:" + str(payload.sn_e))

            # Solver parent domain sends ReservationCredit messages to the Applicant parent domain
            event.reservation_credit_ok = True
            for peer in self.domains[event.info.from_domain_id]:
                self.ez_send(peer, ReservationCreditPayload(payload.applicant_id, payload.sn_e))

    # APPLICANT PARENT DOMAIN
    def process_reservation_cancel_message(self, sender, payload) -> None:
        # Experiments
        if self.is_faulty_peer(self.my_peer_id):
            # Dismiss the message...
            return

        event_id = get_object_id(payload.applicant_id, payload.sn_e)
        sender_id = get_peer_id(sender)

        # Requirements
        if not self.exist_event(event_id):
            self.events[event_id] = HIDRAEvent()
        event = self.events[event_id]
        if sender_id in event.reservation_cancels or event.reservation_cancel_ok:
            # Dismiss the message...
            return

        # Format data
        data = payload.applicant_id + ":" + str(payload.sn_e)

        # Update storage
        event.reservation_cancels[sender_id] = data

        # Required equal cancels for a reservation?
        if self.has_required_replies(event.reservation_cancels, self.required_replies_for_proof()):
            # Update storage
            event.reservation_cancel_ok = True
            applicant = self.peers[payload.applicant_id]
            if payload.sn_e in applicant.deposits:
                del applicant.deposits[payload.sn_e]

    # APPLICANT PARENT DOMAIN
    async def process_reservation_credit_message(self, sender, payload) -> None:
        # Experiments
        if self.is_faulty_peer(self.my_peer_id):
            # Dismiss the message...
            return

        event_id = get_object_id(payload.applicant_id, payload.sn_e)
        sender_id = get_peer_id(sender)

        # Requirements
        if not self.exist_event(event_id) or not self.events[event_id].info:
            await self.add_pending_message(sender, payload)
            return
        event = self.events[event_id]
        if sender_id in event.reservation_credits or event.confirm_ok:
            # Dismiss the message...
            return

        # Format data
        data = payload.applicant_id + ":" + str(payload.sn_e)

        # Update storage
        event.reservation_credits[sender_id] = data

        # Required equal credits for a reservation?
        if self.has_required_replies(event.reservation_credits, self.required_replies_for_proof()):
            # Event in time to start the WEP?
            ts_start = event.info.ts_start
            if self.get_current_time() <= ts_start:
                # Debug
                if self.settings.message_debug:
                    print("[" + str(self.get_current_time()) +
                          "][Domain:" + str(self.parent_domain_id) +
                          "][Peer:" + self.my_peer_id +
                          "] Sending EventConfirm ---> " +
                          "Domain:" + str(self.parent_domain_id) +
                          ", Applicant:" + payload.applicant_id +
                          ", Event:" + str(payload.sn_e) +
                          ", StartTime:" + str(ts_start))

                # Applicant parent domain sends EventConfirm messages to itself
                event.confirm_ok = True
                for peer in self.domains[self.parent_domain_id]:
                    self.ez_send(peer, EventConfirmPayload(payload.applicant_id, payload.sn_e))
            else:
                # Update storage
                applicant = self.peers[payload.applicant_id]
                if payload.sn_e in applicant.deposits:
                    del applicant.deposits[payload.sn_e]

                # Debug
                if self.settings.message_debug:
                    print("[" + str(self.get_current_time()) +
                          "][Domain:" + str(self.parent_domain_id) +
                          "][Peer:" + self.my_peer_id +
                          "] Sending EventCancel ---> " +
                          "Domain:" + str(event.info.to_domain_id) +
                          ", Applicant:" + payload.applicant_id +
                          ", Event:" + str(payload.sn_e) +
                          ", StartTime:" + str(ts_start))

                # Applicant parent domain sends EventCancel messages to the Solver parent domain
                event.confirm_ok = True
                for peer in self.domains[event.info.to_domain_id]:
                    self.ez_send(peer, EventCancelPayload(payload.applicant_id, payload.sn_e))

    # APPLICANT PARENT DOMAIN
    async def process_event_confirm_message(self, sender, payload) -> None:
        event_id = get_object_id(payload.applicant_id, payload.sn_e)
        sender_id = get_peer_id(sender)

        # Requirements
        if not self.exist_event(event_id) or not self.events[event_id].info:
            await self.add_pending_message(sender, payload)
            return
        event = self.events[event_id]
        if sender_id in event.confirms or event.wrp_ok:
            # Dismiss the message...
            return

        # Format data
        data = payload.applicant_id + ":" + str(payload.sn_e)

        # Update storage
        event.confirms[sender_id] = data

        # Required equal confirms for an event?
        if self.has_required_replies(event.confirms, self.required_replies_for_quorum()):
            # Update storage
            event.wrp_ok = True

            # Update storage (in case of a faulty peer)
            # TODO: Update also reservation storage
            applicant = self.peers[payload.applicant_id]
            if payload.sn_e >= applicant.info.sn_e:
                applicant.info.sn_e = payload.sn_e + 1
            if payload.sn_e not in self.peers[payload.applicant_id].deposits:
                deposit = event.info.t_exec_value * event.info.p_ratio_value
                self.peers[payload.applicant_id].deposits[payload.sn_e] = deposit

            # Workload Execution Phase (WEP)
            if event.info.ts_start > self.get_current_time():
                delay = event.info.ts_start - self.get_current_time()
            else:
                delay = 0
            self.register_task("wep_" + event_id, self.wep, payload.applicant_id, payload.sn_e, delay=delay)

    # SOLVER PARENT DOMAIN
    async def process_event_cancel_message(self, sender, payload) -> None:
        # Experiments
        if self.is_faulty_peer(self.my_peer_id):
            # Dismiss the message...
            return

        event_id = get_object_id(payload.applicant_id, payload.sn_e)
        sender_id = get_peer_id(sender)

        # Requirements
        if not self.exist_event(event_id) or not self.events[event_id].info:
            await self.add_pending_message(sender, payload)
            return
        event = self.events[event_id]
        if sender_id in event.cancels or event.cancel_ok:
            # Dismiss the message...
            return

        # Format data
        data = payload.applicant_id + ":" + str(payload.sn_e)

        # Update storage
        event.cancels[sender_id] = data

        # Required equal cancels for an event?
        if self.has_required_replies(event.cancels, self.required_replies_for_proof()):
            # Update storage
            event.cancel_ok = True
            solver = self.peers[event.info.solver_id]
            if event_id in solver.reservations:
                del solver.reservations[event_id]

            # Stop workload (simulated)
            if self.my_peer_id == event.info.solver_id:
                print("[" + str(self.get_current_time()) + "] INFO ---> Solver:" + event.info.solver_id +
                      " has stopped the workload for Applicant:" + payload.applicant_id +
                      ", Event:" + str(payload.sn_e) + ", StartTime:" + str(event.info.ts_start))

    # APPLICANT PARENT DOMAIN (VALIDATORS)
    async def wep(self, applicant_id, sn_e) -> None:
        event = self.events[get_object_id(applicant_id, sn_e)]
        solver_id = event.info.solver_id

        # TODO: Implement epochs composed of minutes, hours, days...
        epochs = event.info.t_exec_value
        epoch_duration = 1  # In seconds
        # TODO: Delays in the execution of WEP tasks (after ts_start)?

        # For each epoch
        for _ in range(epochs):
            if not event.monitoring_result_ok:
                # Monitoring requests per epoch
                elapsed_time = 0
                for _ in range(self.settings.requests_per_epoch):
                    if not event.monitoring_result_ok:
                        # Wait for a random time to send the monitoring request
                        waiting_time = random.uniform(0, epoch_duration - elapsed_time)
                        await asyncio.sleep(waiting_time)
                        elapsed_time += waiting_time

                        # Debug
                        if self.settings.message_debug:
                            print("[" + str(self.get_current_time()) +
                                  "][Domain:" + str(self.parent_domain_id) +
                                  "][Peer:" + self.my_peer_id +
                                  "] Sending MonitoringRequest ---> " +
                                  "Domain:" + str(event.info.to_domain_id) +
                                  ", Solver:" + solver_id +
                                  ", Port:" + str(event.info.workload.port))

                        # Validators send MonitoringRequest messages to the Solver
                        self.ez_send(self.get_peer_from_id(solver_id), MonitoringRequestPayload(applicant_id, sn_e))
                    else:
                        return

                # Next epoch
                last_time = epoch_duration - elapsed_time
                await asyncio.sleep(last_time)
            else:
                return

        # In case of a successful workload execution (ts_end = ts_exec_value)
        if not event.monitoring_result_ok:
            # Payload data
            ts_end = str(self.get_current_time())

            # Format and sign data
            data = applicant_id + ":" + str(sn_e) + ":" + ts_end
            signature = sign_data(self.my_peer.key, data)

            # Debug
            if self.settings.message_debug:
                print("[" + str(self.get_current_time()) +
                      "][Domain:" + str(self.parent_domain_id) +
                      "][Peer:" + self.my_peer_id +
                      "] Sending MonitoringResult ---> " +
                      "Domain:" + str(self.parent_domain_id) +
                      ", Applicant:" + applicant_id +
                      ", Event:" + str(sn_e) +
                      ", EndTime:" + ts_end)

            # Validators send MonitoringResult messages to the Applicant
            event.monitoring_result_ok = True
            self.ez_send(self.get_peer_from_id(applicant_id),
                         MonitoringResultPayload(applicant_id, sn_e, ts_end, signature))

    # SOLVER
    def process_monitoring_request_message(self, sender, payload) -> None:
        event = self.events[get_object_id(payload.applicant_id, payload.sn_e)]

        # Payload data
        # Successful monitoring request?
        response = random.choice([True, False])

        # Debug
        if self.settings.message_debug:
            print("[" + str(self.get_current_time()) +
                  "][Domain:" + str(self.parent_domain_id) +
                  "][Peer:" + self.my_peer_id +
                  "] Sending MonitoringResponse ---> " +
                  "Domain:" + str(event.info.from_domain_id) +
                  ", Validator:" + get_peer_id(sender) +
                  ", Response:" + ("T" if response else "F"))

        # Solver sends a MonitoringResponse message to the Validator
        self.ez_send(sender, MonitoringResponsePayload(payload.applicant_id, payload.sn_e, response))

    # APPLICANT PARENT DOMAIN (VALIDATORS)
    def process_monitoring_response_message(self, _, payload) -> None:
        event = self.events[get_object_id(payload.applicant_id, payload.sn_e)]

        # Requirements
        if event.monitoring_result_ok:
            # Dismiss the message...
            return

        # Update storage
        if not payload.response:
            event.failed_requests += 1

        # Threshold reached?
        if event.failed_requests == self.settings.failed_requests_threshold:
            # Payload data
            ts_end = str(self.get_current_time())

            # Format and sign data
            data = payload.applicant_id + ":" + str(payload.sn_e) + ":" + ts_end
            signature = sign_data(self.my_peer.key, data)

            # Debug
            if self.settings.message_debug:
                print("[" + str(self.get_current_time()) +
                      "][Domain:" + str(self.parent_domain_id) +
                      "][Peer:" + self.my_peer_id +
                      "] Sending MonitoringResult ---> " +
                      "Domain:" + str(self.parent_domain_id) +
                      ", Applicant:" + payload.applicant_id +
                      ", Event:" + str(payload.sn_e) +
                      ", EndTime:" + ts_end)

            # Validators send MonitoringResult messages to the Applicant
            event.monitoring_result_ok = True
            self.ez_send(self.get_peer_from_id(payload.applicant_id),
                         MonitoringResultPayload(payload.applicant_id, payload.sn_e, ts_end, signature))

    # APPLICANT
    def process_monitoring_result_message(self, sender, payload) -> None:
        event = self.events[get_object_id(payload.applicant_id, payload.sn_e)]
        sender_id = get_peer_id(sender)

        # Requirements
        if sender_id in event.monitoring_results or event.monitoring_send_ok:
            # Dismiss the message...
            return
        data = payload.applicant_id + ":" + str(payload.sn_e) + ":" + payload.ts_end
        if not verify_sign(sender.public_key, data, payload.signature):
            print("[" + str(self.get_current_time()) + "] INFO ---> Validator:" + sender_id +
                  " sent an invalid signature for Applicant:" + payload.applicant_id + ", Event:" + str(payload.sn_e))
            # Dismiss the message...
            return

        # Format data
        # Using latin-1 to encode signatures (in bytes) in order to serialize the whole dict
        data = [payload.ts_end, str(payload.signature, "latin-1")]

        # Update storage
        event.monitoring_results[sender_id] = data

        # Required monitoring results?
        if len(event.monitoring_results) == self.required_replies_for_quorum():
            # Debug
            if self.settings.message_debug:
                print("[" + str(self.get_current_time()) +
                      "][Domain:" + str(self.parent_domain_id) +
                      "][Peer:" + self.my_peer_id +
                      "] Sending MonitoringSend ---> " +
                      "Domain:" + str(self.parent_domain_id) +
                      ", Applicant:" + payload.applicant_id +
                      ", Event:" + str(payload.sn_e))

            # Applicant sends MonitoringSend messages to the Applicant parent domain
            event.monitoring_send_ok = True
            for peer in self.domains[self.parent_domain_id]:
                self.ez_send(peer, MonitoringSendPayload(payload.applicant_id, payload.sn_e, event.monitoring_results))

    # APPLICANT PARENT DOMAIN (VALIDATORS)
    def process_monitoring_send_message(self, _, payload) -> None:
        event = self.events[get_object_id(payload.applicant_id, payload.sn_e)]

        # Requirements
        if event.monitoring_echo_ok:
            # Dismiss the message...
            return
        if len(payload.monitoring_results) != self.required_replies_for_quorum():
            print("[" + str(self.get_current_time()) + "] INFO ---> Invalid monitoring QC for Applicant:" +
                  payload.applicant_id + ", Event:" + str(payload.sn_e))
            # Dismiss the message...
            return
        for k, v in payload.monitoring_results.items():
            data = payload.applicant_id + ":" + str(payload.sn_e) + ":" + v[0]
            if not verify_sign(self.get_peer_from_id(k).public_key, data, bytes(v[1], "latin1")):
                print("[" + str(self.get_current_time()) + "] INFO ---> Invalid monitoring QC for Applicant:"
                      + payload.applicant_id + ", Event:" + str(payload.sn_e))
                # Dismiss the message...
                return

        # Update storage (tmp)
        if not event.monitoring_results:
            event.monitoring_results = payload.monitoring_results

        # Payload data
        monitoring_results_hash = hash_dict(payload.monitoring_results)

        # Debug
        if self.settings.message_debug:
            print("[" + str(self.get_current_time()) +
                  "][Domain:" + str(self.parent_domain_id) +
                  "][Peer:" + self.my_peer_id +
                  "] Sending MonitoringEcho ---> " +
                  "Domain:" + str(self.parent_domain_id) +
                  ", Applicant:" + payload.applicant_id +
                  ", Event:" + str(payload.sn_e))

        # Applicant parent domain sends MonitoringEcho messages to itself
        event.monitoring_echo_ok = True
        for peer in self.domains[self.parent_domain_id]:
            self.ez_send(peer, MonitoringEchoPayload(payload.applicant_id, payload.sn_e, monitoring_results_hash))

    # APPLICANT PARENT DOMAIN (VALIDATORS)
    async def process_monitoring_echo_message(self, sender, payload) -> None:
        event = self.events[get_object_id(payload.applicant_id, payload.sn_e)]
        sender_id = get_peer_id(sender)

        # Requirements
        if not event.monitoring_results:
            await self.add_pending_message(sender, payload)
            return
        if sender_id in event.monitoring_echos or event.monitoring_credit_ok:
            # Dismiss the message...
            return

        # Format data
        data = payload.applicant_id + ":" + str(payload.sn_e) + ":" + str(payload.monitoring_results_hash)

        # Update storage
        event.monitoring_echos[sender_id] = data

        # Required equal monitoring echos?
        if self.has_required_replies(event.monitoring_echos, self.required_replies_for_quorum()):
            # Update storage
            event.monitoring_credit_ok = True

            # Checking mismatched QCs sent by the Applicant
            if hash_dict(event.monitoring_results) != payload.monitoring_results_hash:
                print("[" + str(self.get_current_time()) + "] INFO ---> Invalid monitoring QC for Applicant:"
                      + payload.applicant_id + ", Event:" + str(payload.sn_e))
                # Dismiss the message...
                return

            # Payload data
            ts_end = self.select_shared_ts_end(event.monitoring_results)

            # Debug
            if self.settings.message_debug:
                print("[" + str(self.get_current_time()) +
                      "][Domain:" + str(self.parent_domain_id) +
                      "][Peer:" + self.my_peer_id +
                      "] Sending MonitoringCredit ---> " +
                      "ApplicantDomain:" + str(self.parent_domain_id) +
                      ", SolverDomain:" + str(event.info.to_domain_id) +
                      ", Applicant:" + payload.applicant_id +
                      ", Event:" + str(payload.sn_e))

            # Applicant parent domain sends MonitoringCredit messages
            if self.parent_domain_id == event.info.to_domain_id:
                peers = self.domains[self.parent_domain_id]  # Intra
            else:
                peers = self.domains[self.parent_domain_id] + self.domains[event.info.to_domain_id]  # Inter
            for peer in peers:
                self.ez_send(peer, MonitoringCreditPayload(payload.applicant_id, payload.sn_e,
                                                           ts_end, payload.monitoring_results_hash))

    # APPLICANT AND SOLVER PARENT DOMAINS
    async def process_monitoring_credit_message(self, sender, payload) -> None:
        event_id = get_object_id(payload.applicant_id, payload.sn_e)
        event = self.events[event_id]
        sender_id = get_peer_id(sender)

        # Requirements
        if self.parent_domain_id == event.info.from_domain_id and not event.monitoring_results:
            await self.add_pending_message(sender, payload)
            return
        if sender_id in event.monitoring_credits or event.wep_ok:
            # Dismiss the message...
            return

        # Format data
        data = payload.applicant_id + ":" + str(payload.sn_e) + ":" + \
               payload.ts_end + ":" + str(payload.monitoring_results_hash)

        # Update storage
        event.monitoring_credits[sender_id] = data

        # Amplification step (equal readys/credits?)
        if self.parent_domain_id == event.info.from_domain_id and \
                self.has_required_replies(event.monitoring_credits, self.required_replies_for_proof()) and \
                not event.monitoring_credit_ok:
            # Update storage
            event.monitoring_credit_ok = True

            # Checking mismatched QCs sent by the Applicant
            if hash_dict(event.monitoring_results) != payload.monitoring_results_hash:
                print("[" + str(self.get_current_time()) + "] INFO ---> Invalid monitoring QC for Applicant:"
                      + payload.applicant_id + ", Event:" + str(payload.sn_e))
                # Dismiss the message...
                return

            # Payload data
            ts_end = self.select_shared_ts_end(event.monitoring_results)

            # Debug
            if self.settings.message_debug:
                print("[" + str(self.get_current_time()) +
                      "][Domain:" + str(self.parent_domain_id) +
                      "][Peer:" + self.my_peer_id +
                      "] Sending MonitoringCredit ---> " +
                      "ApplicantDomain:" + str(self.parent_domain_id) +
                      ", SolverDomain:" + str(event.info.to_domain_id) +
                      ", Applicant:" + payload.applicant_id +
                      ", Event:" + str(payload.sn_e))

            # Applicant parent domain sends MonitoringCredit messages
            if self.parent_domain_id == event.info.to_domain_id:
                peers = self.domains[self.parent_domain_id]  # Intra
            else:
                peers = self.domains[self.parent_domain_id] + self.domains[event.info.to_domain_id]  # Inter
            for peer in peers:
                self.ez_send(peer, MonitoringCreditPayload(payload.applicant_id, payload.sn_e,
                                                           ts_end, payload.monitoring_results_hash))

        # Required equal monitoring credits?
        if self.has_required_replies(event.monitoring_credits, self.required_replies_for_quorum()):
            # Update storage
            event.wep_ok = True

            # Settlement calculation
            epoch_duration = 1  # In seconds
            completed_epochs = int(round(float(payload.ts_end) - event.info.ts_start, 9) / epoch_duration)
            final_payment = completed_epochs * event.info.p_ratio_value

            # Update storage (Applicant parent domain)
            if self.parent_domain_id == event.info.from_domain_id:
                applicant = self.peers[payload.applicant_id]
                applicant.info.balance -= final_payment
                if payload.sn_e in applicant.deposits:
                    del applicant.deposits[payload.sn_e]

            # Update storage (Solver parent domain)
            if self.parent_domain_id == event.info.to_domain_id:
                solver = self.peers[event.info.solver_id]
                solver.info.balance += final_payment
                if event_id in solver.reservations:
                    del solver.reservations[event_id]

                # Release workload resources (simulated)
                if self.my_peer_id == event.info.solver_id:
                    print("[" + str(self.get_current_time()) + "] INFO ---> Solver:" + event.info.solver_id +
                          " has completed the workload for Applicant:" + payload.applicant_id +
                          ", Event:" + str(payload.sn_e) + ", Payment:" + str(final_payment))

                    print("Ending at %d" % time.time_ns())

    ############
    # Messages #
    ############
    async def add_pending_message(self, sender, payload) -> None:
        # Get the next message ID
        async with self.sn_m_lock:
            message_id = get_object_id(self.my_peer_id, self.next_sn_m)
            self.next_sn_m += 1

        # Update storage
        self.messages[message_id] = IPv8PendingMessage(sender, payload)

    async def process_pending_messages(self) -> None:
        for k, v in list(self.messages.items()):
            # Update storage
            self.messages.pop(k)

            # Depending on the message type...
            if v.payload.msg_id == LOCKING_SEND:
                await self.process_locking_send_message(v.sender, v.payload)
            elif v.payload.msg_id == LOCKING_CREDIT:
                await self.process_locking_credit_message(v.sender, v.payload)
            elif v.payload.msg_id == RESERVATION_ECHO:
                await self.process_reservation_echo_message(v.sender, v.payload)
            elif v.payload.msg_id == RESERVATION_READY:
                await self.process_reservation_ready_message(v.sender, v.payload)
            elif v.payload.msg_id == RESERVATION_CREDIT:
                await self.process_reservation_credit_message(v.sender, v.payload)
            elif v.payload.msg_id == EVENT_CONFIRM:
                await self.process_event_confirm_message(v.sender, v.payload)
            elif v.payload.msg_id == EVENT_CANCEL:
                await self.process_event_cancel_message(v.sender, v.payload)
            elif v.payload.msg_id == MONITORING_ECHO:
                await self.process_monitoring_echo_message(v.sender, v.payload)
            elif v.payload.msg_id == MONITORING_CREDIT:
                await self.process_monitoring_credit_message(v.sender, v.payload)

    ############
    # Checkers #
    ############
    def exist_event(self, event_id: str) -> bool:
        return event_id in self.events

    @staticmethod
    def has_required_replies(replies: dict, required_replies: int) -> bool:
        # Count the most common replies
        counter = Counter(replies.values()).most_common(1)

        if counter and counter[0][1] == required_replies:
            return True
        else:
            return False

    @staticmethod
    def has_required_negative_replies(replies: dict, required_replies: int) -> bool:
        # Count equal replies
        for i in Counter(replies.values()).most_common():
            if i[0].split(":")[3] == 'False' and i[1] == required_replies:
                return True
        return False

    #########
    # Utils #
    #########
    def get_current_time(self):
        if self.settings.deployment_mode:
            return int(time.time())
        else:
            return round(get_event_loop().time(), 9)

    def select_domain(self) -> int:
        if len(self.domains) > 1 and self.settings.domain_selection_policy == "inter":
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

    def required_replies_for_quorum(self) -> int:
        """
        Number of required replies to get a quorum certificate on a message
        """

        if self.settings.deployment_mode:
            return 2 * DeploymentSettings.faulty_peers + 1
        else:
            return 2 * SimulationSettings.faulty_peers + 1

    def required_replies_for_proof(self) -> int:
        """
        Number of required replies to prove a message
        """

        if self.settings.deployment_mode:
            return DeploymentSettings.faulty_peers + 1
        else:
            return SimulationSettings.faulty_peers + 1

    def get_peer_from_id(self, peer_id: str) -> Peer:
        for v in self.domains.values():
            for peer in v:
                if get_peer_id(peer) == peer_id:
                    return peer

    def get_peer_domain(self, peer_id: str) -> int:
        for k, v in self.domains.items():
            for peer in v:
                if get_peer_id(peer) == peer_id:
                    return k

    def select_shared_ts_end(self, monitoring_results: dict) -> str:
        # Sort monitoring results by ts_end (in ascending order)
        mr = sorted(monitoring_results.items(), key=lambda i: float(i[1][0]))

        # Select the (f + 1)-th smallest ts_end
        if self.settings.deployment_mode:
            return mr[DeploymentSettings.faulty_peers][1][0]
        else:
            return mr[SimulationSettings.faulty_peers][1][0]

    ###############
    # Experiments #
    ###############
    def set_faulty_peer(self, peer_id: str) -> None:
        global FAULTY_PEER
        if not FAULTY_PEER:
            FAULTY_PEER = peer_id
            print("[" + str(self.get_current_time()) + "] INFO ---> Peer:" + peer_id + " set as faulty peer")

    @staticmethod
    def is_faulty_peer(peer_id: str) -> bool:
        return FAULTY_PEER == peer_id

    @staticmethod
    def set_repeated_sn_r(sn_r: int, index: int) -> int:
        if sn_r == index > 0:
            return sn_r - 1  # Previous sequence number
        else:
            return sn_r
