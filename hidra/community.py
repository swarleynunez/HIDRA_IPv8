import asyncio
import json
import random
from asyncio import get_event_loop
from binascii import unhexlify
from collections import Counter

from bami.settings import SimulationSettings
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
from hidra.utils import get_peer_id, get_object_id
from pyipv8.ipv8.community import Community
from pyipv8.ipv8.lazy_community import lazy_wrapper
from pyipv8.ipv8.peer import Peer

# Experiments
FAULTY_PEER = "ANY"


class HIDRACommunity(Community):
    """
    HIDRA community
    """

    community_id = unhexlify("2d606de41ee6595b2d3d5c57065b78bf17870f32")

    def __init__(self, my_peer, endpoint, network) -> None:
        super().__init__(my_peer, endpoint, network)
        self.settings = HIDRASettings()

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
    async def start(self) -> None:
        # Register asyncio tasks with the community
        # This ensures that tasks end when the community is unloaded
        self.register_task("initialize_peer", self.initialize_peer)
        await self.wait_for_tasks()

        # To avoid missing IPv8 messages due to an early simulation end
        self.register_task("process_pending_messages", self.process_pending_messages, interval=1)

        # Experiments
        self.set_faulty_peer(self.my_peer_id)
        if self.is_faulty_peer(self.my_peer_id):
            # Dismiss the message...
            return

        # Sending offloading events
        delay = 0
        for _ in range(self.settings.events_per_peer):
            # Next event sequence number
            sn_e = self.next_sn_e
            self.next_sn_e += 1

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
        self.process_monitoring_echo_message(sender, payload)

    @lazy_wrapper(MonitoringCreditPayload)
    async def on_monitoring_credit(self, sender, payload) -> None:
        await self.process_pending_messages()
        self.process_monitoring_credit_message(sender, payload)

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
            print("[" + format(self.get_current_time(), ".3f") +
                  "][Domain:" + str(self.parent_domain_id) +
                  "][Peer:" + self.my_peer_id +
                  "] Sending RequestResourceInfo ---> " +
                  "Domain:" + str(to_domain_id) +
                  ", Applicant:" + self.my_peer_id +
                  ", Event:" + str(sn_e))

        # Applicant sends RequestResourceInfo messages to the selected domain
        for peer in self.domains[to_domain_id]:
            self.ez_send(peer, RequestResourceInfoPayload(self.my_peer_id, sn_e, event_info))

    # SELECTED DOMAIN
    def process_request_resource_info_message(self, sender, payload) -> None:
        # Experiments
        if self.is_faulty_peer(self.my_peer_id):
            # Dismiss the message...
            return

        # Payload data
        # TODO: Check event info to make a decision
        available = random.choice([True, False])
        resource_replies = {}
        for peer in self.domains[self.parent_domain_id]:
            peer_id = get_peer_id(peer)
            resource_replies[peer_id] = self.peers[peer_id].info

        # Debug
        if self.settings.message_debug:
            print("[" + format(self.get_current_time(), ".3f") +
                  "][Domain:" + str(self.parent_domain_id) +
                  "][Peer:" + self.my_peer_id +
                  "] Sending ResourceInfo ---> " +
                  "Domain:" + str(self.get_peer_domain(payload.applicant_id)) +
                  ", Applicant:" + payload.applicant_id +
                  ", Event:" + str(payload.sn_e) +
                  ", Available:" + ("T" if available else "F"))

        # Selected domain sends ResourceInfo messages to the Applicant
        self.ez_send(sender, ResourceInfoPayload(payload.applicant_id, payload.sn_e, available, resource_replies))

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
        for k, v in payload.resource_replies.items():
            # Update storage
            self.peers[k].resource_replies[sender_id] = v

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
            print("[" + format(self.get_current_time(), ".3f") + "] INFO ---> Domain:" + str(event.info.to_domain_id) +
                  " not available for Applicant:" + self.my_peer_id + ", Event:" + str(sn_e) + " ---> Retrying...")
            self.replace_task("ssp_" + str(sn_e), self.ssp, sn_e)
            self.replace_task("wrp_" + str(sn_e), self.wrp, sn_e, delay=self.settings.ssp_timeout)
            return

        # TODO: Check event resource_replies to make a decision
        solver_id = random.choice(event.available_peers)

        # Update storage
        event.info.solver_id = solver_id
        event.info.ts_start = self.get_current_time() + self.settings.ts_start

        # Debug
        if self.settings.message_debug:
            print("[" + format(self.get_current_time(), ".3f") +
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
            print("[" + format(self.get_current_time(), ".3f") + "] INFO ---> Applicant:" + payload.applicant_id +
                  " does not have enough balance for Event:" + str(payload.sn_e) + " ---> Waiting...")
            await self.add_pending_message(sender, payload)
            return

        # Update storage
        applicant.info.sn_e += 1
        applicant.deposits[payload.sn_e] = deposit
        if not self.exist_event(event_id):
            self.events[event_id] = HIDRAEvent()
            self.events[event_id].info = payload.event_info

        # Debug
        if self.settings.message_debug:
            print("[" + format(self.get_current_time(), ".3f") +
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
            # Debug
            if self.settings.message_debug:
                print("[" + format(self.get_current_time(), ".3f") +
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

        # Amplification step
        if self.has_required_replies(event.locking_readys, self.required_replies_for_proof()) and \
                not event.locking_ready_ok:
            # Debug
            if self.settings.message_debug:
                print("[" + format(self.get_current_time(), ".3f") +
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
            # Get Solver domain
            to_domain_id = payload.event_info.to_domain_id

            # Debug
            if self.settings.message_debug:
                print("[" + format(self.get_current_time(), ".3f") +
                      "][Domain:" + str(self.parent_domain_id) +
                      "][Peer:" + self.my_peer_id +
                      "] Sending LockingCredit ---> " +
                      "Domain:" + str(to_domain_id) +
                      ", Applicant:" + payload.applicant_id +
                      ", Event:" + str(payload.sn_e))

            # Applicant parent domain sends LockingCredit messages to the Solver parent domain
            event.locking_credit_ok = True
            for peer in self.domains[to_domain_id]:
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
        if not self.exist_event(event_id):
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
            solver_id = payload.event_info.solver_id

            # Update storage
            if not event.info:
                event.info = payload.event_info

            # Am I the event Solver?
            if self.my_peer_id == solver_id:
                solver = self.peers[solver_id]

                # Next reservation sequence number (accessed via mutex)
                async with self.sn_r_lock_1:
                    sn_r = solver.info.sn_r
                    solver.info.sn_r += 1

                # Debug
                if self.settings.message_debug:
                    print("[" + format(self.get_current_time(), ".3f") +
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
                # sn_r = self.set_repeated_sn_r(sn_r, 1, solver)

                # Solver parent domain sends ReservationEcho messages to itself
                event.reservation_echo_ok = True
                for peer in self.domains[self.parent_domain_id]:
                    self.ez_send(peer, ReservationEchoPayload(payload.applicant_id, payload.sn_e,
                                                              sn_r, True, payload.event_info))
            else:
                if solver_id in event.reservation_echos:
                    solver = self.peers[solver_id]

                    # Next reservation sequence number (accessed via mutex)
                    async with self.sn_r_lock_2:
                        # Payload data
                        # Peers vote YES/NO on the reservation sequence number received from the Solver
                        sn_r = int(event.reservation_echos[solver_id].split(":")[2])
                        vote = sn_r == solver.info.sn_r
                        # TODO: Bug (5x5 peers, inter, 50 events, 1s delay)
                        if vote:
                            solver.info.sn_r += 1
                        else:
                            print("FALSE:", self.my_peer_id, payload.applicant_id, payload.sn_e, sn_r, solver.info.sn_r)

                    # Debug
                    if self.settings.message_debug:
                        print("[" + format(self.get_current_time(), ".3f") +
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
                        self.ez_send(peer, ReservationEchoPayload(payload.applicant_id, payload.sn_e,
                                                                  sn_r, vote, payload.event_info))
                else:
                    # Reservation carried out?
                    # Cleaning of stuck pending messages due to the amplification step
                    if not event.reservation_credit_ok:
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
        if payload.sn_r > solver.next_sn_r:
            await self.add_pending_message(sender, payload)
            return

        # Format data
        data = payload.applicant_id + ":" + str(payload.sn_e) + ":" + str(payload.sn_r) + \
               ":" + str(payload.vote) + ":" + str(payload.event_info)

        # Update storage
        event.reservation_echos[sender_id] = data

        # Required echos for a reservation?
        if self.has_required_negative_replies(event.reservation_echos, self.required_replies_for_proof()):
            # In case of negative echos (i.e. repeated sn_r)
            # Get Applicant domain
            from_domain_id = payload.event_info.from_domain_id

            # Debug
            if self.settings.message_debug:
                print("[" + format(self.get_current_time(), ".3f") +
                      "][Domain:" + str(self.parent_domain_id) +
                      "][Peer:" + self.my_peer_id +
                      "] Sending ReservationCancel ---> " +
                      "Domain:" + str(from_domain_id) +
                      ", Applicant:" + payload.applicant_id +
                      ", Event:" + str(payload.sn_e))

            # Solver parent domain sends ReservationCancel messages to the Applicant parent domain
            event.reservation_ready_ok = True
            for peer in self.domains[from_domain_id]:
                self.ez_send(peer, ReservationCancelPayload(payload.applicant_id, payload.sn_e))
            return
        elif self.has_required_replies(event.reservation_echos, self.required_replies_for_quorum()):
            # In case of positive echos
            # Update storage
            solver.next_sn_r += 1

            # Get Applicant domain
            from_domain_id = payload.event_info.from_domain_id

            # Enough resources?
            resource_limit = payload.event_info.workload.resource_limit
            if resource_limit > self.get_free_resources(solver):
                print("[" + format(self.get_current_time(), ".3f") +
                      "] INFO ---> Solver:" + solver_id + " does not have enough resources for Applicant:" +
                      payload.applicant_id + ", Event:" + str(payload.sn_e) + " ---> Cancelling...")

                # Debug
                if self.settings.message_debug:
                    print("[" + format(self.get_current_time(), ".3f") +
                          "][Domain:" + str(self.parent_domain_id) +
                          "][Peer:" + self.my_peer_id +
                          "] Sending ReservationCancel ---> " +
                          "Domain:" + str(from_domain_id) +
                          ", Applicant:" + payload.applicant_id +
                          ", Event:" + str(payload.sn_e))

                # Solver parent domain sends ReservationCancel messages to the Applicant parent domain
                event.reservation_ready_ok = True
                for peer in self.domains[from_domain_id]:
                    self.ez_send(peer, ReservationCancelPayload(payload.applicant_id, payload.sn_e))
                return

            # Update storage
            solver.reservations[payload.sn_r] = resource_limit

            # Debug
            if self.settings.message_debug:
                print("[" + format(self.get_current_time(), ".3f") +
                      "][Domain:" + str(self.parent_domain_id) +
                      "][Peer:" + self.my_peer_id +
                      "] Sending ReservationReady ---> " +
                      "Domain:" + str(self.parent_domain_id) +
                      ", Applicant:" + payload.applicant_id +
                      ", Event:" + str(payload.sn_e))

            # Solver parent domain sends ReservationReady messages to itself
            event.reservation_ready_ok = True
            for peer in self.domains[self.parent_domain_id]:
                self.ez_send(peer, ReservationReadyPayload(payload.applicant_id, payload.sn_e,
                                                           payload.sn_r, payload.event_info))

    # SOLVER PARENT DOMAIN
    def process_reservation_ready_message(self, sender, payload) -> None:
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
        if sender_id in event.reservation_readys or event.reservation_credit_ok:
            # Dismiss the message...
            return

        # Format data
        data = payload.applicant_id + ":" + str(payload.sn_e) + ":" + str(payload.sn_r) + ":" + str(payload.event_info)

        # Update storage
        event.reservation_readys[sender_id] = data

        # Amplification step
        if self.has_required_replies(event.reservation_readys, self.required_replies_for_proof()) and \
                not event.reservation_ready_ok:
            solver_id = payload.event_info.solver_id
            solver = self.peers[solver_id]

            # Update storage
            solver.next_sn_r += 1

            # Enough resources?
            resource_limit = payload.event_info.workload.resource_limit
            if resource_limit > self.get_free_resources(solver):
                print("[" + format(self.get_current_time(), ".3f") +
                      "] INFO ---> Solver:" + solver_id + " does not have enough resources for Applicant:" +
                      payload.applicant_id + ", Event:" + str(payload.sn_e) + " ---> Cancelling...")

                # Get Applicant domain
                from_domain_id = payload.event_info.from_domain_id

                # Debug
                if self.settings.message_debug:
                    print("[" + format(self.get_current_time(), ".3f") +
                          "][Domain:" + str(self.parent_domain_id) +
                          "][Peer:" + self.my_peer_id +
                          "] Sending ReservationCancel ---> " +
                          "Domain:" + str(from_domain_id) +
                          ", Applicant:" + payload.applicant_id +
                          ", Event:" + str(payload.sn_e))

                # Solver parent domain sends ReservationCancel messages to the Applicant parent domain
                event.reservation_ready_ok = True
                for peer in self.domains[from_domain_id]:
                    self.ez_send(peer, ReservationCancelPayload(payload.applicant_id, payload.sn_e))
                return

            # Update storage
            solver.reservations[payload.sn_r] = resource_limit

            # Debug
            if self.settings.message_debug:
                print("[" + format(self.get_current_time(), ".3f") +
                      "][Domain:" + str(self.parent_domain_id) +
                      "][Peer:" + self.my_peer_id +
                      "] Sending ReservationReady ---> " +
                      "Domain:" + str(self.parent_domain_id) +
                      ", Applicant:" + payload.applicant_id +
                      ", Event:" + str(payload.sn_e))

            # Solver parent domain sends ReservationReady messages to itself
            event.reservation_ready_ok = True
            for peer in self.domains[self.parent_domain_id]:
                self.ez_send(peer, ReservationReadyPayload(payload.applicant_id, payload.sn_e,
                                                           payload.sn_r, payload.event_info))

        # Required equal readys for reservation delivery?
        if self.has_required_replies(event.reservation_readys, self.required_replies_for_quorum()):
            # Start workload (simulated)
            solver_id = payload.event_info.solver_id
            if self.my_peer_id == solver_id:
                print("[" + format(self.get_current_time(), ".3f") + "] INFO ---> Solver:" + solver_id +
                      " is now executing the workload for Applicant:" + payload.applicant_id +
                      ", Event:" + str(payload.sn_e))

            # Get Applicant domain
            from_domain_id = payload.event_info.from_domain_id

            # Debug
            if self.settings.message_debug:
                print("[" + format(self.get_current_time(), ".3f") +
                      "][Domain:" + str(self.parent_domain_id) +
                      "][Peer:" + self.my_peer_id +
                      "] Sending ReservationCredit ---> " +
                      "Domain:" + str(from_domain_id) +
                      ", Applicant:" + payload.applicant_id +
                      ", Event:" + str(payload.sn_e))

            # Solver parent domain sends ReservationCredit messages to the Applicant parent domain
            event.reservation_credit_ok = True
            for peer in self.domains[from_domain_id]:
                self.ez_send(peer, ReservationCreditPayload(payload.applicant_id, payload.sn_e,
                                                            payload.sn_r, payload.event_info))

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
            del self.peers[payload.applicant_id].deposits[payload.sn_e]

    # APPLICANT PARENT DOMAIN
    def process_reservation_credit_message(self, sender, payload) -> None:
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
        if sender_id in event.reservation_credits or event.confirm_ok:
            # Dismiss the message...
            return

        # Format data
        data = payload.applicant_id + ":" + str(payload.sn_e) + ":" + str(payload.sn_r) + ":" + str(payload.event_info)

        # Update storage
        event.reservation_credits[sender_id] = data

        # Required equal credits for a reservation?
        if self.has_required_replies(event.reservation_credits, self.required_replies_for_proof()):
            # Event in time to start the WEP?
            if self.get_current_time() <= payload.event_info.ts_start:
                # Debug
                if self.settings.message_debug:
                    print("[" + format(self.get_current_time(), ".3f") +
                          "][Domain:" + str(self.parent_domain_id) +
                          "][Peer:" + self.my_peer_id +
                          "] Sending EventConfirm ---> " +
                          "Domain:" + str(self.parent_domain_id) +
                          ", Applicant:" + payload.applicant_id +
                          ", Event:" + str(payload.sn_e))

                # Applicant parent domain sends EventConfirm messages to itself
                event.confirm_ok = True
                for peer in self.domains[self.parent_domain_id]:
                    self.ez_send(peer, EventConfirmPayload(payload.applicant_id, payload.sn_e, payload.event_info))
            else:
                # Update storage
                del self.peers[payload.applicant_id].deposits[payload.sn_e]

                # Get Solver domain
                to_domain_id = payload.event_info.to_domain_id

                # Debug
                if self.settings.message_debug:
                    print("[" + format(self.get_current_time(), ".3f") +
                          "][Domain:" + str(self.parent_domain_id) +
                          "][Peer:" + self.my_peer_id +
                          "] Sending EventCancel ---> " +
                          "Domain:" + str(to_domain_id) +
                          ", Applicant:" + payload.applicant_id +
                          ", Event:" + str(payload.sn_e) +
                          ", StartTime:" + str(payload.event_info.ts_start))

                # Applicant parent domain sends EventCancel messages to the Solver parent domain
                event.confirm_ok = True
                for peer in self.domains[to_domain_id]:
                    self.ez_send(peer, EventCancelPayload(payload.applicant_id, payload.sn_e,
                                                          payload.event_info.solver_id, payload.sn_r))

    # APPLICANT PARENT DOMAIN
    def process_event_confirm_message(self, sender, payload) -> None:
        event_id = get_object_id(payload.applicant_id, payload.sn_e)
        sender_id = get_peer_id(sender)

        # Requirements
        if not self.exist_event(event_id):
            self.events[event_id] = HIDRAEvent()
        event = self.events[event_id]
        if sender_id in event.confirms or event.wrp_ok:
            # Dismiss the message...
            return

        # Format data
        data = payload.applicant_id + ":" + str(payload.sn_e) + ":" + str(payload.event_info)

        # Update storage
        event.confirms[sender_id] = data

        # Required equal confirms for an event?
        if self.has_required_replies(event.confirms, self.required_replies_for_quorum()):
            # Update storage
            event.wrp_ok = True
            if not event.info:
                event.info = payload.event_info

            # Update storage (in case of a faulty peer)
            # TODO: Update also reservation storage
            applicant = self.peers[payload.applicant_id]
            if payload.sn_e >= applicant.info.sn_e:
                applicant.info.sn_e = payload.sn_e + 1
            if payload.sn_e not in self.peers[payload.applicant_id].deposits:
                deposit = payload.event_info.t_exec_value * payload.event_info.p_ratio_value
                self.peers[payload.applicant_id].deposits[payload.sn_e] = deposit

            # Workload Execution Phase (WEP)
            delay = payload.event_info.ts_start - self.get_current_time()
            self.register_task("wep_" + event_id, self.wep, payload.applicant_id, payload.sn_e, delay=delay)

    # SOLVER PARENT DOMAIN
    def process_event_cancel_message(self, sender, payload) -> None:
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
        if sender_id in event.cancels or event.cancel_ok:
            # Dismiss the message...
            return

        # Format data
        data = payload.applicant_id + ":" + str(payload.sn_e) + ":" + payload.solver_id + ":" + str(payload.sn_r)

        # Update storage
        event.cancels[sender_id] = data

        # Required equal cancels for an event?
        if self.has_required_replies(event.cancels, self.required_replies_for_proof()):
            # Update storage
            event.cancel_ok = True
            del self.peers[payload.solver_id].reservations[payload.sn_r]

            # Stop workload (simulated)
            if self.my_peer_id == payload.solver_id:
                print("[" + format(self.get_current_time(), ".3f") + "] INFO ---> Solver:" + payload.solver_id +
                      " has stopped the workload for Applicant:" + payload.applicant_id +
                      ", Event:" + str(payload.sn_e))

    # APPLICANT PARENT DOMAIN (VALIDATORS)
    async def wep(self, applicant_id, sn_e) -> None:
        event = self.events[get_object_id(applicant_id, sn_e)]
        solver_id = event.info.solver_id

        # TODO: Implement epochs composed of minutes, hours, days...
        epochs = event.info.t_exec_value
        epoch_duration = 1  # One second

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
                            print("[" + format(self.get_current_time(), ".3f") +
                                  "][Domain:" + str(self.parent_domain_id) +
                                  "][Peer:" + self.my_peer_id +
                                  "] Sending MonitoringRequest ---> " +
                                  "Domain:" + str(event.info.to_domain_id) +
                                  ", Solver:" + solver_id +
                                  ", Port:" + str(event.info.workload.port))

                        # Validator sends a MonitoringRequest message to the Solver
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
            # TODO: Signed ts_end
            ts_end = str(self.get_current_time())

            # Debug
            if self.settings.message_debug:
                print("[" + format(self.get_current_time(), ".3f") +
                      "][Domain:" + str(self.parent_domain_id) +
                      "][Peer:" + self.my_peer_id +
                      "] Sending MonitoringResult ---> " +
                      "Domain:" + str(self.parent_domain_id) +
                      ", Applicant:" + applicant_id +
                      ", Event:" + str(sn_e) +
                      ", EndTime:" + ts_end)

            # Validator sends a MonitoringResult message to the Applicant
            event.monitoring_result_ok = True
            self.ez_send(self.get_peer_from_id(applicant_id), MonitoringResultPayload(applicant_id, sn_e, ts_end))

    # SOLVER
    def process_monitoring_request_message(self, sender, payload) -> None:
        event = self.events[get_object_id(payload.applicant_id, payload.sn_e)]

        # Payload data
        # Successful monitoring request?
        response = random.choice([True, False])

        # Debug
        if self.settings.message_debug:
            print("[" + format(self.get_current_time(), ".3f") +
                  "][Domain:" + str(self.parent_domain_id) +
                  "][Peer:" + self.my_peer_id +
                  "] Sending MonitoringResponse ---> " +
                  "Domain:" + str(event.info.from_domain_id) +
                  ", Validator:" + get_peer_id(sender) +
                  ", Response:" + ("T" if response else "F"))

        # Solver sends a MonitoringResponse message to the Validator
        self.ez_send(sender, MonitoringResponsePayload(payload.applicant_id, payload.sn_e, response))

    # APPLICANT PARENT DOMAIN (VALIDATORS)
    def process_monitoring_response_message(self, sender, payload) -> None:
        event = self.events[get_object_id(payload.applicant_id, payload.sn_e)]

        # Update storage
        if not payload.response:
            event.failed_requests += 1

        # Threshold reached?
        if event.failed_requests >= self.settings.failed_requests_threshold:
            print("FAILED WORKLOAD", self.my_peer_id, self.get_current_time())

            # Debug

            #

    # APPLICANT
    def process_monitoring_result_message(self, sender, payload) -> None:
        pass

    # APPLICANT PARENT DOMAIN (VALIDATORS)
    def process_monitoring_send_message(self, sender, payload) -> None:
        pass

    # APPLICANT PARENT DOMAIN (VALIDATORS)
    def process_monitoring_echo_message(self, sender, payload) -> None:
        pass

    # SOLVER PARENT DOMAIN
    def process_monitoring_credit_message(self, sender, payload) -> None:
        pass

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
    @staticmethod
    def get_current_time() -> float:
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

    ###############
    # Experiments #
    ###############
    def set_faulty_peer(self, peer_id: str) -> None:
        global FAULTY_PEER
        if not FAULTY_PEER:
            FAULTY_PEER = peer_id
            print("[" + format(self.get_current_time(), ".3f") + "] INFO ---> Peer:" + peer_id + " set as faulty peer")

    @staticmethod
    def is_faulty_peer(peer_id: str) -> bool:
        return FAULTY_PEER == peer_id

    @staticmethod
    def set_repeated_sn_r(sn_r: int, index: int, solver: HIDRAPeer) -> int:
        if sn_r == index > 0:
            solver.info.sn_r -= 1  # Previous sequence number
            return sn_r - 1  # Previous sequence number
        else:
            return sn_r
