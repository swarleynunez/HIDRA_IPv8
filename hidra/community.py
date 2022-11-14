import random
import time
from binascii import unhexlify

from hidra.caches import HIDRANumberCache
from hidra.payload import PeerInitPayload, NewEventPayload, EventReplyPayload, EventCommitPayload, EventCreditPayload, \
    EventDiscoveryPayload, PEER_INIT_MESSAGE, NEW_EVENT_MESSAGE, EVENT_REPLY_MESSAGE, EVENT_COMMIT_MESSAGE, \
    EVENT_CREDIT_MESSAGE, EVENT_DISCOVERY_MESSAGE
from hidra.settings import HIDRASettings
from hidra.types import HIDRAEvent, IPv8PendingMessage, HIDRAPeer, HIDRAContainer
from hidra.utils import get_peer_id, get_event_solver, get_object_id, sign_data, verify_sign, select_own_solver
from pyipv8.ipv8.community import Community
from pyipv8.ipv8.keyvault.crypto import default_eccrypto
from pyipv8.ipv8.lazy_community import lazy_wrapper
from pyipv8.ipv8.peer import Peer
from pyipv8.ipv8.requestcache import RequestCache

# Cache prefixes
EVENT_PREFIX = "HIDRA_events"
CONTAINER_PREFIX = "HIDRA_containers"
MESSAGES_PREFIX = "IPv8_messages"

# Global variables
CONTAINER_IMAGE_TAGS = ["nginx", "postgres", "hello-world", "busybox", "ubuntu"]

# Experiments
FREE_RIDER = None


class HIDRACommunity(Community):
    """
    HIDRA community
    """

    community_id = unhexlify("2d606de41ee6595b2d3d5c57065b78bf17870f32")

    def __init__(self, my_peer, endpoint, network) -> None:
        super().__init__(my_peer, endpoint, network)

        # Attributes
        self.peers = {}
        self.events = {}
        self.containers = {}
        self.messages = {}
        self.event_sn = 0  # Sequence number
        self.container_sn = 0  # Sequence number
        self.message_sn = 0  # Sequence number

        # Overlay/community/cluster cache
        self.request_cache = RequestCache()

        # Testing
        self.pi_msg_count = 0
        self.e_count = 0
        self.ne_msg_count = 0
        self.er_msg_count = 0
        self.eco_msg_count = 0
        self.ecr_msg_count = 0
        self.ed_msg_count = 0

        # Experiments
        self.free_rider = None
        self.ex1_containers = []

        # Set the initial peer state
        self.initialize_peer()

        # Message handlers
        self.add_message_handler(PEER_INIT_MESSAGE, self.on_peer_init)
        self.add_message_handler(NEW_EVENT_MESSAGE, self.on_new_event)
        self.add_message_handler(EVENT_REPLY_MESSAGE, self.on_event_reply)
        self.add_message_handler(EVENT_COMMIT_MESSAGE, self.on_event_commit)
        self.add_message_handler(EVENT_CREDIT_MESSAGE, self.on_event_credit)
        self.add_message_handler(EVENT_DISCOVERY_MESSAGE, self.on_event_discovery)

        # Register an asyncio task with the community
        # This ensures that the task ends when the community is unloaded
        self.register_task("send_peer_offer", self.send_peer_offer, delay=1)
        self.register_task("send_new_event", self.send_new_event, delay=5, interval=1)

    ########
    # Peer #
    ########
    def initialize_peer(self) -> None:
        # Update storage
        public_key = self.my_peer.public_key
        max_usage = random.randint(10 ** (HIDRASettings.usage_size - 1), 10 ** HIDRASettings.usage_size)
        self.peers[get_peer_id(self.my_peer)] = HIDRAPeer(public_key, max_usage)

        # Any initial container to deploy?
        self.initialize_containers()

        # Experiments
        self.set_free_rider()

    def initialize_containers(self) -> None:
        my_peer_id = get_peer_id(self.my_peer)

        for _ in range(HIDRASettings.initial_containers):
            # Get the next container ID
            container_id = get_object_id(my_peer_id, self.container_sn)
            self.container_sn += 1

            # Update storage
            self.containers[container_id] = HIDRAContainer(random.choice(CONTAINER_IMAGE_TAGS))
            self.containers[container_id].host = my_peer_id

        # Experiments
        self.ex1_containers.append(HIDRASettings.initial_containers)

    def get_fanout_peers(self, applicant_peer_id: str) -> [Peer]:
        my_peer_id = get_peer_id(self.my_peer)

        # Convert IPv8 peers to HIDRA peers
        peer_ids = []
        for peer in self.get_peers():
            peer_ids.append(get_peer_id(peer))

        # Select peers from the applicant perspective
        if my_peer_id != applicant_peer_id:
            peer_ids.remove(applicant_peer_id)
            peer_ids.append(my_peer_id)

        # Sort array of HIDRA peers
        peer_ids.sort()

        # Select a deterministic random peer fanout
        if HIDRASettings.max_fanout <= len(self.get_peers()) + 1:
            fanout = HIDRASettings.max_fanout
        else:
            fanout = len(self.get_peers()) + 1
        random.seed(applicant_peer_id)
        my_peer_ids = random.sample(peer_ids, k=fanout - 1)
        random.seed()  # Default timestamp seed

        # Select the applicant peer
        if my_peer_id != applicant_peer_id:
            my_peer_ids.append(applicant_peer_id)

        # Convert HIDRA peers to IPv8 peers
        my_peers = []
        for peer in self.get_peers():
            if get_peer_id(peer) in my_peer_ids:
                my_peers.append(peer)

        # Select myself
        my_peers.append(self.my_peer)

        return my_peers

    def get_peer_from_id(self, peer_id) -> Peer:
        if get_peer_id(self.my_peer) == peer_id:
            return self.my_peer
        else:
            for peer in self.get_peers():
                if get_peer_id(peer) == peer_id:
                    return peer

    async def unload(self) -> None:
        await self.request_cache.shutdown()
        await super().unload()

    ############
    # Messages #
    ############
    @lazy_wrapper(PeerInitPayload)
    def on_peer_init(self, peer, payload) -> None:
        # Debug
        self.pi_msg_count += 1

        # Update storage
        public_key = default_eccrypto.key_from_public_bin(payload.public_key)
        self.peers[get_peer_id(peer)] = HIDRAPeer(public_key, payload.max_usage)

    @lazy_wrapper(NewEventPayload)
    def on_new_event(self, peer, payload) -> None:
        self.process_new_event_message(get_peer_id(peer), payload)

    @lazy_wrapper(EventReplyPayload)
    def on_event_reply(self, peer, payload) -> None:
        self.process_pending_messages()
        self.process_event_reply_message(get_peer_id(peer), payload)

    @lazy_wrapper(EventCommitPayload)
    def on_event_commit(self, peer, payload) -> None:
        self.process_pending_messages()
        self.process_event_commit_message(get_peer_id(peer), payload)

    @lazy_wrapper(EventCreditPayload)
    def on_event_credit(self, peer, payload) -> None:
        self.process_pending_messages()
        self.process_event_credit_message(get_peer_id(peer), payload)

    @lazy_wrapper(EventDiscoveryPayload)
    def on_event_discovery(self, peer, payload) -> None:
        self.process_event_discovery_message(get_peer_id(peer), payload)

    def send_peer_offer(self) -> None:
        my_peer_id = get_peer_id(self.my_peer)

        # Payload data
        bin_pk = default_eccrypto.key_to_bin(self.peers[my_peer_id].public_key)
        max_usage = self.peers[my_peer_id].max_usage

        # Send a PeerInit message to all peers
        for peer in self.get_peers():
            self.ez_send(peer, PeerInitPayload(bin_pk, max_usage))

    def send_new_event(self) -> None:
        my_peer_id = get_peer_id(self.my_peer)

        # Payload data
        # container_id = self.select_container()
        # if not container_id:
        #     return

        # Get the next container ID
        container_id = get_object_id(my_peer_id, self.container_sn)
        self.container_sn += 1
        image_tag = random.choice(CONTAINER_IMAGE_TAGS)

        # Get the next event ID
        event_id = get_object_id(my_peer_id, self.event_sn)
        self.event_sn += 1

        # Debug
        # print("[" + my_peer_id + "] Sending event ---> EID=" + str(event_id) + ", CID=" + str(container_id))

        # Send a NewEvent/PREPARE message to my peers
        for peer in self.get_fanout_peers(my_peer_id):
            self.ez_send(peer, NewEventPayload(event_id, container_id, image_tag))

    def process_new_event_message(self, sender_id, payload) -> None:
        # Requirements
        if self.exist_event(payload.event_id):
            # Dismissing the message...
            return

        # Debug
        self.ne_msg_count += 1
        # print("[" + get_peer_id(self.my_peer) + "] NewEvent received ---> EID=" +
        #       str(payload.event_id) + ", From=" + get_peer_id(sender))

        # Payload data
        # Experiments
        if FREE_RIDER == get_peer_id(self.my_peer):
            usage_offer = self.fake_usage_offer()
        else:
            usage_offer = random.randint(0, 10 ** (HIDRASettings.usage_size - 1))

        # TODO: reputation burning
        reputation_offer = 0

        # Update storage and cache
        self.events[payload.event_id] = HIDRAEvent(sender_id, payload.container_id)
        if not self.exist_container(payload.container_id):
            self.containers[payload.container_id] = HIDRAContainer(payload.container_image_tag)
        self.request_cache.add(HIDRANumberCache(self.request_cache, EVENT_PREFIX, payload.event_id))

        # Format and sign event data
        data = str(payload.event_id) + "," + \
               str(usage_offer) + "," + \
               str(reputation_offer)
        signature = sign_data(self.my_peer.key, data)

        # Send an EventReply/ACK message to the applicant peer
        self.ez_send(self.get_peer_from_id(sender_id),
                     EventReplyPayload(payload.event_id, usage_offer, reputation_offer, signature))

    # Executed by applicant peers
    def process_event_reply_message(self, sender_id, payload) -> None:
        # Requirements
        if not self.exist_event(payload.event_id):
            self.add_pending_message(sender_id, payload)
            return
        event = self.events[payload.event_id]
        if len(event.ack_signatures) == self.get_required_replies():
            # Dismissing the message...
            return
        # Verify NewEvent/PREPARE payload signature
        data = str(payload.event_id) + "," + \
               str(payload.usage_offer) + "," + \
               str(payload.reputation_offer)
        if not verify_sign(self.peers[sender_id].public_key, data, payload.signature):
            # Dismissing the message...
            return

        # Debug
        self.er_msg_count += 1
        # print("[" + get_peer_id(self.my_peer) + "] EventReply received ---> EID=" +
        #       str(payload.event_id) + ", From=" + sender_id)

        # Update storage
        event.usage_offers[sender_id] = payload.usage_offer
        event.reputation_offers[sender_id] = payload.reputation_offer
        event.ack_signatures[sender_id] = str(payload.signature, "latin1")  # Due to JSON bytes serialization

        # Send all EventReply/ACKs to my peers (COMMIT message)
        if len(event.ack_signatures) == self.get_required_replies():
            for peer in self.get_fanout_peers(event.applicant):
                self.ez_send(peer, EventCommitPayload(payload.event_id,
                                                      event.usage_offers,
                                                      event.reputation_offers,
                                                      event.ack_signatures))

    def process_event_commit_message(self, sender_id, payload) -> None:
        # Requirements
        if not self.exist_event(payload.event_id):
            self.add_pending_message(sender_id, payload)
            return
        if len(payload.ack_signatures) != self.get_required_replies():
            # Dismissing the message...
            return
        # Verify NewEvent/PREPARE payload signatures
        event = self.events[payload.event_id]
        for k, v in payload.ack_signatures.items():
            data = str(payload.event_id) + "," + \
                   str(payload.usage_offers[k]) + "," + \
                   str(payload.reputation_offers[k])
            if not verify_sign(self.peers[k].public_key, data, bytes(v, "latin1")):
                # Dismissing the message...
                return

        # Debug
        self.eco_msg_count += 1
        # print("[" + get_peer_id(self.my_peer) + "] EventCommit received ---> EID=" +
        #       str(payload.event_id) + ", From=" + get_peer_id(sender))

        # Update storage
        event.usage_offers = payload.usage_offers
        event.reputation_offers = payload.reputation_offers
        event.ack_signatures = payload.ack_signatures

        # Select the event solver
        solver = select_own_solver(event.usage_offers, event.reputation_offers)

        # TODO: settle reputations
        self.peers[solver].reputation -= event.reputation_offers[solver]
        # ...
        # And for the other event peers?

        # Am I the event solver?
        execution_result = 0
        if solver == get_peer_id(self.my_peer):
            # Execute container synchronously
            execution_result = self.execute_container(event.container_id)

        # Format and sign approval data
        data = str(payload.event_id) + "," + \
               solver + "," + \
               str(execution_result)
        signature = sign_data(self.my_peer.key, data)

        # Send an EventCredit/CREDIT message to the applicant peer
        self.ez_send(self.get_peer_from_id(sender_id),
                     EventCreditPayload(payload.event_id, solver, execution_result, signature))

    # Executed by applicant peers
    def process_event_credit_message(self, sender_id, payload) -> None:
        # Requirements
        if not self.exist_event(payload.event_id):
            self.add_pending_message(sender_id, payload)
            return
        event = self.events[payload.event_id]
        solver = get_event_solver(event.votes, self.get_required_votes())
        if solver and self.has_already_voted(payload.event_id, solver):
            # Dismissing the message...
            return
        # Verify EventCredit/CREDIT payload signature
        data = str(payload.event_id) + "," + \
               payload.solver + "," + \
               str(payload.execution_result)
        if not verify_sign(self.peers[sender_id].public_key, data, payload.signature):
            # Dismissing the message...
            return

        # Debug
        self.ecr_msg_count += 1
        # print("[" + get_peer_id(self.my_peer) + "] EventCredit received ---> EID=" +
        #       str(payload.event_id) + ", From=" + sender_id)

        # Update storage
        event.votes[sender_id] = payload.solver
        event.credit_signatures[sender_id] = str(payload.signature, "latin1")  # Due to JSON bytes serialization
        event.execution_results[sender_id] = payload.execution_result

        # Waiting for the solver and f + 1 EventCredit/CREDIT messages (final certificate)
        solver = get_event_solver(event.votes, self.get_required_votes())
        if solver and self.has_already_voted(payload.event_id, solver):
            # Debug
            self.e_count += 1
            # print("[" + get_peer_id(self.my_peer) + "] Executed container ---> CID=" +
            #       str(event.container_id), str(event.execution_results[solver]))

            # Update storage and cache
            event.end_time = time.time_ns()
            self.containers[event.container_id].host = solver
            self.request_cache.pop(EVENT_PREFIX, payload.event_id)
            if self.request_cache.has(CONTAINER_PREFIX, event.container_id):
                self.request_cache.pop(CONTAINER_PREFIX, event.container_id)

            # Optional. Send the whole event to other peers
            # Sending the event only to my fanout
            # for peer in self.get_fanout_peers(event.applicant):
            #    self.ez_send(peer, EventDiscoveryPayload(payload.event_id, event))

    def process_event_discovery_message(self, sender_id, payload) -> None:
        # Requirements
        if len(payload.event["ack_signatures"]) != self.get_required_replies():
            # Dismissing the message...
            return
        # Verify NewEvent/PREPARE payload signatures
        for k, v in payload.event["ack_signatures"].items():
            data = str(payload.event_id) + "," + \
                   str(payload.event["usage_offers"][k]) + "," + \
                   str(payload.event["reputation_offers"][k])
            if not verify_sign(self.peers[k].public_key, data, bytes(v, "latin1")):
                # Dismissing the message...
                return
        if not get_event_solver(payload.event["votes"], self.get_required_votes()):
            # Dismissing the message...
            return
        # Verify EventCredit/CREDIT payload signatures
        for k, v in payload.event["credit_signatures"].items():
            data = str(payload.event_id) + "," + \
                   payload.event["votes"][k] + "," + \
                   str(payload.event["execution_results"][k])
            if not verify_sign(self.peers[k].public_key, data, bytes(v, "latin1")):
                # Dismissing the message...
                return

        # Debug
        self.ed_msg_count += 1
        # print("[" + get_peer_id(self.my_peer) + "] EventDelivery received ---> EID=" +
        #       str(payload.event_id) + ", From=" + sender_id)

        # Update storage and cache
        if self.exist_event(payload.event_id):
            if get_peer_id(self.my_peer) != sender_id:
                event = self.events[payload.event_id]
                event.votes = payload.event["votes"]
                event.credit_signatures = payload.event["credit_signatures"]
                event.execution_results = payload.event["execution_results"]
                event.end_time = payload.event["end_time"]
                self.containers[event.container_id].host = get_event_solver(event.votes, self.get_required_votes())
                self.request_cache.pop(EVENT_PREFIX, payload.event_id)
                if self.request_cache.has(CONTAINER_PREFIX, event.container_id):
                    self.request_cache.pop(CONTAINER_PREFIX, event.container_id)
        else:
            pass

    def add_pending_message(self, sender_id, payload) -> None:
        # Get the next message ID
        message_id = get_object_id(get_peer_id(self.my_peer), self.message_sn)
        self.message_sn += 1

        # Update storage and cache
        self.messages[message_id] = IPv8PendingMessage(sender_id, payload)
        self.request_cache.add(HIDRANumberCache(self.request_cache, MESSAGES_PREFIX, message_id))

    def process_pending_messages(self):
        for k, v in list(self.messages.items()):
            # Update storage and cache
            self.messages.pop(k)
            self.request_cache.pop(MESSAGES_PREFIX, k)

            if v.payload.msg_id == EVENT_REPLY_MESSAGE:
                # Process pending 'EventReply' message
                self.process_event_reply_message(v.sender, v.payload)
            elif v.payload.msg_id == EVENT_COMMIT_MESSAGE:
                # Process pending 'EventCommit' message
                self.process_event_commit_message(v.sender, v.payload)
            elif v.payload.msg_id == EVENT_CREDIT_MESSAGE:
                # Process pending 'EventCommit' message
                self.process_event_credit_message(v.sender, v.payload)

    ##########
    # Events #
    ##########
    @staticmethod
    def get_required_replies() -> int:
        """
        Number of required replies/ACKs in events
        """

        return 2 * HIDRASettings.faulty_peers + 1

    @staticmethod
    def get_required_votes() -> int:
        """
        Number of required votes/CREDITs in events
        """

        return HIDRASettings.faulty_peers + 1

    ##############
    # Containers #
    ##############
    def select_container(self) -> int:
        # Selecting a random own container not used by an event
        selectable = []
        for k, v in self.containers.items():
            if v.host == get_peer_id(self.my_peer) and not self.request_cache.has(CONTAINER_PREFIX, k):
                selectable.append(k)

        if len(selectable) > 0:
            container_id = random.choice(selectable)
            self.request_cache.add(HIDRANumberCache(self.request_cache, CONTAINER_PREFIX, container_id))
            return container_id

    @staticmethod
    def execute_container(container_id) -> int:
        # Debug
        # print("[" + get_peer_id(self.my_peer) + "] Executing container ---> CID=" + str(container_id))
        return container_id

    ############
    # Checkers #
    ############
    def exist_event(self, event_id) -> bool:
        return event_id in self.events

    def has_already_voted(self, event_id, peer_id) -> bool:
        return peer_id in self.events[event_id].votes

    def exist_container(self, container_id) -> bool:
        return container_id in self.containers

    ###############
    # Experiments #
    ###############
    def set_free_rider(self) -> None:
        # Choose the first peer as the free-rider
        global FREE_RIDER
        if HIDRASettings.enable_free_rider and not FREE_RIDER:
            FREE_RIDER = get_peer_id(self.my_peer)

        # Testing
        self.free_rider = FREE_RIDER

    def fake_usage_offer(self) -> int:
        if HIDRASettings.free_riding_type == "over":
            return self.peers[get_peer_id(self.my_peer)].max_usage
        elif HIDRASettings.free_riding_type == "under":
            return 0
        else:
            return random.randint(0, 10 ** (HIDRASettings.usage_size - 1))
