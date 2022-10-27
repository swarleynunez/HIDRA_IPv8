import random
import sys
import time
from binascii import unhexlify

from hidra.caches import HIDRANumberCache
from hidra.payload import NewEventPayload, EventReplyPayload, VoteSolverPayload, EventSolvedPayload, \
    EVENT_REPLY_MESSAGE, VOTE_SOLVER_MESSAGE, EVENT_SOLVER_MESSAGE, PeerOfferPayload
from hidra.settings import HIDRASettings
from hidra.types import HIDRAEvent, IPv8PendingMessage, HIDRAPeer, HIDRAContainer
from hidra.utils import get_peer_id, select_own_solver, get_event_solver
from pyipv8.ipv8.community import Community
from pyipv8.ipv8.lazy_community import lazy_wrapper
from pyipv8.ipv8.requestcache import RequestCache

# Cache prefixes
EVENT_PREFIX = "HIDRA_events"
CONTAINER_PREFIX = "HIDRA_containers"
MESSAGES_PREFIX = "IPv8_messages"

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

        # Overlay/community/cluster cache
        self.request_cache = RequestCache()

        # Testing
        self.po_msg_count = 0
        self.e_count = 0
        self.ne_msg_count = 0
        self.er_msg_count = 0
        self.vs_msg_count = 0
        self.es_msg_count = 0
        self.free_rider = None

        # Set the initial peer state
        self.initialize_peer()

        # Message handlers
        self.add_message_handler(1, self.on_peer_offer)
        self.add_message_handler(2, self.on_new_event)
        self.add_message_handler(3, self.on_event_reply)
        self.add_message_handler(4, self.on_vote_solver)
        self.add_message_handler(5, self.on_solve_event)

        # Register an asyncio task with the community
        # This ensures that the task ends when the community is unloaded
        self.register_task("send_peer_offer", self.send_peer_offer, delay=1)
        self.register_task("send_new_event", self.send_new_event, delay=3, interval=1)

    async def unload(self) -> None:
        await self.request_cache.shutdown()
        await super().unload()

    ####################
    # Message handlers #
    ####################
    @lazy_wrapper(PeerOfferPayload)
    def on_peer_offer(self, peer, payload) -> None:
        # Debug
        self.po_msg_count += 1

        # Update storage
        peer_id = get_peer_id(peer)
        if not self.exist_peer(peer_id):
            self.peers[peer_id] = HIDRAPeer(payload.max_usage)

    @lazy_wrapper(NewEventPayload)
    def on_new_event(self, peer, payload) -> None:
        self.process_new_event_message(get_peer_id(peer), payload)

    @lazy_wrapper(EventReplyPayload)
    def on_event_reply(self, peer, payload) -> None:
        self.process_pending_messages()
        self.process_event_reply_message(get_peer_id(peer), payload)

    @lazy_wrapper(VoteSolverPayload)
    def on_vote_solver(self, peer, payload) -> None:
        self.process_pending_messages()
        self.process_vote_solver_message(get_peer_id(peer), payload)

    @lazy_wrapper(EventSolvedPayload)
    def on_solve_event(self, peer, payload) -> None:
        self.process_pending_messages()
        self.process_event_solved_message(get_peer_id(peer), payload)

    ########
    # Init #
    ########
    def initialize_peer(self):
        self.set_peer_offer()
        self.initialize_containers()

        # Experiments
        self.set_free_rider()

    def set_peer_offer(self) -> None:
        """
        Set the amount of resources that the peer offers to the cluster
        """

        # Payload data
        peer_id = get_peer_id(self.my_peer)
        max_usage = random.randint(10 ** (HIDRASettings.usage_size - 1), 10 ** HIDRASettings.usage_size)

        # Update storage
        self.peers[peer_id] = HIDRAPeer(max_usage)

    def initialize_containers(self) -> None:
        for _ in range(HIDRASettings.initial_container_count):
            # Randomize container ID
            while True:
                container_id = random.randint(1, 10 ** HIDRASettings.object_id_size)
                if not self.exist_container(container_id):
                    break

            # Update storage
            image_tags = ["nginx", "postgres", "hello-world", "busybox", "ubuntu"]
            self.containers[container_id] = HIDRAContainer(random.choice(image_tags))
            self.containers[container_id].host = get_peer_id(self.my_peer)

    ############
    # Messages #
    ############
    def send_peer_offer(self) -> None:
        # Payload data
        peer_id = get_peer_id(self.my_peer)
        max_usage = self.peers[peer_id].max_usage

        # Send a PeerOffer message to each peer
        for peer in self.get_peers():
            self.ez_send(peer, PeerOfferPayload(peer_id, max_usage))

    def send_new_event(self) -> None:
        # Payload data
        while True:
            event_id = random.randint(1, 10 ** HIDRASettings.object_id_size)
            if not self.exist_event(event_id):
                break
        if FREE_RIDER == get_peer_id(self.my_peer):
            my_usage = self.fake_usage()
        else:
            while True:
                my_usage = random.randint(1, 10 ** (HIDRASettings.usage_size - 1))
                if my_usage <= self.peers[get_peer_id(self.my_peer)].max_usage:
                    break
        container_id = self.select_container()
        if not container_id:
            return

        # Debug
        self.e_count += 1
        # print("[" + get_peer_id(self.my_peer) + "] Sending event ---> EID=" + str(event_id) +
        # ", CID=" + str(container_id) + ", Usage=" + str(my_usage))

        # Update storage and cache
        self.events[event_id] = HIDRAEvent(get_peer_id(self.my_peer), container_id, my_usage)
        self.request_cache.add(HIDRANumberCache(self.request_cache, EVENT_PREFIX, event_id))

        # Send a NewEvent message to each peer
        for peer in self.get_peers():
            image_tag = self.containers[container_id].image_tag
            self.ez_send(peer, NewEventPayload(event_id, container_id, image_tag, my_usage))

    def process_new_event_message(self, sender, payload) -> None:
        # Requirements
        if self.exist_event(payload.event_id):
            sys.exit("Repeated EID between peers. Exiting...")
        if self.exist_container(payload.container_id):
            sys.exit("Repeated CID between peers. Exiting...")

        # Debug
        self.ne_msg_count += 1
        # print("[" + get_peer_id(self.my_peer) + "] NewEvent received ---> EID=" + str(payload.event_id) +
        # ", CID=" + str(payload.container_id) + ", Image=" + payload.container_image_tag +
        # ", From=" + sender + ", Usage=" + str(payload.usage))

        # Payload data
        if FREE_RIDER == get_peer_id(self.my_peer):
            my_usage = self.fake_usage()
        else:
            while True:
                my_usage = random.randint(1, 10 ** (HIDRASettings.usage_size - 1))
                if my_usage <= self.peers[get_peer_id(self.my_peer)].max_usage:
                    break

        # Update storage and cache
        event = self.events[payload.event_id] = HIDRAEvent(sender, payload.container_id, payload.usage)
        event.usages[get_peer_id(self.my_peer)] = my_usage
        self.containers[payload.container_id] = HIDRAContainer(payload.container_image_tag)
        self.request_cache.add(HIDRANumberCache(self.request_cache, EVENT_PREFIX, payload.event_id))

        # Send a EventReply message to each peer
        for peer in self.get_peers():
            self.ez_send(peer, EventReplyPayload(payload.event_id, my_usage))

    def process_event_reply_message(self, sender, payload) -> None:
        # Requirements
        if not self.exist_event(payload.event_id):
            self.add_pending_message(sender, payload)
            return
        if self.has_already_replied(payload.event_id, sender):
            # Dismissing the message...
            return
        if self.has_required_replies(payload.event_id):
            # Dismissing the message...
            return

        # Debug
        self.er_msg_count += 1
        # print("[" + get_peer_id(self.my_peer) + "] EventReply received ---> EID=" + str(payload.event_id) +
        # ", From=" + sender + ", Usage=" + str(payload.usage))

        # Update storage
        event = self.events[payload.event_id]
        event.usages[sender] = payload.usage

        # Check the number of EventReply/usages received
        if self.has_required_replies(payload.event_id):
            # Select my own best solver
            solver = select_own_solver(event.usages)

            # Update storage
            event.votes[get_peer_id(self.my_peer)] = solver

            # Send a VoteSolver message to each peer
            for peer in self.get_peers():
                self.ez_send(peer, VoteSolverPayload(payload.event_id, solver))

    def process_vote_solver_message(self, sender, payload) -> None:
        # Requirements
        if not (self.exist_event(payload.event_id) and
                self.has_already_replied(payload.event_id, sender) and
                self.has_required_replies(payload.event_id)):
            self.add_pending_message(sender, payload)
            return
        if self.has_already_voted(payload.event_id, sender):
            # Dismissing the message...
            return
        if self.has_required_votes(payload.event_id):
            # Dismissing the message...
            return

        # Debug
        self.vs_msg_count += 1
        # print("[" + get_peer_id(self.my_peer) + "] VoteSolver received ---> EID=" + str(payload.event_id) +
        # ", From=" + sender + ", Solver=" + payload.solver)

        # Update storage
        event = self.events[payload.event_id]
        event.votes[sender] = payload.solver

        # Get the elected event solver (if any)
        solver = get_event_solver(event.votes, self.get_required_votes())

        # Am I the elected solver?
        if solver == get_peer_id(self.my_peer):
            # Debug
            # print("[" + get_peer_id(self.my_peer) + "] Executing event tasks ---> EID=" + str(payload.event_id) +
            # ", CID=" + str(event.container_id))

            # Executing event tasks
            # ...
            # DONE!

            # Update storage and cache
            event.solver = solver
            event.end_time = time.time_ns()
            self.request_cache.pop(EVENT_PREFIX, payload.event_id)

            # Am I the event applicant?
            if event.applicant == get_peer_id(self.my_peer):
                self.request_cache.pop(CONTAINER_PREFIX, event.container_id)
            else:
                self.containers[event.container_id].host = get_peer_id(self.my_peer)

            # Testing
            self.containers[event.container_id].migrated = True

            # Send a EventSolved message to each peer
            for peer in self.get_peers():
                self.ez_send(peer, EventSolvedPayload(payload.event_id))

    def process_event_solved_message(self, sender, payload) -> None:
        # Requirements
        if not (self.exist_event(payload.event_id) or
                self.has_already_replied(payload.event_id, sender) or
                self.has_required_replies(payload.event_id) or
                self.has_already_voted(payload.event_id, sender) or
                self.has_required_votes(payload.event_id) or
                self.can_solve_event(payload.event_id, sender)):
            self.add_pending_message(sender, payload)
            return
        if self.is_event_solved(payload.event_id):
            # Dismissing the message...
            return

        # Debug
        self.es_msg_count += 1
        # print("[" + get_peer_id(self.my_peer) + "] EventSolved received ---> EID=" + str(payload.event_id) +
        # ", From=" + sender)

        # Update storage and cache
        event = self.events[payload.event_id]
        event.solver = sender
        event.end_time = time.time_ns()
        self.containers[event.container_id].host = sender
        self.request_cache.pop(EVENT_PREFIX, payload.event_id)

        # Am I the event applicant?
        if event.applicant == get_peer_id(self.my_peer):
            self.request_cache.pop(CONTAINER_PREFIX, event.container_id)

        # Testing
        self.containers[event.container_id].migrated = True

    def add_pending_message(self, sender, payload) -> None:
        while True:
            message_id = random.randint(1, 10 ** HIDRASettings.object_id_size)
            if not self.exist_message(message_id):
                break

        # Update storage and cache
        self.messages[message_id] = IPv8PendingMessage(sender, payload)
        self.request_cache.add(HIDRANumberCache(self.request_cache, MESSAGES_PREFIX, message_id))

    def process_pending_messages(self):
        for k, v in list(self.messages.items()):
            # Update storage and cache
            self.messages.pop(k)
            self.request_cache.pop(MESSAGES_PREFIX, k)

            if v.payload.msg_id == EVENT_REPLY_MESSAGE:
                # Process pending 'EventReply' message
                self.process_event_reply_message(v.sender, v.payload)
            elif v.payload.msg_id == VOTE_SOLVER_MESSAGE:
                # Process pending 'VoteSolver' message
                self.process_vote_solver_message(v.sender, v.payload)
            elif v.payload.msg_id == EVENT_SOLVER_MESSAGE:
                # Process pending 'EventSolved' message
                self.process_event_solved_message(v.sender, v.payload)

    ##############
    # Containers #
    ##############
    def select_container(self) -> int:
        # Selecting the first container not used by an event
        for k, v in self.containers.items():
            if v.host == get_peer_id(self.my_peer) \
                    and not v.migrated \
                    and not self.request_cache.has(CONTAINER_PREFIX, k):
                self.request_cache.add(HIDRANumberCache(self.request_cache, CONTAINER_PREFIX, k))
                return k

    ###########
    # Getters #
    ###########
    def get_required_replies(self) -> int:
        """
        Number of required replies before selecting a solver
        """

        return len(self.get_peers()) + 1

    def get_required_votes(self) -> int:
        """
        Number of required votes to establish the solver
        """

        return len(self.get_peers()) + 1

    ############
    # Checkers #
    ############
    def exist_peer(self, peer_id) -> bool:
        return peer_id in self.peers

    def exist_event(self, event_id) -> bool:
        return event_id in self.events

    def has_already_replied(self, event_id, peer_id) -> bool:
        return peer_id in self.events[event_id].usages

    def has_required_replies(self, event_id) -> bool:
        return len(self.events[event_id].usages) == self.get_required_replies()

    def has_already_voted(self, event_id, peer_id) -> bool:
        return peer_id in self.events[event_id].votes

    def has_required_votes(self, event_id) -> bool:
        return len(self.events[event_id].votes) == self.get_required_votes()

    def can_solve_event(self, event_id, peer_id) -> bool:
        return peer_id == get_event_solver(self.events[event_id].votes, self.get_required_votes())

    def is_event_solved(self, event_id) -> bool:
        return True if self.events[event_id].solver else False

    def exist_container(self, container_id) -> bool:
        return container_id in self.containers

    def exist_message(self, message_id) -> bool:
        return message_id in self.messages

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

    def fake_usage(self) -> int:
        if HIDRASettings.free_riding_type == "over":
            return self.peers[get_peer_id(self.my_peer)].max_usage
        elif HIDRASettings.free_riding_type == "under":
            return 1
        else:
            while True:
                my_usage = random.randint(1, 10 ** (HIDRASettings.usage_size - 1))
                if my_usage <= self.peers[get_peer_id(self.my_peer)].max_usage:
                    return my_usage
