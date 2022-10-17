import random
import time
from binascii import unhexlify

from hidra.caches import HIDRAEventCache
from hidra.payload import NewEventPayload, EventReplyPayload, VoteSolverPayload, EventSolvedPayload
from hidra.utils import get_peer_id, select_own_solver, get_event_solver
from pyipv8.ipv8.community import Community
from pyipv8.ipv8.lazy_community import lazy_wrapper
from pyipv8.ipv8.requestcache import RequestCache

# Cache prefixes
EVENT_PREFIX = "events"

# Testing
SENT = False


class HIDRACommunity(Community):
    """
    HIDRA community
    """

    community_id = unhexlify("2d606de41ee6595b2d3d5c57065b78bf17870f32")

    def __init__(self, my_peer, endpoint, network) -> None:
        super().__init__(my_peer, endpoint, network)

        # Overlay/community/cluster cache
        self.request_cache = RequestCache()

        # Attributes

        # Message handlers
        self.add_message_handler(1, self.on_new_event)
        self.add_message_handler(2, self.on_event_reply)
        self.add_message_handler(3, self.on_vote_solver)
        self.add_message_handler(4, self.on_solve_event)

    def init(self) -> None:
        # Testing
        global SENT
        if not SENT:
            SENT = True
            self.register_task("send_new_event", self.send_new_event, delay=0, interval=10.0)

    #########
    # Tasks #
    #########
    def send_new_event(self) -> None:
        # Payload data
        event_id = random.randint(1, 10 ** 3)
        my_usage = random.randint(1, 10 ** 6)
        container_id = random.randint(1, 10 ** 3)

        # Debug
        print("[" + get_peer_id(self.my_peer) + "] Sending NewEvent:")
        print("         ---> EID=" + str(event_id) + ", CID=" + str(container_id) + ", Usage=" + str(my_usage))

        # Update cache
        self.request_cache.add(
            HIDRAEventCache(self.request_cache,
                            EVENT_PREFIX,
                            event_id,
                            get_peer_id(self.my_peer),
                            container_id,
                            my_usage))

        # Send the NewEvent to each peer
        for peer in self.get_peers():
            self.ez_send(peer, NewEventPayload(event_id, container_id, my_usage))

    ####################
    # Message handlers #
    ####################
    @lazy_wrapper(NewEventPayload)
    def on_new_event(self, peer, payload) -> None:
        # Debug
        print("[" + get_peer_id(self.my_peer) + "] NewEvent received:")
        print("         ---> EID=" + str(payload.event_id) + ", CID=" + str(payload.container_id) +
              ", From=" + get_peer_id(peer) + ", Usage=" + str(payload.usage))

        # Payload data
        my_usage = random.randint(1, 10 ** 6)

        # Update cache
        self.request_cache.add(
            HIDRAEventCache(self.request_cache,
                            EVENT_PREFIX,
                            payload.event_id,
                            get_peer_id(peer),
                            payload.container_id,
                            payload.usage))

        cache = self.request_cache.get(EVENT_PREFIX, payload.event_id)
        cache.usages[get_peer_id(self.my_peer)] = my_usage

        # To avoid locks when the community has only two peers
        peers = self.get_peers()
        if len(peers) == 1:
            peers.append(self.my_peer)

        # Send a EventReply message to each peer
        for peer in peers:
            self.ez_send(peer, EventReplyPayload(payload.event_id, my_usage))

    @lazy_wrapper(EventReplyPayload)
    def on_event_reply(self, peer, payload) -> None:
        # Debug
        print("[" + get_peer_id(self.my_peer) + "] EventReply received:")
        print("         ---> EID=" + str(payload.event_id) + ", From=" + get_peer_id(peer) +
              ", Usage=" + str(payload.usage))

        # Update cache
        cache = self.request_cache.get(EVENT_PREFIX, payload.event_id)
        cache.usages[get_peer_id(peer)] = payload.usage

        # Check the number of EventReply/usages received
        if len(cache.usages) == self.get_required_replies():
            # Select my own best solver
            solver = select_own_solver(cache.usages)

            # Update cache
            cache.votes[get_peer_id(self.my_peer)] = solver

            # Send a VoteSolver message to each peer
            for peer in self.get_peers():
                self.ez_send(peer, VoteSolverPayload(payload.event_id, solver))

    @lazy_wrapper(VoteSolverPayload)
    def on_vote_solver(self, peer, payload) -> None:
        # Debug
        print("[" + get_peer_id(self.my_peer) + "] VoteSolver received:")
        print("         ---> EID=" + str(payload.event_id) + ", From=" + get_peer_id(peer) +
              ", Solver=" + payload.solver)

        # Update cache
        cache = self.request_cache.get(EVENT_PREFIX, payload.event_id)
        cache.votes[get_peer_id(peer)] = payload.solver

        # Get the elected event solver (if any)
        solver = get_event_solver(cache.votes, self.get_required_votes())

        # Am I the elected solver?
        if solver == get_peer_id(self.my_peer):
            # Debug
            print("[" + get_peer_id(self.my_peer) + "] Executing event tasks:")
            print("         ---> EID=" + str(payload.event_id) + ", CID=" + str(cache.container_id))

            # Executing event tasks
            # ...
            # DONE!

            # Update cache
            cache.solver = solver
            cache.end_time = time.time_ns()

            # Send a EventSolved message to each peer
            for peer in self.get_peers():
                self.ez_send(peer, EventSolvedPayload(payload.event_id))

            # Debug
            self.print_event(payload.event_id, cache)

    @lazy_wrapper(EventSolvedPayload)
    def on_solve_event(self, peer, payload) -> None:
        # Debug
        print("[" + get_peer_id(self.my_peer) + "] EventSolved received:")
        print("         ---> EID=" + str(payload.event_id) + ", From=" + get_peer_id(peer))

        # Update cache
        cache = self.request_cache.get(EVENT_PREFIX, payload.event_id)
        cache.solver = get_peer_id(peer)
        cache.end_time = time.time_ns()

        # Debug
        self.print_event(payload.event_id, cache)

    #############
    # Functions #
    #############
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

    def print_event(self, event_id, cache) -> None:
        print("[" + get_peer_id(self.my_peer) + "] State for EID=" + str(event_id) + ":")
        print("         ---> " + cache.applicant,
              cache.start_time,
              cache.container_id,
              cache.usages,
              cache.votes,
              cache.solver,
              cache.end_time)
