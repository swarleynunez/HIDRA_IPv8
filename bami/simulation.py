import asyncio
import logging
import os
import shutil
import time
from asyncio import sleep
from typing import Optional

import yappi

from hidra.community import HIDRACommunity
from pyipv8.ipv8.messaging.interfaces.statistics_endpoint import StatisticsEndpoint
from pyipv8.ipv8.configuration import ConfigBuilder
from pyipv8.ipv8.taskmanager import TaskManager
from pyipv8.ipv8_service import IPv8

from pyipv8.simulation.discrete_loop import DiscreteLoop
from pyipv8.simulation.simulation_endpoint import SimulationEndpoint

from bami.settings import SimulationSettings
from hidra.utils import get_peer_id


class BamiSimulation(TaskManager):
    """
    The main logic to run simulations with the various algorithms included in BAMI.
    To create your own simulation, you should subclass the BamiSimulation class and override the get_ipv8_builder
    method to load custom communities. One can override on_simulation_finished to parse data after the simulation
    is finished.
    One should pass a SimulationSettings object when initializing this class. This object contains various settings
    related to the simulation, for example, the number of peers.
    Each experiment will write data to a subdirectory in the data directory. The name of this subdirectory depends
    on the simulation settings.
    """
    MAIN_OVERLAY: Optional[str] = "HIDRACommunity"

    def __init__(self, settings: SimulationSettings) -> None:
        super().__init__()
        self.settings = settings
        self.nodes = []
        self.logger = logging.getLogger(self.__class__.__name__)
        dir_name = "n_%d" % self.settings.peers if not self.settings.identifier else \
            "n_%d_%s" % (self.settings.peers, self.settings.identifier)
        if self.settings.name:
            dir_name = "%s_%s" % (dir_name, self.settings.name)
        self.data_dir = os.path.join("data", dir_name)

        self.loop = DiscreteLoop()
        asyncio.set_event_loop(self.loop)

    def get_ipv8_builder(self, peer_id: int) -> ConfigBuilder:
        builder = ConfigBuilder().clear_keys().clear_overlays()
        builder.add_key("my peer", "curve25519", os.path.join(self.data_dir, f"ec{peer_id}.pem"))
        return builder

    async def start_ipv8_nodes(self) -> None:
        for peer_id in range(1, self.settings.peers + 1):
            if peer_id % 100 == 0:
                print("Created %d peers..." % peer_id)

            endpoint = SimulationEndpoint()
            config = self.get_ipv8_builder(peer_id)
            config.set_log_level(self.settings.logging_level)
            instance = IPv8(config.finalize(), endpoint_override=endpoint,
                            extra_communities={'HIDRACommunity': HIDRACommunity})

            if self.settings.enable_community_statistics:
                instance.endpoint = StatisticsEndpoint(endpoint)

            await instance.start()

            if not self.settings.enable_ipv8_ticker:
                # Disable the IPv8 ticker
                instance.state_machine_task.cancel()

            # Set the WAN address of the peer to the address of the endpoint
            for overlay in instance.overlays:
                overlay.max_peers = -1
                overlay.my_peer.address = instance.overlays[0].endpoint.wan_address
                overlay.my_estimated_wan = instance.overlays[0].endpoint.wan_address

            # If we have a main overlay set, find it and assign it to the overlay attribute
            instance.overlay = None
            if self.MAIN_OVERLAY:
                for overlay in instance.overlays:
                    if overlay.__class__.__name__ == self.MAIN_OVERLAY:
                        instance.overlay = overlay
                        break

            if self.settings.enable_community_statistics:
                for overlay in instance.overlays:
                    overlay.endpoint = instance.endpoint
                    instance.endpoint.enable_community_statistics(overlay.get_prefix(), True)

            self.nodes.append(instance)

    def setup_directories(self) -> None:
        if os.path.exists(self.data_dir):
            shutil.rmtree(self.data_dir)
        os.makedirs(self.data_dir, exist_ok=True)

    async def ipv8_discover_peers(self) -> None:
        for node_a in self.nodes:
            # connect_nodes = random.sample(self.nodes, min(100, len(self.nodes)))
            connect_nodes = self.nodes
            for node_b in connect_nodes:
                if node_a == node_b:
                    continue

                node_a.overlay.walk_to(node_b.endpoint.wan_address)

        await sleep(5)  # Make sure peers have time to discover each other

        print("IPv8 peer discovery complete")

    def apply_latencies(self):
        """
        If specified in the settings, add latencies between the endpoints.
        """
        if not self.settings.latencies_file:
            return

        latencies = []
        with open(self.settings.latencies_file) as latencies_file:
            for line in latencies_file.readlines():
                latencies.append([float(l) for l in line.strip().split(",")])

        print("Read latency matrix with %d sites!" % len(latencies))

        # Assign nodes to sites in a round-robin fashion and apply latencies accordingly
        for from_ind, from_node in enumerate(self.nodes):
            for to_ind, to_node in enumerate(self.nodes):
                from_site_ind = from_ind % len(latencies)
                to_site_ind = to_ind % len(latencies)
                latency_ms = int(latencies[from_site_ind][to_site_ind]) / 1000
                from_node.endpoint.latencies[to_node.endpoint.wan_address] = latency_ms

        print("Latencies applied!")

    async def start_simulation(self) -> None:
        print("Starting simulation with %d peers..." % self.settings.peers)

        if self.settings.profile:
            yappi.start(builtins=True)

        start_time = time.time()
        await asyncio.sleep(self.settings.duration)
        print("Simulation took %f seconds" % (time.time() - start_time))

        if self.settings.profile:
            yappi.stop()
            yappi_stats = yappi.get_func_stats()
            yappi_stats.sort("tsub")
            yappi_stats.save(os.path.join(self.data_dir, "yappi.stats"), type='callgrind')

        self.loop.stop()

    def on_ipv8_ready(self) -> None:
        """
        This method is called when IPv8 is started and peer discovery is finished.
        """

        # Initialize HIDRA peers
        for node in self.nodes:
            node.overlay.initialize_peer()

        # Execute HIDRA network
        for node in self.nodes:
            node.overlay.start()

    def on_simulation_finished(self) -> None:
        """
        This method is called when the simulations are finished.
        """

        # HIDRA
        self.print_final_results()

    async def run(self) -> None:
        self.setup_directories()
        start_time = time.time()
        await self.start_ipv8_nodes()
        await self.ipv8_discover_peers()
        self.apply_latencies()
        self.on_ipv8_ready()
        print("Simulation setup took %f seconds" % (time.time() - start_time))
        await self.start_simulation()
        self.on_simulation_finished()

        await sleep(1)  # To avoid asyncio errors

    #########
    # HIDRA #
    #########
    def print_final_results(self):
        # Debug
        self.print_local_state()
        self.print_pending_messages()
        self.print_endpoint_statistics()

    def print_local_state(self):
        print("\n----------------- Local state ------------------")
        for peer in self.nodes:
            peer_id = get_peer_id(peer.overlay.my_peer)
            print("- [Peer:" + peer_id + "] --->")
            for k, v in peer.overlay.peers.items():
                if v.info.sn_e > 0 or v.info.sn_r > 0:
                    print("     [Peer:" + k + "]", v.info,
                          dict(sorted(v.deposits.items())), dict(sorted(v.reservations.items())))

    def print_pending_messages(self):
        print("\n--------------- Pending messages ---------------")
        for peer in self.nodes:
            peer_id = get_peer_id(peer.overlay.my_peer)
            print("- [Peer:" + peer_id + "] --->", len(peer.overlay.messages))
            for k, v in peer.overlay.messages.items():
                print("     MSG_ID:", v.payload.msg_id, "APPLICANT:", v.payload.applicant_id, "EVENT:", v.payload.sn_e)

    def print_endpoint_statistics(self):
        print("\n-------------- Endpoint statistics -------------")
        total_num_up = total_bytes_up = 0
        for peer in self.nodes:
            peer_id = get_peer_id(peer.overlay.my_peer)
            print("- [Peer:" + peer_id + "] --->")
            msg_statistics = peer.endpoint.get_statistics(peer.overlay.get_prefix())
            for i in range(1, 19):
                if i in msg_statistics:
                    total_num_up += msg_statistics[i].num_up
                    total_bytes_up += msg_statistics[i].bytes_up
                    print("     MSG_ID:" + str(i), "--->", msg_statistics[i].num_up, msg_statistics[i].bytes_up)
        print("- TOTAL --->", total_num_up, total_bytes_up)
