import asyncio
import os
import time
import warnings
from asyncio import ensure_future, get_event_loop
from typing import Optional

import numpy as np
from matplotlib import pyplot as plt
from matplotlib.ticker import MaxNLocator

from deployment.settings import DeploymentSettings

from pyipv8.ipv8.configuration import ConfigBuilder
from pyipv8.ipv8_service import IPv8

from hidra.community import HIDRACommunity
from hidra.utils import get_peer_id

# Ignoring warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)


class HIDRADeployment:
    MAIN_OVERLAY: Optional[str] = "HIDRACommunity"

    def __init__(self):
        self.settings = DeploymentSettings()
        self.data_dir = os.path.join("data", "n_%d" % self.settings.peers)
        self.nodes = []

    def set_ipv8_builder(self, peer_id: int) -> ConfigBuilder:
        builder = ConfigBuilder().clear_keys().clear_overlays()
        builder.add_key("my peer", "curve25519", os.path.join(self.data_dir, f"ec{peer_id}.pem"))
        builder.add_overlay("HIDRACommunity", "my peer", [], [], {}, [])
        builder.set_log_level(self.settings.logging_level)

        return builder

    async def run(self) -> None:
        self.setup_directories()
        await self.start_ipv8_nodes()
        await self.ipv8_discover_peers()
        self.on_ipv8_ready()
        await asyncio.sleep(self.settings.duration)
        self.print_final_results()
        get_event_loop().stop()

    def setup_directories(self) -> None:
        os.makedirs(self.data_dir, exist_ok=True)

    async def start_ipv8_nodes(self) -> None:
        for peer_id in range(1, self.settings.peers + 1):
            builder = self.set_ipv8_builder(peer_id)

            # IPv8 instance
            instance = IPv8(builder.finalize(),
                            enable_statistics=self.settings.enable_community_statistics,
                            extra_communities={'HIDRACommunity': HIDRACommunity})
            await instance.start()

            if not self.settings.enable_ipv8_ticker:
                # Disable the IPv8 ticker
                instance.state_machine_task.cancel()

            for overlay in instance.overlays:
                overlay.max_peers = -1

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

    async def ipv8_discover_peers(self) -> None:
        for node_a in self.nodes:
            connect_nodes = self.nodes
            for node_b in connect_nodes:
                if node_a == node_b:
                    continue

                node_a.overlay.walk_to(node_b.endpoint.my_estimated_lan)

        await asyncio.sleep(5)  # Make sure peers have time to discover each other

        print("IPv8 peer discovery complete")

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

    def print_final_results(self):
        # Debug
        self.print_local_state()
        self.print_pending_messages()
        self.print_endpoint_statistics()

        # Experiments
        # self.experiment1()

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

    def experiment1(self):
        # HIDRA: one event (simulated nginx, 1024, 80)
        # Light-HIDRA (no domains): one event (simulated nginx, 1024, 80), intra, no requests_per_epoch
        # Light-HIDRA (with domains): one event (simulated nginx, 1024, 80), inter, no requests_per_epoch

        # Data
        ex1_peers_per_domain = 15
        ex1_total_events = 1
        ex1_sc_deployment = round(22982 / ex1_total_events)

        # Figure
        fig, ax = plt.subplots(layout='constrained')

        # Figure data
        fanouts = (2 * ex1_peers_per_domain, 4 * ex1_peers_per_domain,
                   6 * ex1_peers_per_domain, 8 * ex1_peers_per_domain)

        # sim=258320
        # dep=261210
        # wir=293090
        # hid=748120

        bandwidth = {
            'HIDRA': (
                self.b_to_mb(
                    (ex1_sc_deployment + 1202 + 1010 + (913 * (fanouts[0] - 1)) + (783 * fanouts[0]) + 750 + 750) *
                    (fanouts[0] - 1)),
                self.b_to_mb(
                    (ex1_sc_deployment + 1202 + 1010 + (913 * (fanouts[1] - 1)) + (783 * fanouts[1]) + 750 + 750) *
                    (fanouts[1] - 1)),
                self.b_to_mb(
                    (ex1_sc_deployment + 1202 + 1010 + (913 * (fanouts[2] - 1)) + (783 * fanouts[2]) + 750 + 750) *
                    (fanouts[2] - 1)),
                self.b_to_mb(
                    (ex1_sc_deployment + 1202 + 1010 + (913 * (fanouts[3] - 1)) + (783 * fanouts[3]) + 750 + 750) *
                    (fanouts[3] - 1))
            ),
            'Light-HIDRA (no domains)': (
                self.b_to_mb(2328720),
                self.b_to_mb(9228300),
                self.b_to_mb(20644020),
                self.b_to_mb(36695040)
            ),
            'Light-HIDRA (with domains)': (
                self.b_to_mb(641055),
                self.b_to_mb(641055),
                self.b_to_mb(641055),
                self.b_to_mb(641055)
            ),
        }
        x = np.arange(len(fanouts))
        width = 0.3
        multiplier = 0
        for k, v in bandwidth.items():
            offset = width * multiplier
            bar = ax.bar(x + offset, v, width, label=k)
            ax.bar_label(bar, fontsize=10)
            multiplier += 1

        # X-axis
        ax.set_xlabel('Peers', fontweight="bold", fontsize=12)
        ax.set_xticks(x + width, fanouts)
        ax.set_ylim(top=50)

        # Y-axis
        ax.set_ylabel('Total bandwidth usage (MB)', fontweight="bold", fontsize=12)
        ax.yaxis.set_major_locator(MaxNLocator(integer=True))

        # Figure style
        ax.legend(loc="upper center", bbox_to_anchor=(0, 1, 1, 0.09), ncol=3, frameon=False)
        ax.set_axisbelow(True)
        plt.grid(True, alpha=0.33)
        plt.show()

    @staticmethod
    def b_to_mb(n_bytes):
        return round(n_bytes / 1024 / 1024, 2)


if __name__ == "__main__":
    deployment = HIDRADeployment()
    ensure_future(deployment.run())
    get_event_loop().run_forever()
