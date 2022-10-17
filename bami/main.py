import warnings
from asyncio import ensure_future

from pyipv8.ipv8.configuration import ConfigBuilder

from bami.settings import SimulationSettings
from bami.simulation import BamiSimulation

# Ignoring warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)


class HIDRASimulation(BamiSimulation):

    def get_ipv8_builder(self, peer_id: int) -> ConfigBuilder:
        builder = super().get_ipv8_builder(peer_id)
        builder.add_overlay("HIDRACommunity", "my peer", [], [], {}, [('init',)])
        return builder


if __name__ == "__main__":
    settings = SimulationSettings()
    settings.peers = 3
    settings.duration = 20
    simulation = HIDRASimulation(settings)
    ensure_future(simulation.run())

    simulation.loop.run_forever()
