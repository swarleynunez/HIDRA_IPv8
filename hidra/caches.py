from pyipv8.ipv8.requestcache import NumberCache


class HIDRANumberCache(NumberCache):
    """
    Number cache for HIDRA objects identified by an ID
    """

    def __init__(self, request_cache, prefix: str, object_id: int):
        super().__init__(request_cache, prefix, object_id)

    @property
    def timeout_delay(self):
        return 30

    def on_timeout(self):
        # print("Timeout for \"" + self.prefix + "\":", str(self.number))
        pass
