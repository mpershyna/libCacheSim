from collections import deque
from libcachesim import CommonCacheParams, Request
import random
import math

class FifoCache:
    def __init__(self, cache_size: int):
        self.queue = deque()
        self.cache_size = cache_size
        self.hand = 0
        self.tracker_array = []

    def on_hit(self, req: Request):
        index_visited = self.queue.index(req.obj_id)
        self.tracker_array[index_visited] = 1

    def on_miss(self, req: Request):
        if req.obj_size <= self.cache_size:
            self.queue.append(req.obj_id)
            self.tracker_array.append(0)

    def evict(self, req: Request):
        if not self.queue:
            return 0
        hand = self.hand
        if hand > len(self.queue) - 1:
            hand = 0
        while self.tracker_array[hand] == 1:
            self.tracker_array[hand] = 0
            hand = (hand + 1) % len(self.queue)
        victim = self.queue[hand]
        
        self.queue.rotate(-hand) 
        victim = self.queue.popleft() 
        self.queue.rotate(hand)

        self.tracker_array.pop(hand)
        self.hand = hand
        return victim

    def on_remove(self, obj_id: int):
        try:
           index_removed = self.queue.index(obj_id) 
           self.queue.remove(obj_id)
           self.tracker_array.pop(index_removed)
        except ValueError:
            pass  # Object not in queue


def cache_init_hook(common_cache_params: CommonCacheParams):
    return FifoCache(common_cache_params.cache_size)


def cache_hit_hook(data: FifoCache, req: Request):
    data.on_hit(req)


def cache_miss_hook(data: FifoCache, req: Request):
    data.on_miss(req)


def cache_eviction_hook(data: FifoCache, req: Request):
    return data.evict(req)


def cache_remove_hook(data: FifoCache, obj_id: int):
    data.on_remove(obj_id)


def cache_free_hook(data: FifoCache):
    data.queue.clear()

if __name__ == "__main__":
    from pathlib import Path
    from libcachesim import PluginCache, TraceReader, TraceType

    plugin_fifo_cache = PluginCache(
        cache_size=1024 * 1024,  # 1 MB
        cache_init_hook=cache_init_hook,
        cache_hit_hook=cache_hit_hook,
        cache_miss_hook=cache_miss_hook,
        cache_eviction_hook=cache_eviction_hook,
        cache_remove_hook=cache_remove_hook,
        cache_free_hook=cache_free_hook,
        cache_name="fifo",
    )

    trace = Path(__file__).parent.parent / "data" / "cloudPhysicsIO.vscsi"
    reader = TraceReader(trace=str(trace), trace_type=TraceType.VSCSI_TRACE)

    req_miss_ratio, byte_miss_ratio = plugin_fifo_cache.process_trace(reader)
    print(f"Request miss ratio: {req_miss_ratio:.4f}")
    print(f"Byte miss ratio: {byte_miss_ratio:.4f}")
