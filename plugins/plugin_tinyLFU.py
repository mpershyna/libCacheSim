import libcachesim
from collections import OrderedDict
from libcachesim import PluginCache, CommonCacheParams, Request, SyntheticReader


class WTinyLFU:
    def __init__(
        self,
        cache_size: int = 1024,
        window_ratio: float = 0.01,
        protected_ratio: float = 0.80,
        sample_factor: int = 10,
    ):
        self.cache_size = cache_size

        self.window_size = max(1, int(window_ratio * cache_size))
        main_size = max(1, cache_size - self.window_size)
        self.protected_size = max(1, int(protected_ratio * main_size))
        self.probation_size = max(1, main_size - self.protected_size)

        # LRU -> MRU
        self.window = OrderedDict()
        self.probation = OrderedDict()
        self.protected = OrderedDict()

        self.window_bytes = 0
        self.probation_bytes = 0
        self.protected_bytes = 0

        self.freq = {}
        self.obj_sizes = {}

        self.num_requests = 0
        self.sample_size = max(1000, sample_factor * cache_size)

    def bytes_used(self) -> int:
        return self.window_bytes + self.probation_bytes + self.protected_bytes

    def bytes_left(self) -> int:
        return self.cache_size - self.bytes_used()

    def _touch_freq(self, obj_id):
        self.num_requests += 1
        self.freq[obj_id] = self.freq.get(obj_id, 0) + 1

        if self.num_requests % self.sample_size == 0:
            for k in list(self.freq.keys()):
                new_val = self.freq[k] // 2
                if new_val == 0:
                    del self.freq[k]
                else:
                    self.freq[k] = new_val

    def _remove_from_segment(self, segment: OrderedDict, obj_id):
        if obj_id not in segment:
            return False

        size = segment.pop(obj_id)
        if segment is self.window:
            self.window_bytes -= size
        elif segment is self.probation:
            self.probation_bytes -= size
        else:
            self.protected_bytes -= size
        return True

    def _insert_mru(self, segment: OrderedDict, obj_id, size: int):
        segment[obj_id] = size
        segment.move_to_end(obj_id, last=True)

        if segment is self.window:
            self.window_bytes += size
        elif segment is self.probation:
            self.probation_bytes += size
        else:
            self.protected_bytes += size

    def _move_to_mru(self, segment: OrderedDict, obj_id):
        segment.move_to_end(obj_id, last=True)

    def _pop_lru(self, segment: OrderedDict):
        if not segment:
            return None

        obj_id, size = next(iter(segment.items()))
        segment.pop(obj_id)

        if segment is self.window:
            self.window_bytes -= size
        elif segment is self.probation:
            self.probation_bytes -= size
        else:
            self.protected_bytes -= size

        return obj_id, size

    def _evict_record(self, obj_id):
        self.freq.pop(obj_id, None)
        self.obj_sizes.pop(obj_id, None)
        return obj_id

    def contains(self, req: Request) -> bool:
        obj_id = req.obj_id
        return obj_id in self.window or obj_id in self.probation or obj_id in self.protected

    def cache_hit(self, req: Request):
        obj_id = req.obj_id
        self.obj_sizes[obj_id] = req.obj_size
        self._touch_freq(obj_id)

        if obj_id in self.window:
            self._move_to_mru(self.window, obj_id)
            return

        if obj_id in self.protected:
            self._move_to_mru(self.protected, obj_id)
            return

        if obj_id in self.probation:
            size = self.probation.pop(obj_id)
            self.probation_bytes -= size
            self._insert_mru(self.protected, obj_id, size)
            self._ensure_protected_limit()
            return

    def cache_miss(self, req: Request):
        obj_id = req.obj_id
        size = req.obj_size

        self.obj_sizes[obj_id] = size
        self._touch_freq(obj_id)

        if size > self.cache_size:
            return

        self.cache_remove(obj_id)
        self._insert_mru(self.window, obj_id, size)

    def _ensure_protected_limit(self):
        while self.protected_bytes > self.protected_size:
            demoted = self._pop_lru(self.protected)
            if demoted is None:
                break
            obj_id, size = demoted
            self._insert_mru(self.probation, obj_id, size)

    def _rebalance_window_once(self):
        """
        If window is oversized, move its LRU candidate toward probation.
        Returns a real evicted obj_id if TinyLFU rejects someone.
        Otherwise returns None after internal movement only.
        """
        if self.window_bytes <= self.window_size:
            return None

        candidate = self._pop_lru(self.window)
        if candidate is None:
            return None

        cand_id, cand_size = candidate

        # Too large for probation: reject the candidate.
        if cand_size > self.probation_size:
            return self._evict_record(cand_id)

        # Probation has room: internal move only.
        if self.probation_bytes + cand_size <= self.probation_size:
            self._insert_mru(self.probation, cand_id, cand_size)
            return None

        # Probation full: compare against LRU probation victim.
        victim = self._pop_lru(self.probation)
        if victim is None:
            self._insert_mru(self.probation, cand_id, cand_size)
            return None

        victim_id, victim_size = victim
        cand_freq = self.freq.get(cand_id, 0)
        victim_freq = self.freq.get(victim_id, 0)

        if cand_freq >= victim_freq:
            self._insert_mru(self.probation, cand_id, cand_size)
            return self._evict_record(victim_id)

        self._insert_mru(self.probation, victim_id, victim_size)
        return self._evict_record(cand_id)

    def _pick_real_victim(self):
        """
        Pick a real object to leave the cache.
        Prefer probation, then window, then protected.
        """
        victim = self._pop_lru(self.probation)
        if victim is None:
            victim = self._pop_lru(self.window)
        if victim is None:
            victim = self._pop_lru(self.protected)
        if victim is None:
            raise RuntimeError("cache_evict could not find any cached object to evict")

        victim_id, _ = victim
        return self._evict_record(victim_id)

    def cache_evict(self, req: Request):
        """
        Must always return a real object ID.

        libcachesim appears to call eviction before inserting the missed object,
        so we evict until there is enough room for req.obj_size.
        """
        req_size = req.obj_size

        # If the request itself can never fit, we still must return a real ID if
        # eviction hook was called. Evict one victim conservatively.
        if req_size > self.cache_size:
            return self._pick_real_victim()

        while True:
            # Internal maintenance first.
            self._ensure_protected_limit()

            evicted_id = self._rebalance_window_once()
            if evicted_id is not None:
                return evicted_id

            self._ensure_protected_limit()

            # Key fix: make room for the incoming request, not just current occupancy.
            if self.bytes_used() + req_size > self.cache_size:
                return self._pick_real_victim()

            # If all segment constraints are satisfied and there is enough room for
            # the incoming object, but libcachesim still asked for eviction, return
            # one conservative victim instead of None.
            return self._pick_real_victim()

    def cache_remove(self, obj_id):
        removed = False
        removed = self._remove_from_segment(self.window, obj_id) or removed
        removed = self._remove_from_segment(self.probation, obj_id) or removed
        removed = self._remove_from_segment(self.protected, obj_id) or removed

        if removed:
            self.freq.pop(obj_id, None)
            self.obj_sizes.pop(obj_id, None)

        return removed


def cache_init_hook(common_cache_params: CommonCacheParams):
    return WTinyLFU(cache_size=common_cache_params.cache_size)


def cache_hit_hook(cache, request: Request):
    cache.cache_hit(request)


def cache_miss_hook(cache, request: Request):
    cache.cache_miss(request)


def cache_eviction_hook(cache, request: Request):
    return cache.cache_evict(request)


def cache_remove_hook(cache, obj_id):
    return cache.cache_remove(obj_id)


def cache_free_hook(cache):
    pass


if __name__ == "__main__":
    from pathlib import Path
    from libcachesim import PluginCache, TraceReader, TraceType

    plugin_s3fifo_cache = PluginCache(
        cache_size=1024 * 1024,  # 1 MB
        cache_init_hook=cache_init_hook,
        cache_hit_hook=cache_hit_hook,
        cache_miss_hook=cache_miss_hook,
        cache_eviction_hook=cache_eviction_hook,
        cache_remove_hook=cache_remove_hook,
        cache_free_hook=cache_free_hook,
        cache_name="s3fifo",
    )

    trace = Path(__file__).parent.parent / "data" / "cloudPhysicsIO.vscsi"
    reader = TraceReader(trace=str(trace), trace_type=TraceType.VSCSI_TRACE)

    req_miss_ratio, byte_miss_ratio = plugin_s3fifo_cache.process_trace(reader)
    print(f"Request miss ratio: {req_miss_ratio:.4f}")
    print(f"Byte miss ratio: {byte_miss_ratio:.4f}")
