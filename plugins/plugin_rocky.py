import libcachesim
from collections import OrderedDict
from libcachesim import PluginCache, CommonCacheParams, Request, SyntheticReader


class RockyHybridCache:
    def __init__(
        self,
        cache_size: int = 1024,
        sram_ratio: float = 0.10,
        promote_threshold: int = 2,
        aging_interval: int = 10000,
    ):
        self.cache_size = cache_size
        self.sram_size = max(1, int(sram_ratio * cache_size))
        self.stt_size = max(1, cache_size - self.sram_size)

        # LRU -> MRU
        self.sram = OrderedDict()
        self.stt = OrderedDict()

        self.sram_bytes = 0
        self.stt_bytes = 0

        self.freq = {}
        self.obj_sizes = {}
        self.last_touch = {}

        self.promote_threshold = promote_threshold
        self.aging_interval = aging_interval
        self.clock = 0

    def bytes_used(self) -> int:
        return self.sram_bytes + self.stt_bytes

    def bytes_left(self) -> int:
        return self.cache_size - self.bytes_used()

    def contains(self, req: Request) -> bool:
        obj_id = req.obj_id
        return obj_id in self.sram or obj_id in self.stt

    def _tick(self, obj_id):
        self.clock += 1
        self.freq[obj_id] = self.freq.get(obj_id, 0) + 1
        self.last_touch[obj_id] = self.clock

        if self.clock % self.aging_interval == 0:
            for k in list(self.freq.keys()):
                new_val = self.freq[k] // 2
                if new_val == 0:
                    del self.freq[k]
                else:
                    self.freq[k] = new_val

    def _insert_mru(self, segment: OrderedDict, obj_id, size: int):
        segment[obj_id] = size
        segment.move_to_end(obj_id, last=True)
        if segment is self.sram:
            self.sram_bytes += size
        else:
            self.stt_bytes += size

    def _remove(self, segment: OrderedDict, obj_id):
        if obj_id not in segment:
            return False

        size = segment.pop(obj_id)
        if segment is self.sram:
            self.sram_bytes -= size
        else:
            self.stt_bytes -= size
        return True

    def _move_to_mru(self, segment: OrderedDict, obj_id):
        segment.move_to_end(obj_id, last=True)

    def _pop_lru(self, segment: OrderedDict):
        if not segment:
            return None

        obj_id, size = next(iter(segment.items()))
        segment.pop(obj_id)

        if segment is self.sram:
            self.sram_bytes -= size
        else:
            self.stt_bytes -= size

        return obj_id, size

    def _evict_record(self, obj_id):
        self.freq.pop(obj_id, None)
        self.obj_sizes.pop(obj_id, None)
        self.last_touch.pop(obj_id, None)
        return obj_id

    def _demote_sram_once(self):
        """
        Move the LRU SRAM block into STT instead of dropping it immediately.
        Returns a real evicted ID only if STT admission rejects or evicts someone.
        """
        if self.sram_bytes <= self.sram_size:
            return None

        victim = self._pop_lru(self.sram)
        if victim is None:
            return None

        obj_id, size = victim

        # If object is too large for STT partition, evict it for real.
        if size > self.stt_size:
            return self._evict_record(obj_id)

        self._insert_mru(self.stt, obj_id, size)
        return None

    def _pick_stt_victim(self):
        """
        Reliability-friendly STT victim selection.

        Since libcachesim traces do not expose write count or Hamming weight,
        we approximate ROCKY's preference by evicting the coldest STT block:
        low frequency first, then older recency.
        """
        if not self.stt:
            return None

        best_id = None
        best_score = None

        for obj_id in self.stt.keys():
            f = self.freq.get(obj_id, 0)
            t = self.last_touch.get(obj_id, -1)

            # Lower is worse, so more evictable.
            score = (f, t)

            if best_score is None or score < best_score:
                best_score = score
                best_id = obj_id

        size = self.stt.pop(best_id)
        self.stt_bytes -= size
        return best_id, size

    def _ensure_sram_limit(self):
        while self.sram_bytes > self.sram_size:
            evicted_id = self._demote_sram_once()
            if evicted_id is not None:
                return evicted_id
        return None

    def cache_hit(self, req: Request):
        obj_id = req.obj_id
        size = req.obj_size
        self.obj_sizes[obj_id] = size
        self._tick(obj_id)

        if obj_id in self.sram:
            self._move_to_mru(self.sram, obj_id)
            return

        if obj_id in self.stt:
            # Promote hot STT blocks into SRAM.
            if self.freq.get(obj_id, 0) >= self.promote_threshold:
                self._remove(self.stt, obj_id)
                self._insert_mru(self.sram, obj_id, size)
                self._ensure_sram_limit()
            else:
                self._move_to_mru(self.stt, obj_id)

    def cache_miss(self, req: Request):
        obj_id = req.obj_id
        size = req.obj_size

        self.obj_sizes[obj_id] = size
        self._tick(obj_id)

        if size > self.cache_size:
            return

        # libcachesim appears to call eviction before insertion, so this method
        # only performs admission.
        if size <= self.sram_size and self.freq.get(obj_id, 0) >= self.promote_threshold:
            self._insert_mru(self.sram, obj_id, size)
            self._ensure_sram_limit()
        else:
            self._insert_mru(self.stt, obj_id, size)

    def cache_evict(self, req: Request):
        """
        Always return a real object ID.

        We make room for the incoming request before cache_miss() inserts it.
        """
        req_size = req.obj_size

        while True:
            evicted_id = self._ensure_sram_limit()
            if evicted_id is not None:
                return evicted_id

            if self.bytes_used() + req_size <= self.cache_size:
                # libcachesim still expects an object id, so evict from STT first.
                victim = self._pick_stt_victim()
                if victim is None:
                    victim = self._pop_lru(self.sram)
                if victim is None:
                    raise RuntimeError("ROCKY-style cache could not find an eviction victim")

                victim_id, _ = victim
                return self._evict_record(victim_id)

            victim = self._pick_stt_victim()
            if victim is None:
                victim = self._pop_lru(self.sram)
            if victim is None:
                raise RuntimeError(
                    f"cache_evict could not find victim for obj_id={req.obj_id}, "
                    f"size={req.obj_size}, bytes_used={self.bytes_used()}, "
                    f"cache_size={self.cache_size}"
                )

            victim_id, _ = victim
            return self._evict_record(victim_id)

    def cache_remove(self, obj_id):
        removed = False
        removed = self._remove(self.sram, obj_id) or removed
        removed = self._remove(self.stt, obj_id) or removed

        if removed:
            self.freq.pop(obj_id, None)
            self.obj_sizes.pop(obj_id, None)
            self.last_touch.pop(obj_id, None)

        return removed


def cache_init_hook(common_cache_params: CommonCacheParams):
    return RockyHybridCache(cache_size=common_cache_params.cache_size)


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
