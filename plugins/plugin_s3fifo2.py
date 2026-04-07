import libcachesim
from collections import deque
from libcachesim import PluginCache, CommonCacheParams, Request, FIFO, SyntheticReader


class S3FIFO:
    def __init__(
        self,
        small_cache_ratio: float = 0.1,
        ghost_cache_ratio: float = 0.9,
        move_to_main_threshold: int = 2,
        cache_size: int = 1024,
    ):
        self.cache_size = cache_size
        small_queue_size = int(small_cache_ratio * cache_size)
        main_queue_size = cache_size - small_queue_size
        ghost_queue_size = int(ghost_cache_ratio * cache_size)

        self.small_set = set()
        self.main_set = set()
        self.ghost_set = deque(maxlen=ghost_queue_size)

        self.small_fifo = FIFO(small_queue_size)
        self.main_fifo = FIFO(main_queue_size)
        self.ghost_fifo = FIFO(ghost_queue_size)

        self.frequency = {}
        self.obj_sizes = {}

        self.max_frequency = 3
        self.move_to_main_threshold = move_to_main_threshold
        self.has_evicted = False
        self.hit_on_ghost = False

    def cache_hit(self, req: Request):
        self.obj_sizes[req.obj_id] = req.obj_size

        if self.small_fifo.find(req, update_cache=False):
            self.frequency[req.obj_id] += 1
        if self.main_fifo.find(req, update_cache=False):
            self.frequency[req.obj_id] += 1

    def cache_miss(self, req: Request):
        self.obj_sizes[req.obj_id] = req.obj_size

        if not self.hit_on_ghost:
            obj = self.ghost_fifo.find(req, update_cache=False)
            if obj is not None:
                self.hit_on_ghost = True
                self.ghost_fifo.remove(req.obj_id)
                self.ghost_set.remove(req.obj_id)

        if not self.hit_on_ghost:
            # Keep Code 2 admission policy unchanged.
            if req.obj_size >= self.small_fifo.cache_size:
                return

            if not self.has_evicted and self.small_fifo.get_occupied_byte() >= self.small_fifo.cache_size:
                obj = self.main_fifo.insert(req)
                self.main_set.add(obj.obj_id)
            else:
                obj = self.small_fifo.insert(req)
                self.small_set.add(obj.obj_id)
        else:
            obj = self.main_fifo.insert(req)
            self.main_set.add(req.obj_id)
            self.hit_on_ghost = False

        self.frequency[obj.obj_id] = 0

    def cache_evict_small(self, req: Request):
        has_evicted = False
        real_evicted_id = None

        while not has_evicted and self.small_fifo.get_occupied_byte() > 0:
            obj_to_evict = self.small_fifo.to_evict(req)
            assert obj_to_evict is not None

            evicted_id = obj_to_evict.obj_id
            obj_size = self.obj_sizes.get(evicted_id, obj_to_evict.obj_size)

            if self.frequency[evicted_id] >= self.move_to_main_threshold:
                new_req = Request(obj_id=evicted_id, obj_size=obj_size)
                self.main_fifo.insert(new_req)
                self.main_set.add(evicted_id)
                self.frequency[evicted_id] = 0
            else:
                new_req = Request(obj_id=evicted_id, obj_size=obj_size)
                self.ghost_fifo.get(new_req)
                self.ghost_set.append(evicted_id)
                has_evicted = True
                real_evicted_id = evicted_id

            removal_successful = self.small_fifo.remove(evicted_id)
            self.small_set.remove(evicted_id)
            assert removal_successful

        return real_evicted_id

    def cache_evict_main(self, req: Request):
        has_evicted = False
        evicted_id = None

        while not has_evicted and self.main_fifo.get_occupied_byte() > 0:
            obj_to_evict = self.main_fifo.to_evict(req)
            assert obj_to_evict is not None

            evicted_id = obj_to_evict.obj_id
            freq = self.frequency[evicted_id]
            obj_size = self.obj_sizes.get(evicted_id, obj_to_evict.obj_size)

            if freq >= 1:
                self.main_fifo.remove(evicted_id)
                self.main_set.remove(evicted_id)

                new_req = Request(obj_id=evicted_id, obj_size=obj_size)
                self.main_fifo.insert(new_req)
                self.main_set.add(evicted_id)
                self.frequency[evicted_id] = min(freq, self.max_frequency) - 1
            else:
                removal_successful = self.main_fifo.remove(evicted_id)
                self.main_set.remove(evicted_id)
                assert removal_successful
                has_evicted = True

        return evicted_id

    def cache_evict(self, req: Request):
        self.obj_sizes[req.obj_id] = req.obj_size

        if not self.hit_on_ghost:
            obj = self.ghost_fifo.find(req, update_cache=False)
            if obj is not None:
                self.hit_on_ghost = True
                self.ghost_fifo.remove(req.obj_id)
                self.ghost_set.remove(req.obj_id)

        self.has_evicted = True
        cond = self.main_fifo.get_occupied_byte() > self.main_fifo.cache_size

        if cond or (self.small_fifo.get_occupied_byte() == 0):
            obj_id = self.cache_evict_main(req)
        else:
            obj_id = self.cache_evict_small(req)

        if obj_id is not None:
            del self.frequency[obj_id]

        return obj_id

    def cache_remove(self, obj_id):
        removed = False
        removed = (removed or self.small_fifo.remove(obj_id))
        removed = (removed or self.ghost_fifo.remove(obj_id))
        removed = (removed or self.main_fifo.remove(obj_id))

        self.small_set.discard(obj_id)
        self.main_set.discard(obj_id)
        try:
            self.ghost_set.remove(obj_id)
        except ValueError:
            pass

        self.frequency.pop(obj_id, None)
        self.obj_sizes.pop(obj_id, None)

        return removed


def cache_init_hook(common_cache_params: CommonCacheParams):
    return S3FIFO(cache_size=common_cache_params.cache_size)


def cache_hit_hook(cache, request: Request):
    cache.cache_hit(request)


def cache_miss_hook(cache, request: Request):
    cache.cache_miss(request)


def cache_eviction_hook(cache, request: Request):
    evicted = None
    while evicted is None:
        evicted = cache.cache_evict(request)
    return evicted


def cache_remove_hook(cache, obj_id):
    return cache.cache_remove(obj_id)


def cache_free_hook(cache):
    pass

if __name__ == "__main__":
    from pathlib import Path
    from libcachesim import PluginCache, TraceReader, TraceType, ReaderInitParam

    def build_cache(cache_size: int) -> PluginCache:
        return PluginCache(
        cache_size=cache_size,
        cache_init_hook=cache_init_hook,
        cache_hit_hook=cache_hit_hook,
        cache_miss_hook=cache_miss_hook,
        cache_eviction_hook=cache_eviction_hook,
        cache_remove_hook=cache_remove_hook,
        cache_free_hook=cache_free_hook,
        cache_name="sieve",
        )

    def make_reader(trace):
        trace = str(trace)

        if trace.endswith(".vscsi"):
            return TraceReader(trace=trace, trace_type=TraceType.VSCSI_TRACE)

        if trace.endswith(".oracleGeneral") or trace.endswith(".oracleGeneral.zst"):
            return TraceReader(
                trace=trace,
            trace_type=TraceType.ORACLE_GENERAL_TRACE,
            reader_init_params=ReaderInitParam(ignore_obj_size=False),
            )

        raise ValueError(
            f"Unsupported trace format for {trace}. "
            "Prefer .vscsi or .oracleGeneral(.zst)."
        )

    def run_one(trace, cache_size=1024 * 1024, start_req=0, max_req=None):
        reader = make_reader(trace)
        cache = build_cache(cache_size)

        kwargs = {}
        if max_req is not None:
            kwargs["start_req"] = start_req
            kwargs["max_req"] = max_req

        req_miss_ratio, byte_miss_ratio = cache.process_trace(reader, **kwargs)
        print(f"{trace}")
        print(f"  request miss ratio: {req_miss_ratio:.4f}")
        print(f"  byte miss ratio:    {byte_miss_ratio:.4f}")
 
    traces = [
        Path("data/cloudPhysicsIO.vscsi"),
        Path("data/twitter_cluster52.oracleGeneral.zst"),
        Path("data/wiki_trace.oracleGeneral.zst"),

        # Direct S3 also works if your environment can access it.
        # Replace <actual-file> with a real file from the dataset listing.
        # "s3://cache-datasets/cache_dataset_oracleGeneral/2020_twitter/<actual-file>.oracleGeneral.zst",
    ]

    for trace in traces:
        run_one(trace, cache_size=1024 * 1024, max_req=1_000_000)
