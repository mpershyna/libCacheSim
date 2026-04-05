import libcachesim
from collections import deque
from libcachesim import PluginCache, CommonCacheParams, Request, FIFO, SyntheticReader


class MyS3FIFO:
    def __init__(
        self,
        small_cache_ratio: float = 0.1,
        ghost_cache_ratio: float = 0.9,
        move_to_main_threshold: int = 2,
        cache_size: int = 1024,
    ):
        self.cache_size = cache_size
        self.small_queue_size = max(1, int(small_cache_ratio * cache_size))
        self.main_queue_size = max(1, cache_size - self.small_queue_size)
        self.ghost_queue_size = max(1, int(ghost_cache_ratio * cache_size))
        self.small_fifo = FIFO(self.small_queue_size)
        self.main_fifo = FIFO(self.main_queue_size)
        self.small_set = set()
        self.main_set = set()
        self.ghost_set = set()
        self.ghost_queue = deque(maxlen=self.ghost_queue_size)

        self.frequency = {}
        self.obj_sizes = {}

        self.max_frequency = 3
        self.move_to_main_threshold = move_to_main_threshold

        self.num_requests = 0
        self.num_hits = 0
        self.num_misses = 0
        self.request_bytes = 0
        self.hit_bytes = 0
        self.miss_bytes = 0

    def bytes_used(self) -> int:
        return self.small_fifo.get_occupied_byte() + self.main_fifo.get_occupied_byte()

    def bytes_left(self) -> int:
        return self.cache_size - self.bytes_used()

    def byte_miss_ratio(self) -> float:
        if self.request_bytes == 0:
            return 0.0
        return self.miss_bytes / self.request_bytes

    def req_miss_ratio(self) -> float:
        if self.num_requests == 0:
            return 0.0
        return self.num_misses / self.num_requests

    def _remember_request(self, req: Request):
        self.num_requests += 1
        self.request_bytes += req.obj_size
        self.obj_sizes[req.obj_id] = req.obj_size

    def _record_hit(self, req: Request):
        self.num_hits += 1
        self.hit_bytes += req.obj_size

    def _record_miss(self, req: Request):
        self.num_misses += 1
        self.miss_bytes += req.obj_size

    def _in_small(self, req: Request) -> bool:
        return self.small_fifo.find(req, update_cache=False) is not None

    def _in_main(self, req: Request) -> bool:
        return self.main_fifo.find(req, update_cache=False) is not None

    def contains(self, req: Request) -> bool:
        return self._in_small(req) or self._in_main(req)

    def _add_to_ghost(self, obj_id):
        if obj_id in self.ghost_set:
            return

        if len(self.ghost_queue) == self.ghost_queue.maxlen:
            old = self.ghost_queue.popleft()
            self.ghost_set.remove(old)

        self.ghost_queue.append(obj_id)
        self.ghost_set.add(obj_id)

    def _remove_from_ghost(self, obj_id):
        if obj_id not in self.ghost_set:
            return False
        self.ghost_set.remove(obj_id)
        try:
            self.ghost_queue.remove(obj_id)
        except ValueError:
            pass
        return True

    def cache_hit(self, req: Request):
        self._remember_request(req)
        self._record_hit(req)

        if self._in_small(req):
            self.frequency[req.obj_id] = self.frequency.get(req.obj_id, 0) + 1
            return

        if self._in_main(req):
            self.frequency[req.obj_id] = self.frequency.get(req.obj_id, 0) + 1
            return

        self.frequency.setdefault(req.obj_id, 0)

    def cache_miss(self, req: Request):
        self._remember_request(req)
        self._record_miss(req)

        if req.obj_size > self.cache_size:
            return

        hit_on_ghost = req.obj_id in self.ghost_set
        if hit_on_ghost:
            self._remove_from_ghost(req.obj_id)
            inserted = self.main_fifo.insert(req)
            self.main_set.add(inserted.obj_id)
            self.frequency[inserted.obj_id] = 0
            return

        if req.obj_size > self.small_queue_size:
            inserted = self.main_fifo.insert(req)
            self.main_set.add(inserted.obj_id)
        else:
            inserted = self.small_fifo.insert(req)
            self.small_set.add(inserted.obj_id)

        self.frequency[inserted.obj_id] = 0

    def cache_evict_small(self, req: Request):
        """
        Try to evict one object from the small queue.
        Returns the obj_id of a real eviction, or None if only promotions happened.
        """
        while self.small_fifo.get_occupied_byte() > 0:
            obj_to_evict = self.small_fifo.to_evict(req)
            if obj_to_evict is None:
                return None

            evicted_id = obj_to_evict.obj_id
            freq = self.frequency.get(evicted_id, 0)
            obj_size = self.obj_sizes.get(evicted_id, obj_to_evict.obj_size)

            removal_successful = self.small_fifo.remove(evicted_id)
            assert removal_successful
            self.small_set.remove(evicted_id)

            if freq >= self.move_to_main_threshold:

                promoted_req = Request(obj_id=evicted_id, obj_size=obj_size)
                inserted = self.main_fifo.insert(promoted_req)
                self.main_set.add(inserted.obj_id)
                self.frequency[evicted_id] = 0

                continue

            self._add_to_ghost(evicted_id)
            return evicted_id

        return None

    def cache_evict_main(self, req: Request):
        """
        Try to evict one object from the main queue.
        Returns the obj_id of a real eviction, or None if no victim exists.
        """
        while self.main_fifo.get_occupied_byte() > 0:
            obj_to_evict = self.main_fifo.to_evict(req)
            if obj_to_evict is None:
                return None

            evicted_id = obj_to_evict.obj_id
            freq = self.frequency.get(evicted_id, 0)
            obj_size = self.obj_sizes.get(evicted_id, obj_to_evict.obj_size)

            if freq >= 1:
                removal_successful = self.main_fifo.remove(evicted_id)
                assert removal_successful
                self.main_set.remove(evicted_id)

                same_req = Request(obj_id=evicted_id, obj_size=obj_size)
                inserted = self.main_fifo.insert(same_req)
                self.main_set.add(inserted.obj_id)

                self.frequency[evicted_id] = min(freq, self.max_frequency) - 1
                continue

            removal_successful = self.main_fifo.remove(evicted_id)
            assert removal_successful
            self.main_set.remove(evicted_id)
            return evicted_id

        return None

    def cache_evict(self, req: Request):
        """
        Perform one eviction operation that MUST return a real object id
        that leaves the cache.

        S3FIFO may first do internal rearrangement (e.g. promote from small
        to main), but this function must not return until a true victim is found.
        """

        target_main = (req.obj_id in self.ghost_set) or (req.obj_size > self.small_queue_size)

        obj_id = None

        if target_main:
            obj_id = self.cache_evict_main(req)

            if obj_id is None:
                obj_id = self.cache_evict_small(req)
        else:
            obj_id = self.cache_evict_small(req)

            if obj_id is None:
                obj_id = self.cache_evict_main(req)

        if obj_id is None:
            raise RuntimeError(
                f"cache_evict could not find a real victim for request "
                f"obj_id={req.obj_id}, size={req.obj_size}. "
                f"bytes_used={self.bytes_used()}, bytes_left={self.bytes_left()}, "
                f"small_used={self.small_fifo.get_occupied_byte()}, "
                f"main_used={self.main_fifo.get_occupied_byte()}"
            )

        self.frequency.pop(obj_id, None)
        return obj_id

    def cache_remove(self, obj_id):
        removed = False

        if obj_id in self.small_set:
            removed = self.small_fifo.remove(obj_id) or removed
            self.small_set.discard(obj_id)

        if obj_id in self.main_set:
            removed = self.main_fifo.remove(obj_id) or removed
            self.main_set.discard(obj_id)

        if obj_id in self.ghost_set:
            self._remove_from_ghost(obj_id)
            removed = True

        self.frequency.pop(obj_id, None)

        return removed


def cache_init_hook(common_cache_params: CommonCacheParams):
    return MyS3FIFO(cache_size=common_cache_params.cache_size)


def cache_hit_hook(cache, request: Request):
    cache.cache_hit(request)


def cache_miss_hook(cache, request: Request):
    cache.cache_miss(request)


def cache_eviction_hook(cache, request: Request):
    return cache.cache_evict(request)

def cache_remove_hook(cache, obj_id):
    return cache.cache_remove(obj_id)


def cache_free_hook(cache):
    print("===== S3FIFO stats =====")
    print(f"requests        : {cache.num_requests}")
    print(f"hits            : {cache.num_hits}")
    print(f"misses          : {cache.num_misses}")
    print(f"request bytes   : {cache.request_bytes}")
    print(f"hit bytes       : {cache.hit_bytes}")
    print(f"miss bytes      : {cache.miss_bytes}")
    print(f"req miss ratio  : {cache.req_miss_ratio():.6f}")
    print(f"byte miss ratio : {cache.byte_miss_ratio():.6f}")
    print(f"bytes used      : {cache.bytes_used()}")
    print(f"bytes left      : {cache.bytes_left()}")

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
