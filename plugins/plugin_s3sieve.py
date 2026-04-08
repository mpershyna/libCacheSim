from collections import deque
from typing import Optional

from libcachesim import CommonCacheParams, PluginCache, Request


class Node:
    __slots__ = ("obj_id", "obj_size", "queue_name", "visited", "prev", "next")

    def __init__(self, obj_id: int, obj_size: int, queue_name: str):
        self.obj_id = obj_id
        self.obj_size = obj_size
        self.queue_name = queue_name
        self.visited = False
        self.prev: Optional["Node"] = None
        self.next: Optional["Node"] = None


class SieveQueue:
    def __init__(self, name: str, cache_size: int):
        self.name = name
        self.cache_size = cache_size
        self.head: Optional[Node] = None
        self.tail: Optional[Node] = None
        self.hand: Optional[Node] = None
        self.used_bytes = 0

    def insert(self, node: Node) -> Node:
        node.queue_name = self.name
        node.visited = False
        node.prev = None
        node.next = self.head

        if self.head is not None:
            self.head.prev = node
        else:
            self.tail = node

        self.head = node
        self.used_bytes += node.obj_size

        if self.hand is None:
            self.hand = self.tail

        return node

    def remove(self, node: Node) -> bool:
        if node.prev is not None:
            node.prev.next = node.next
        else:
            self.head = node.next

        if node.next is not None:
            node.next.prev = node.prev
        else:
            self.tail = node.prev

        if self.hand is node:
            self.hand = node.prev if node.prev is not None else self.tail

        node.prev = None
        node.next = None
        self.used_bytes -= node.obj_size

        if self.head is None:
            self.hand = None

        return True

    def get_occupied_byte(self) -> int:
        return self.used_bytes

    def to_evict(self) -> Optional[Node]:
        if self.tail is None:
            return None

        if self.hand is None:
            self.hand = self.tail

        while self.hand is not None and self.hand.visited:
            self.hand.visited = False
            self.hand = self.hand.prev if self.hand.prev is not None else self.tail

        return self.hand

    def pop_victim(self) -> Optional[Node]:
        victim = self.to_evict()
        if victim is None:
            return None

        self.remove(victim)
        return victim


class S3Sieve:
    def __init__(
        self,
        small_cache_ratio: float = 0.1,
        ghost_cache_ratio: float = 0.9,
        move_to_main_threshold: int = 2,
        cache_size: int = 1024,
    ):
        if cache_size <= 0:
            raise ValueError("cache_size must be positive")
        if not 0 < small_cache_ratio < 1:
            raise ValueError("small_cache_ratio must be between 0 and 1")
        if ghost_cache_ratio <= 0:
            raise ValueError("ghost_cache_ratio must be positive")
        if move_to_main_threshold < 1:
            raise ValueError("move_to_main_threshold must be at least 1")

        self.cache_size = cache_size
        self.small_queue_size = max(1, int(small_cache_ratio * cache_size))
        self.main_queue_size = cache_size - self.small_queue_size
        self.ghost_queue_size = max(1, int(ghost_cache_ratio * cache_size))

        self.small_queue = SieveQueue("small", self.small_queue_size)
        self.main_queue = SieveQueue("main", self.main_queue_size)

        self.index: dict[int, Node] = {}
        self.frequency: dict[int, int] = {}
        self.obj_sizes: dict[int, int] = {}

        self.ghost_fifo: deque[int] = deque()
        self.ghost_sizes: dict[int, int] = {}
        self.ghost_used_bytes = 0

        self.move_to_main_threshold = move_to_main_threshold
        self.pending_eviction = False
        self.pending_ghost_hit = False

    def cache_hit(self, req: Request) -> None:
        node = self.index.get(req.obj_id)
        if node is None:
            return

        self.obj_sizes[req.obj_id] = req.obj_size
        node.visited = True
        if node.queue_name == "small":
            freq = self.frequency.get(req.obj_id, 0)
            if freq < self.move_to_main_threshold:
                self.frequency[req.obj_id] = freq + 1

    def cache_miss(self, req: Request) -> None:
        if req.obj_size > self.cache_size:
            self.pending_eviction = False
            self.pending_ghost_hit = False
            return

        self.obj_sizes[req.obj_id] = req.obj_size
        self._mark_pending_ghost_hit(req.obj_id)

        if not self.pending_ghost_hit:
            if req.obj_size >= self.small_queue.cache_size:
                self.pending_eviction = False
                return

            if not self.pending_eviction and self.small_queue.get_occupied_byte() >= self.small_queue.cache_size:
                node = Node(req.obj_id, req.obj_size, "main")
                self.main_queue.insert(node)
            else:
                node = Node(req.obj_id, req.obj_size, "small")
                self.small_queue.insert(node)
        else:
            node = Node(req.obj_id, req.obj_size, "main")
            self.main_queue.insert(node)
            self.pending_ghost_hit = False

        self.index[req.obj_id] = node
        self.frequency[req.obj_id] = 0
        self.pending_eviction = False

    def cache_evict_small(self) -> Optional[int]:
        while self.small_queue.get_occupied_byte() > 0:
            victim = self.small_queue.pop_victim()
            if victim is None:
                return None

            evicted_id = victim.obj_id
            obj_size = self.obj_sizes.get(evicted_id, victim.obj_size)

            if self.frequency.get(evicted_id, 0) >= self.move_to_main_threshold:
                victim.obj_size = obj_size
                self.main_queue.insert(victim)
                self.frequency[evicted_id] = 0
            else:
                self.index.pop(evicted_id, None)
                self.frequency.pop(evicted_id, None)
                self.obj_sizes.pop(evicted_id, None)
                self._add_ghost(evicted_id, obj_size)
                return evicted_id

        return None

    def cache_evict_main(self) -> Optional[int]:
        victim = self.main_queue.pop_victim()
        if victim is None:
            return None

        evicted_id = victim.obj_id
        self.index.pop(evicted_id, None)
        self.frequency.pop(evicted_id, None)
        self.obj_sizes.pop(evicted_id, None)
        return evicted_id

    def cache_evict(self, req: Request) -> Optional[int]:
        if req.obj_size > self.cache_size:
            return 0

        self.obj_sizes[req.obj_id] = req.obj_size
        self._mark_pending_ghost_hit(req.obj_id)
        self.pending_eviction = True

        main_is_over_target = self.main_queue.get_occupied_byte() > self.main_queue.cache_size

        if main_is_over_target or self.small_queue.get_occupied_byte() == 0:
            return self.cache_evict_main()
        return self.cache_evict_small()

    def cache_remove(self, obj_id: int) -> bool:
        node = self.index.pop(obj_id, None)
        if node is not None:
            queue = self.small_queue if node.queue_name == "small" else self.main_queue
            queue.remove(node)
            self.frequency.pop(obj_id, None)
            self.obj_sizes.pop(obj_id, None)
            return True

        removed = self._remove_ghost(obj_id)
        self.frequency.pop(obj_id, None)
        self.obj_sizes.pop(obj_id, None)
        return removed

    def clear(self) -> None:
        self.index.clear()
        self.frequency.clear()
        self.obj_sizes.clear()
        self.small_queue = SieveQueue("small", self.small_queue_size)
        self.main_queue = SieveQueue("main", self.main_queue_size)
        self.ghost_fifo.clear()
        self.ghost_sizes.clear()
        self.ghost_used_bytes = 0
        self.pending_eviction = False
        self.pending_ghost_hit = False

    def _mark_pending_ghost_hit(self, obj_id: int) -> None:
        if not self.pending_ghost_hit and obj_id in self.ghost_sizes:
            self.pending_ghost_hit = True
            self._remove_ghost(obj_id)

    def _add_ghost(self, obj_id: int, obj_size: int) -> None:
        if obj_id in self.ghost_sizes:
            self._remove_ghost(obj_id)

        self.ghost_fifo.append(obj_id)
        self.ghost_sizes[obj_id] = obj_size
        self.ghost_used_bytes += obj_size

        while self.ghost_used_bytes > self.ghost_queue_size and self.ghost_fifo:
            old_obj_id = self.ghost_fifo.popleft()
            old_size = self.ghost_sizes.pop(old_obj_id, 0)
            self.ghost_used_bytes -= old_size

    def _remove_ghost(self, obj_id: int) -> bool:
        old_size = self.ghost_sizes.pop(obj_id, None)
        if old_size is None:
            return False

        self.ghost_used_bytes -= old_size
        return True


def make_s3sieve_plugin(
    cache_size: int,
    small_cache_ratio: float = 0.1,
    ghost_cache_ratio: float = 0.9,
    move_to_main_threshold: int = 2,
    cache_name: str = "s3sieve",
) -> PluginCache:
    def init_hook(common_cache_params: CommonCacheParams) -> S3Sieve:
        return S3Sieve(
            small_cache_ratio=small_cache_ratio,
            ghost_cache_ratio=ghost_cache_ratio,
            move_to_main_threshold=move_to_main_threshold,
            cache_size=common_cache_params.cache_size,
        )

    return PluginCache(
        cache_size=cache_size,
        cache_init_hook=init_hook,
        cache_hit_hook=cache_hit_hook,
        cache_miss_hook=cache_miss_hook,
        cache_eviction_hook=cache_eviction_hook,
        cache_remove_hook=cache_remove_hook,
        cache_free_hook=cache_free_hook,
        cache_name=cache_name,
    )


DEFAULT_SMALL_CACHE_RATIO = 0.1
DEFAULT_GHOST_CACHE_RATIO = 0.9
DEFAULT_MOVE_TO_MAIN_THRESHOLD = 2


def cache_init_hook(common_cache_params: CommonCacheParams) -> S3Sieve:
    return S3Sieve(
        small_cache_ratio=DEFAULT_SMALL_CACHE_RATIO,
        ghost_cache_ratio=DEFAULT_GHOST_CACHE_RATIO,
        move_to_main_threshold=DEFAULT_MOVE_TO_MAIN_THRESHOLD,
        cache_size=common_cache_params.cache_size,
    )


def cache_hit_hook(cache: S3Sieve, request: Request) -> None:
    cache.cache_hit(request)


def cache_miss_hook(cache: S3Sieve, request: Request) -> None:
    cache.cache_miss(request)


def cache_eviction_hook(cache: S3Sieve, request: Request) -> int:
    evicted = None
    while evicted is None:
        evicted = cache.cache_evict(request)
    return evicted


def cache_remove_hook(cache: S3Sieve, obj_id: int) -> bool:
    return cache.cache_remove(obj_id)


def cache_free_hook(cache: S3Sieve) -> None:
    cache.clear()

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
