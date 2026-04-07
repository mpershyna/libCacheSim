from typing import Optional

from libcachesim import CommonCacheParams, Request


class Node:
    __slots__ = ("obj_id", "obj_size", "visited", "prev", "next")

    def __init__(self, obj_id: int, obj_size: int):
        self.obj_id = obj_id
        self.obj_size = obj_size
        self.visited = False
        self.prev: Optional["Node"] = None  # newer object, toward head
        self.next: Optional["Node"] = None  # older object, toward tail


class SieveCache:
    def __init__(self, cache_size: int):
        self.cache_size = cache_size
        self.used_bytes = 0
        self.nodes: dict[int, Node] = {}
        self.head: Optional[Node] = None  # newest object
        self.tail: Optional[Node] = None  # oldest object
        self.hand: Optional[Node] = None  # next eviction candidate

    def _insert_at_head(self, node: Node) -> None:
        node.prev = None
        node.next = self.head
        if self.head is not None:
            self.head.prev = node
        else:
            self.tail = node
        self.head = node

    def _unlink(self, node: Node) -> None:
        if node.prev is not None:
            node.prev.next = node.next
        else:
            self.head = node.next

        if node.next is not None:
            node.next.prev = node.prev
        else:
            self.tail = node.prev

        node.prev = None
        node.next = None

    def _step_hand(self, node: Node) -> Optional[Node]:
        if node.prev is not None:
            return node.prev
        return self.tail

    def on_hit(self, req: Request) -> None:
        node = self.nodes.get(req.obj_id)
        if node is not None:
            node.visited = True

    def on_miss(self, req: Request) -> None:
        if req.obj_size > self.cache_size or req.obj_id in self.nodes:
            return

        node = Node(req.obj_id, req.obj_size)
        self.nodes[req.obj_id] = node
        self._insert_at_head(node)
        self.used_bytes += req.obj_size

        if self.hand is None:
            self.hand = self.tail

    def evict(self, _: Request) -> int:
        if not self.nodes:
            return 0

        if self.hand is None:
            self.hand = self.tail

        while self.hand is not None and self.hand.visited:
            self.hand.visited = False
            self.hand = self._step_hand(self.hand)

        victim = self.hand
        if victim is None:
            return 0

        next_candidate = victim.prev
        self._unlink(victim)
        self.nodes.pop(victim.obj_id, None)
        self.used_bytes -= victim.obj_size

        if not self.nodes:
            self.hand = None
        elif next_candidate is not None:
            self.hand = next_candidate
        else:
            self.hand = self.tail

        return victim.obj_id

    def on_remove(self, obj_id: int) -> None:
        node = self.nodes.pop(obj_id, None)
        if node is None:
            return

        hand_was_node = self.hand is node
        next_candidate = node.prev
        self._unlink(node)
        self.used_bytes -= node.obj_size

        if not self.nodes:
            self.hand = None
        elif hand_was_node:
            if next_candidate is not None:
                self.hand = next_candidate
            else:
                self.hand = self.tail


def cache_init_hook(common_cache_params: CommonCacheParams) -> SieveCache:
    return SieveCache(common_cache_params.cache_size)


def cache_hit_hook(data: SieveCache, req: Request) -> None:
    data.on_hit(req)


def cache_miss_hook(data: SieveCache, req: Request) -> None:
    data.on_miss(req)


def cache_eviction_hook(data: SieveCache, req: Request) -> int:
    return data.evict(req)


def cache_remove_hook(data: SieveCache, obj_id: int) -> None:
    data.on_remove(obj_id)


def cache_free_hook(data: SieveCache) -> None:
    data.nodes.clear()
    data.head = None
    data.tail = None
    data.hand = None
    data.used_bytes = 0

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
