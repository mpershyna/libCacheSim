from libcachesim import CommonCacheParams, Request

class Node:
    __slots__ = ("obj_id", "size", "ref", "prev", "next")

    def __init__(self, obj_id: int, size: int):
        self.obj_id = obj_id
        self.size = size
        self.ref = 0
        self.prev = None
        self.next = None


class SieveCache:
    def __init__(self, cache_size: int):
        self.cache_size = cache_size
        self.current_size = 0
        self.nodes = {}   # obj_id -> Node
        self.head = None
        self.tail = None
        self.hand = None
        self.ghost = set()

    def _append(self, node: Node):
        if self.tail is None:
            self.head = self.tail = node
            node.prev = node.next = node
            self.hand = node
        else:
            node.prev = self.tail
            node.next = self.head
            self.tail.next = node
            self.head.prev = node
            self.tail = node

    def _remove_node(self, node: Node):
        if node.next is node:  # single-node list
            self.head = self.tail = self.hand = None
        else:
            node.prev.next = node.next
            node.next.prev = node.prev
            if self.head is node:
                self.head = node.next
            if self.tail is node:
                self.tail = node.prev
            if self.hand is node:
                self.hand = node.next

        self.current_size -= node.size
        del self.nodes[node.obj_id]

    def on_hit(self, req: Request):
        node = self.nodes.get(req.obj_id)
        if node is not None:
            node.ref = 1

    def _evict_one(self):
        if self.hand is None:
            return None

        while True:
            node = self.hand
            if node.ref == 0:
                victim_id = node.obj_id
                self._remove_node(node)
                return victim_id
            node.ref = 0
            self.hand = node.next

    def on_miss(self, req: Request):
        size = req.obj_size

        # Never admit objects larger than the whole cache
        if size > self.cache_size / 4:
            return

        if req.obj_id not in self.ghost:
            self.ghost.add(req.obj_id)
            return

        # Already present: ignore duplicate insert
        if req.obj_id in self.nodes:
            return

        # Evict until the object fits
        while self.current_size + size > self.cache_size:
            if self._evict_one() is None:
                break

        node = Node(req.obj_id, size)
        self._append(node)
        self.nodes[req.obj_id] = node
        self.current_size += size

    def evict(self, req: Request):
        return self._evict_one()

    def on_remove(self, obj_id: int):
        node = self.nodes.get(obj_id)
        if node is not None:
            self._remove_node(node)

    def free(self):
        self.nodes.clear()
        self.head = self.tail = self.hand = None
        self.current_size = 0


def cache_init_hook(common_cache_params: CommonCacheParams):
    return SieveCache(common_cache_params.cache_size)


def cache_hit_hook(data: SieveCache, req: Request):
    data.on_hit(req)


def cache_miss_hook(data: SieveCache, req: Request):
    data.on_miss(req)


def cache_eviction_hook(data: SieveCache, req: Request):
    return data.evict(req)


def cache_remove_hook(data: SieveCache, obj_id: int):
    data.on_remove(obj_id)


def cache_free_hook(data: SieveCache):
    data.free()

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
