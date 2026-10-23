"""Consistent hash ring with virtual nodes.

Each physical node is placed on the ring many times ("virtual nodes") so keys spread
evenly and adding or removing a node only moves about 1/N of the keys.
"""

import bisect
import hashlib


def _hash(value: str) -> int:
    return int.from_bytes(hashlib.md5(value.encode()).digest()[:8], "big")


class HashRing:
    def __init__(self, node_ids=(), vnodes=64):
        self.vnodes = vnodes
        self._points = []  # sorted hash positions
        self._owners = {}  # position -> node id
        for node_id in node_ids:
            self.add(node_id)

    @property
    def nodes(self):
        return sorted(set(self._owners.values()))

    def add(self, node_id):
        for i in range(self.vnodes):
            point = _hash(f"{node_id}#{i}")
            if point in self._owners:  # astronomically rare collision; skip it
                continue
            self._owners[point] = node_id
            bisect.insort(self._points, point)

    def remove(self, node_id):
        self._points = [p for p in self._points if self._owners[p] != node_id]
        self._owners = {p: n for p, n in self._owners.items() if n != node_id}

    def preference_list(self, key):
        """All distinct nodes in clockwise order starting from the key's position.

        The first N are the key's "home" replicas; later ones are fallbacks when a
        home node is down.
        """
        if not self._points:
            return []
        start = bisect.bisect(self._points, _hash(key)) % len(self._points)
        seen, order = set(), []
        for i in range(len(self._points)):
            owner = self._owners[self._points[(start + i) % len(self._points)]]
            if owner not in seen:
                seen.add(owner)
                order.append(owner)
                if len(order) == len(set(self._owners.values())):
                    break
        return order
