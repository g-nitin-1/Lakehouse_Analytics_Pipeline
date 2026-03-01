from __future__ import annotations

from collections import OrderedDict
from heapq import nlargest
from typing import Generic, Hashable, TypeVar

K = TypeVar("K", bound=Hashable)
V = TypeVar("V")


class LRUCache(Generic[K, V]):
    def __init__(self, capacity: int = 128) -> None:
        if capacity < 1:
            raise ValueError("capacity must be >= 1")
        self.capacity = capacity
        self._data: OrderedDict[K, V] = OrderedDict()

    def get(self, key: K) -> V | None:
        if key not in self._data:
            return None
        self._data.move_to_end(key)
        return self._data[key]

    def put(self, key: K, value: V) -> None:
        if key in self._data:
            self._data.move_to_end(key)
        self._data[key] = value
        if len(self._data) > self.capacity:
            self._data.popitem(last=False)


class TopKCache:
    def __init__(self, capacity: int = 16) -> None:
        self._cache = LRUCache[str, list[tuple[str, float]]](capacity=capacity)
        self.hits = 0
        self.misses = 0

    def get_or_compute(
        self,
        key: str,
        rows: list[tuple[str, float]],
        k: int,
    ) -> list[tuple[str, float]]:
        cached = self._cache.get(key)
        if cached is not None:
            self.hits += 1
            return cached
        self.misses += 1
        computed = nlargest(k, rows, key=lambda pair: pair[1])
        self._cache.put(key, computed)
        return computed
