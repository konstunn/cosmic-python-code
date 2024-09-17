from __future__ import annotations

from collections.abc import Collection
from dataclasses import dataclass
from datetime import date
from typing import Optional, List


class OutOfStock(Exception):
    pass


class NotAllocated(Exception):
    pass


def allocate(line: OrderLine, batches: List[Batch]) -> str:
    try:
        batch = next(b for b in sorted(batches) if b.can_allocate(line))
        batch.allocate(line)
        return batch.reference
    except StopIteration:
        raise OutOfStock(f"Out of stock for sku {line.sku}")


def deallocate(orderid, sku, batches):
    try:
        batch = next(b for b in batches if b.has_allocation(orderid, sku))
        line = batch.get_allocation_by_orderid(orderid)
        batch.deallocate(line)
        return batch.reference
    except StopIteration:
        raise NotAllocated(f"Not allocated for orderid {orderid}")


@dataclass(unsafe_hash=True)
class OrderLine:
    orderid: str
    sku: str
    qty: int


class AllocationsContainer:
    def __init__(self):
        self._allocations_by_orderid: dict[str, OrderLine] = dict()

    def add(self, line: OrderLine):
        self._allocations_by_orderid[line.orderid] = line

    def __repr__(self):
        return str(self._allocations_by_orderid.values())

    def __eq__(self, other: set):
        return set(self._allocations_by_orderid.values()) == other

    def __hash__(self):
        return hash(set(self._allocations_by_orderid.values()))

    def remove(self, line: OrderLine):
        del self._allocations_by_orderid[line.orderid]

    def __iter__(self):
        for a in self._allocations_by_orderid.values():
            yield a

    def __contains__(self, line: OrderLine) -> bool:
        return self.contains_allocation_with_orderid(line.orderid)

    def contains_allocation_with_orderid(self, orderid) -> bool:
        return orderid in self._allocations_by_orderid.keys()

    def get_allocation_by_orderid(self, orderid) -> OrderLine:
        if self.contains_allocation_with_orderid(orderid):
            return self._allocations_by_orderid[orderid]


class Batch:
    def __init__(self, ref: str, sku: str, qty: int, eta: Optional[date]):
        self.reference = ref
        self.sku = sku
        self.eta = eta
        self._purchased_quantity = qty
        self._allocations = AllocationsContainer()

    def __repr__(self):
        return f"<Batch {self.reference}>"

    def __eq__(self, other):
        if not isinstance(other, Batch):
            return False
        return other.reference == self.reference

    def __hash__(self):
        return hash(self.reference)

    def __gt__(self, other):
        if self.eta is None:
            return False
        if other.eta is None:
            return True
        return self.eta > other.eta

    def allocate(self, line: OrderLine):
        if self.can_allocate(line):
            self._allocations.add(line)

    def deallocate(self, line: OrderLine):
        if line in self._allocations:
            self._allocations.remove(line)

    @property
    def allocated_quantity(self) -> int:
        return sum(line.qty for line in self._allocations)

    @property
    def available_quantity(self) -> int:
        return self._purchased_quantity - self.allocated_quantity

    def can_allocate(self, line: OrderLine) -> bool:
        return self.sku == line.sku and self.available_quantity >= line.qty

    def has_allocation(self, orderid, sku):
        if self.sku != sku:
            return False
        return self._allocations.contains_allocation_with_orderid(orderid)

    def get_allocation_by_orderid(self, orderid):
        return self._allocations.get_allocation_by_orderid(orderid)

    @property
    def allocations(self):
        return [a for a in self._allocations]
