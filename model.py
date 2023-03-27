import dataclasses


class OutOfStock(Exception):
    ...


@dataclasses.dataclass(frozen=True)
class OrderLine:
    order_reference: str
    sku: str
    quantity: int


@dataclasses.dataclass
class Batch:
    def __init__(self, ref, sku, qty, eta):
        self.reference = ref
        self.sku = sku
        self.eta = eta
        self._purchased_quantity = qty
        self._allocations: set[OrderLine] = set()

    def __gt__(self, other):
        if self.eta is None:
            return False
        if other.eta is None:
            return True
        return self.eta > other.eta

    @property
    def available_quantity(self):
        return self._purchased_quantity - self.allocated_quantity

    @property
    def allocated_quantity(self):
        return sum([allocation.quantity for allocation in self._allocations])

    def allocate(self, line: OrderLine):
        if self.can_allocate(line):
            self._allocations.add(line)

    def deallocate(self, line):
        if line in self._allocations:
            self._allocations.remove(line)

    def can_allocate(self, line):
        return self.available_quantity >= line.quantity and self.sku == line.sku


def allocate(line: OrderLine, batches: list[Batch]):
    try:
        batch = next(b for b in sorted(batches) if b.can_allocate(line))
        batch.allocate(line)
        return batch.reference
    except StopIteration:
        raise OutOfStock(line.sku)
