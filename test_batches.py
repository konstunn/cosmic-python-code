from datetime import date

import model
from model import Batch, OrderLine


def test_allocating_to_a_batch_reduces_the_available_quantity():
    batch = Batch("batch-001", "SMALL-TABLE", qty=20, eta=date.today())
    line = OrderLine("order-ref", "SMALL-TABLE", 2)

    batch.allocate(line)

    assert batch.available_quantity == 18


def make_batch_and_line(sku, batch_qty, line_qty):
    return (
        Batch("batch-001", sku, batch_qty, eta=date.today()),
        OrderLine("order-123", sku, line_qty),
    )


def test_can_allocate_if_available_greater_than_required():
    large_batch, small_line = make_batch_and_line("ELEGANT-LAMP", 20, 2)
    assert large_batch.can_allocate(small_line)


def test_cannot_allocate_if_available_smaller_than_required():
    small_batch, large_line = make_batch_and_line("ELEGANT-LAMP", 2, 20)
    assert small_batch.can_allocate(large_line) is False


def test_can_allocate_if_available_equal_to_required():
    batch, line = make_batch_and_line("ELEGANT-LAMP", 2, 2)
    assert batch.can_allocate(line)


def test_cannot_allocate_if_skus_do_not_match():
    batch = Batch("batch-001", "UNCOMFORTABLE-CHAIR", 100, eta=None)
    different_sku_line = OrderLine("order-123", "EXPENSIVE-TOASTER", 10)
    assert batch.can_allocate(different_sku_line) is False


def test_allocation_is_idempotent():
    batch, line = make_batch_and_line("ANGULAR-DESK", 20, 2)
    batch.allocate(line)
    batch.allocate(line)
    assert batch.available_quantity == 18


def test_deallocate():
    batch, line = make_batch_and_line("EXPENSIVE-FOOTSTOOL", 20, 2)
    batch.allocate(line)
    batch.deallocate(line)
    assert batch.available_quantity == 20


def test_can_only_deallocate_allocated_lines():
    batch, unallocated_line = make_batch_and_line("DECORATIVE-TRINKET", 20, 2)
    batch.deallocate(unallocated_line)
    assert batch.available_quantity == 20


def test_batch_has_allocation_wrong_orderid():
    batch, line = make_batch_and_line("DIRTY-CHAIR", 20, 2)
    batch.allocate(line)
    unallocated_line = model.OrderLine("order-456", "DIRTY-CHAIR", 5)

    assert batch.has_allocation(line.orderid, line.sku) is True
    assert batch.has_allocation(unallocated_line.orderid, unallocated_line.sku) is False


def test_batch_has_allocation_wrong_sku():
    batch, line = make_batch_and_line("DIRTY-CHAIR", 20, 2)
    batch.allocate(line)
    another_sku_line = model.OrderLine("order-123", "DIRTY-TABLE", 5)

    assert batch.has_allocation(line.orderid, line.sku) is True
    assert batch.has_allocation(another_sku_line.orderid, another_sku_line.sku) is False


def test_batch_get_allocation():
    batch, line = make_batch_and_line("DIRTY-CHAIR", 20, 2)
    unallocated_line = model.OrderLine("order-456", "DIRTY-CHAIR", 5)

    assert batch.get_allocation_by_orderid(unallocated_line.orderid) is None

    batch.allocate(line)

    assert batch.get_allocation_by_orderid(line.orderid) == line


def test_allocations_container_add():
    container = model.AllocationsContainer()
    line1 = model.OrderLine("o1", "BLUE-TABLE", 1)

    assert line1 not in container

    container.add(line1)
    assert line1 in container


def test_allocations_container_remove():
    container = model.AllocationsContainer()
    line1 = model.OrderLine("o1", "BLUE-TABLE", 1)
    container.add(line1)

    container.remove(line1)
    assert line1 not in container


def test_allocations_container_iter():
    container = model.AllocationsContainer()
    line1 = model.OrderLine("o1", "BLUE-TABLE", 1)
    line2 = model.OrderLine("o2", "BLUE-TABLE", 2)

    container.add(line1)
    container.add(line2)

    assert line1 in [_ for _ in container]
    assert line2 in [_ for _ in container]

    container.remove(line2)
    assert line2 not in [_ for _ in container]
