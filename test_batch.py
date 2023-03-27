from datetime import date, timedelta

from model import Batch, OrderLine, allocate

today = date.today()
tomorrow = today + timedelta(days=1)
later = tomorrow + timedelta(days=10)


def make_batch_and_line(sku, batch_qty, line_qty):
    return (
        Batch("batch-001", sku, batch_qty, eta=date.today()),
        OrderLine("order-123", sku, line_qty),
    )


def test_allocating_to_a_batch_reduces_the_available_quantity():
    batch = Batch("batch-001", "SMALL-TABLE", 20, date.today())
    line = OrderLine("order-ref", "SMALL-TABLE", 2)

    batch.allocate(line)

    assert batch.available_quantity == 18


def test_can_allocate_if_available_greater_than_required():
    batch, line = make_batch_and_line("SMALL-TABLE", 20, 2)

    assert batch.can_allocate(line) is True


def test_cannot_allocate_if_available_smaller_than_required():
    batch, line = make_batch_and_line("SMALL-TABLE", 1, 2)

    assert batch.can_allocate(line) is False


def test_can_allocate_if_available_equal_to_required():
    batch, line = make_batch_and_line("SMALL-TABLE", 2, 2)

    assert batch.can_allocate(line) is True


def test_cannot_allocate_if_skus_do_not_match():
    batch = Batch("batch-001", "SMALL-CHAIR", 10, date.today())
    different_sku_line = OrderLine("order-123", "LARGE-TABLE", 2)

    assert batch.can_allocate(different_sku_line) is False


def test_can_not_deallocate_unallocated_lines():
    batch, unallocated_line = make_batch_and_line("DECORATIVE-TRINKET", 20, 2)

    batch.deallocate(unallocated_line)

    assert batch.available_quantity == 20


def test_can_deallocate_allocated_lines():
    batch, line = make_batch_and_line("SMALL-TABLE", 20, 2)
    batch.allocate(line)

    batch.deallocate(line)

    assert batch.available_quantity == 20


def test_allocation_is_idempotent():
    batch, line = make_batch_and_line("ANGULAR-DESK", 20, 2)
    batch.allocate(line)
    batch.allocate(line)
    assert batch.available_quantity == 18
