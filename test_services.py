import pytest
import model
import repository
import services


class FakeRepository(repository.AbstractRepository):
    def __init__(self, batches):
        self._batches = set(batches)

    def add(self, batch):
        self._batches.add(batch)

    def get(self, reference):
        return next(b for b in self._batches if b.reference == reference)

    def list(self):
        return list(self._batches)


class FakeSession:
    committed = False

    def commit(self):
        self.committed = True


def test_returns_allocation():
    line = model.OrderLine("o1", "COMPLICATED-LAMP", 10)
    batch = model.Batch("b1", "COMPLICATED-LAMP", 100, eta=None)
    repo = FakeRepository([batch])

    result = services.allocate(line, repo, FakeSession())
    assert result == "b1"


def test_error_for_invalid_sku():
    line = model.OrderLine("o1", "NONEXISTENTSKU", 10)
    batch = model.Batch("b1", "AREALSKU", 100, eta=None)
    repo = FakeRepository([batch])

    with pytest.raises(services.InvalidSku, match="Invalid sku NONEXISTENTSKU"):
        services.allocate(line, repo, FakeSession())


def test_commits():
    line = model.OrderLine("o1", "OMINOUS-MIRROR", 10)
    batch = model.Batch("b1", "OMINOUS-MIRROR", 100, eta=None)
    repo = FakeRepository([batch])
    session = FakeSession()

    services.allocate(line, repo, session)
    assert session.committed is True


def test_deallocate_decrements_available_quantity():
    repo, session = FakeRepository([]), FakeSession()
    services.add_batch("b1", "BLUE-PLINTH", 100, None, repo, session)
    line = model.OrderLine("o1", "BLUE-PLINTH", 10)
    services.allocate(line, repo, session)
    batch = repo.get(reference="b1")
    assert batch.available_quantity == 90

    services.deallocate(line.orderid, line.sku, repo, session)
    assert batch.available_quantity == 100
    assert session.committed is True


def test_deallocate_decrements_correct_quantity():
    repo, session = FakeRepository([]), FakeSession()
    services.add_batch("b1", "BLUE-CHAIR", 100, None, repo, session)
    services.add_batch("b2", "RED-TABLE", 100, None, repo, session)
    line = model.OrderLine("o1", "RED-TABLE", 10)
    services.allocate(line, repo, session)
    batch = repo.get("b2")
    assert batch.available_quantity == 90

    session.committed = False
    services.deallocate(line.orderid, line.sku, repo, session)
    assert batch.available_quantity == 100
    assert session.committed is True

    another_batch = repo.get("b1")
    assert another_batch.available_quantity == 100


def test_trying_to_deallocate_unallocated_batch():
    repo, session = FakeRepository([]), FakeSession()
    services.add_batch("b1", "BLUE-TABLE", 100, None, repo, session)
    line = model.OrderLine("o1", "BLUE-TABLE", 10)

    with pytest.raises(model.NotAllocated, match="Not allocated for orderid o1"):
        services.deallocate(line.orderid, line.sku, repo, session)

    batch = repo.get("b1")
    assert batch.available_quantity == 100


def test_get_batches_with_allocations():
    repo, session = FakeRepository([]), FakeSession()

    services.add_batch("b1", "BLUE-TABLE", 100, None, repo, session)
    line = model.OrderLine("o1", "BLUE-TABLE", 10)
    services.allocate(line, repo, session)

    data = services.get_batches_with_allocations(repo, session)
    assert data == [
        {
            "batchref": "b1",
            "sku": "BLUE-TABLE",
            "eta": None,
            "allocations": [{"orderid": "o1", "sku": "BLUE-TABLE", "qty": 10}],
        }
    ]