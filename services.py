from __future__ import annotations

import model
from model import OrderLine
from repository import AbstractRepository


class InvalidSku(Exception):
    pass


def is_valid_sku(sku, batches):
    return sku in {b.sku for b in batches}


def allocate(line: OrderLine, repo: AbstractRepository, session) -> str:
    batches = repo.list()
    if not is_valid_sku(line.sku, batches):
        raise InvalidSku(f"Invalid sku {line.sku}")
    batchref = model.allocate(line, batches)
    session.commit()
    return batchref


def add_batch(batch_ref, sku, qty, eta, repo, session):
    batch = model.Batch(batch_ref, sku, qty, eta)
    repo.add(batch)
    session.commit()
    return batch.reference


def deallocate(orderid, sku, repo: AbstractRepository, session):
    batches = repo.list()
    batchref = model.deallocate(orderid, sku, batches)
    session.commit()
    return batchref


def get_batches_with_allocations(repo, session):
    batches: list[model.Batch] = repo.list()
    return [
        {
            "batchref": b.reference,
            "sku": b.sku,
            "eta": b.eta,
            "allocations": [
                {"orderid": a.orderid, "sku": a.sku, "qty": a.qty}
                for a in b.allocations
            ],
        }
        for b in batches
    ]
