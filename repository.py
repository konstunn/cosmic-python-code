import abc
from typing import Sequence, Iterable

from sqlalchemy import text, Row, bindparam, column, table, select, insert, null, delete
from sqlalchemy.orm import Session

import model


class AbstractRepository(abc.ABC):
    @abc.abstractmethod
    def add(self, batch: model.Batch):
        raise NotImplementedError

    @abc.abstractmethod
    def get(self, reference) -> model.Batch:
        raise NotImplementedError


class SqlRepository(AbstractRepository):
    def __init__(self, session: Session):
        self.session = session

    def add(self, batch: model.Batch):
        batch_id = _select_batch_id_by_ref(self.session, batch.reference)
        # TODO: make sql exists query
        if batch_id is None:
            _insert_batch(self.session, batch)
        batch_id = _select_batch_id_by_ref(self.session, batch.reference)

        _delete_batch_allocations_if_exist(self.session, batch_id)

        allocations: set[model.OrderLine] = batch._allocations
        if allocations:
            _insert_order_lines(self.session, allocations)
            order_lines_ids = select_order_lines_ids_unallocated(self.session)
            _insert_allocations(self.session, order_lines_ids, batch_id)

    def get(self, reference) -> model.Batch:
        result = self.session.execute(
            text(
                """
                select 
                    b.id as batch_id,
                    b.reference as batch_reference,
                    b.sku as batch_sku,
                    b._purchased_quantity as batch_purchased_qty,
                    b.eta as batch_eta,
                    l.orderid as order_line_order_reference,
                    l.sku as order_line_sku,
                    l.qty as order_line_qty
                from 
                    batches as b
                join 
                    allocations as a
                on 
                    b.id = a.batch_id
                join 
                    order_lines as l
                on 
                    a.orderline_id = l.id
                where 
                    b.reference = :ref
                """
            ),
            dict(ref=reference),
        ).fetchall()
        batch_result = result[0]
        batch = model.Batch(
            ref=batch_result.batch_reference,
            sku=batch_result.batch_sku,
            qty=batch_result.batch_purchased_qty,
            eta=batch_result.batch_sku,
        )
        order_lines = [
            model.OrderLine(
                order_reference=r.order_line_order_reference,
                sku=r.order_line_sku,
                quantity=r.order_line_qty,
            )
            for r in result
        ]
        for l in order_lines:
            batch._allocations.add(l)
        return batch


def _select_batch_id_by_ref(session, batch_ref):
    result = session.execute(
        text(
            """
            select id from batches where reference = :batch_ref
            """
        ),
        dict(batch_ref=batch_ref),
    )
    row = result.fetchone()
    if row is not None:
        batch_id = row[0]
    else:
        batch_id = None
    return batch_id


def _insert_batch(session, batch: model.Batch):
    session.execute(
        text(
            """
            INSERT INTO batches (
                reference,
                sku,
               _purchased_quantity,
               eta
            ) VALUES (:ref, :sku, :qty, :eta)
            """
        ),
        dict(
            ref=batch.reference,
            sku=batch.sku,
            qty=batch._purchased_quantity,
            eta=batch.eta,
        ),
    )


def _insert_order_lines(session, lines: Iterable[model.OrderLine]):
    order_lines_rows = [
        dict(sku=line.sku, qty=line.quantity, ref=line.order_reference)
        for line in lines
    ]
    stmt = text(
        """
        insert into order_lines (
            sku,
            qty,
            orderid
        ) values (
            :sku, :qty, :ref
        )
        """
    )
    session.execute(
        stmt,
        order_lines_rows,
    )


def select_order_lines_ids_unallocated(
    session: Session,
) -> Iterable[int]:
    stmt = text(
        "select o.id "
        "from order_lines as o "
        "left outer join allocations as a "
        "on o.id = a.orderline_id "
        "where a.id is null"
    )
    result = session.execute(stmt)
    rows = result.fetchall()
    order_ids = [row.id for row in rows]
    return order_ids


def _insert_allocations(session: Session, order_lines_ids, batch_id):
    values = [
        dict(orderline_id=line_id, batch_id=batch_id) for line_id in order_lines_ids
    ]
    stmt = text(
        """
        insert into allocations (
            orderline_id,
            batch_id
        ) values (
            :orderline_id, :batch_id
        )
        """
    )
    session.execute(stmt, values)


def _delete_batch_allocations_if_exist(session, batch_id):
    result = session.execute(
        text(
            """
            select id, orderline_id from allocations where batch_id = :batch_id
            """
        ),
        dict(batch_id=batch_id),
    )
    rows: Iterable[Row] = result.fetchall()
    if rows:
        allocations_ids = [row.id for row in rows]
        order_line_ids = [row.orderline_id for row in rows]
        stmt = delete(table("order_lines")).where(column("id").in_(order_line_ids))
        session.execute(
            stmt
        )
        stmt = delete(table("allocations")).where(column("id").in_(allocations_ids))
        session.execute(
            stmt
        )
