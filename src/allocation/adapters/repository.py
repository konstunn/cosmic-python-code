import abc
from typing import List

import sqlalchemy.exc
import typing

from allocation.domain import model


class AbstractRepository(abc.ABC):
    def add(self, batch: model.Product):
        raise NotImplementedError

    @abc.abstractmethod
    def get(self, reference) -> model.Product:
        raise NotImplementedError


class SqlAlchemyRepository(AbstractRepository):
    def __init__(self, session):
        self.session = session

    def add(self, batch):
        self.session.add(batch)

    def get(self, sku) -> typing.Optional[model.Product]:
        try:
            return self.session.query(model.Product).filter_by(sku=sku).one()
        except sqlalchemy.exc.NoResultFound:
            return None

    def list(self) -> List[model.Product]:
        return self.session.query(model.Product).all()


class ProductNotFound(Exception):
    ...
