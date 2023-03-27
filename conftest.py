# pytest: disable=redefined-outer-name
from typing import Type

import pytest
from sqlalchemy import create_engine, Engine
from sqlalchemy.orm import sessionmaker, Session

from db_tables import metadata


@pytest.fixture
def in_memory_db() -> Engine:
    engine = create_engine("sqlite:///:memory:")
    metadata.create_all(engine)
    return engine


@pytest.fixture
def session(in_memory_db: Engine) -> Session:
    yield sessionmaker(bind=in_memory_db)()
