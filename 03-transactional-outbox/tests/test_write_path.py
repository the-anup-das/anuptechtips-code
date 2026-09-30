import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from db import SQLALCHEMY_URL
from write_path import Order, emit, place_order


@pytest.fixture
def engine(dsn):
    engine = create_engine(SQLALCHEMY_URL)
    yield engine
    engine.dispose()


def test_commit_writes_the_order_and_its_event(engine, conn):
    order_id = place_order(engine, "cust-42", 1999)

    assert conn.execute("SELECT count(*) FROM orders WHERE id = %s", (order_id,)).fetchone()[0] == 1
    row = conn.execute(
        "SELECT aggregatetype, aggregateid, type, payload, published_at, attempts FROM outbox"
    ).fetchall()
    assert row == [("order", str(order_id), "OrderPlaced",
                    {"order_id": str(order_id), "customer_id": "cust-42", "total_cents": 1999},
                    None, 0)]


def test_rollback_writes_neither(engine, conn):
    with pytest.raises(RuntimeError):
        with Session(engine) as session, session.begin():
            order = Order(customer_id="cust-42", total_cents=1999)
            session.add(order)
            session.flush()
            emit(session, "order", str(order.id), "OrderPlaced", {"order_id": str(order.id)})
            session.flush()  # both INSERTs have reached Postgres...
            raise RuntimeError("payment declined")  # ...and roll back together

    assert conn.execute("SELECT count(*) FROM orders").fetchone()[0] == 0
    assert conn.execute("SELECT count(*) FROM outbox").fetchone()[0] == 0


def test_ids_are_uuidv7(engine, conn):
    place_order(engine, "cust-1", 100)
    version = conn.execute("SELECT uuid_extract_version(id) FROM outbox").fetchone()[0]
    assert version == 7
