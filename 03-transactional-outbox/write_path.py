"""The write side: the business row and its event commit in one transaction.

Nothing here talks to Kafka. The request path only writes to Postgres; the
relay publishes later.
"""
from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import create_engine, func, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column


class Base(DeclarativeBase):
    pass


class Order(Base):
    __tablename__ = "orders"
    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, server_default=text("uuidv7()"))
    customer_id: Mapped[str]
    total_cents: Mapped[int]


class OutboxEvent(Base):
    __tablename__ = "outbox"
    # Only the columns the app sets; the relay's bookkeeping columns have defaults.
    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, server_default=text("uuidv7()"))
    created_at: Mapped[datetime] = mapped_column(primary_key=True, server_default=func.now())
    aggregatetype: Mapped[str]
    aggregateid: Mapped[str]
    type: Mapped[str]
    payload: Mapped[dict] = mapped_column(JSONB)


def emit(session: Session, aggregatetype: str, aggregateid: str, event_type: str,
         payload: dict) -> None:
    """Queue an event in the caller's transaction. It commits or rolls back with it."""
    session.add(OutboxEvent(aggregatetype=aggregatetype, aggregateid=aggregateid,
                            type=event_type, payload=payload))


def place_order(engine, customer_id: str, total_cents: int) -> uuid.UUID:
    with Session(engine) as session, session.begin():  # one transaction, committed on exit
        order = Order(customer_id=customer_id, total_cents=total_cents)
        session.add(order)
        session.flush()  # sends the INSERT, so Postgres has assigned order.id
        order_id = order.id
        emit(session, "order", str(order_id), "OrderPlaced",
             {"order_id": str(order_id), "customer_id": customer_id, "total_cents": total_cents})
    return order_id


if __name__ == "__main__":
    from db import SQLALCHEMY_URL

    engine = create_engine(SQLALCHEMY_URL)
    print("placed order", place_order(engine, "cust-42", 1999))
