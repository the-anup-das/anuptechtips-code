"""Django + Celery version of the outbox. Syntax-checked only (tests/test_django_snippet.py):
Django and Celery aren't installed in this repo's environment.

Assumes an app with an Order model, an OutboxEvent model on the same outbox
table as schema.sql, and a Celery task send_order_email.
"""
from django.db import transaction
from django.utils import timezone

from shop.models import Order, OutboxEvent
from shop.tasks import send_order_email


# Instead of this
def place_order_delay(customer_id: str, total_cents: int) -> Order:
    with transaction.atomic():
        order = Order.objects.create(customer_id=customer_id, total_cents=total_cents)
        send_order_email.delay(order.id)  # may run before COMMIT, or after a rollback
        return order


# Or this: runs after COMMIT, but is lost if the process dies right after it
def place_order_on_commit(customer_id: str, total_cents: int) -> Order:
    with transaction.atomic():
        order = Order.objects.create(customer_id=customer_id, total_cents=total_cents)
        send_order_email.delay_on_commit(order.id)
        return order


# Use this: the event row commits with the order
def place_order(customer_id: str, total_cents: int) -> Order:
    with transaction.atomic():
        order = Order.objects.create(customer_id=customer_id, total_cents=total_cents)
        OutboxEvent.objects.create(
            aggregatetype="order", aggregateid=str(order.id), type="OrderPlaced",
            payload={"order_id": str(order.id), "total_cents": total_cents},
        )
        return order


# ...and a relay (a management command in a loop) turns rows into Celery tasks
def relay_batch(batch_size: int = 100) -> int:
    with transaction.atomic():
        events = list(
            OutboxEvent.objects.select_for_update(skip_locked=True)
            .filter(published_at__isnull=True, dead_at__isnull=True,
                    next_attempt_at__lte=timezone.now())
            .order_by("next_attempt_at")[:batch_size]
        )
        for event in events:
            # task_id = the outbox id, so the worker can drop a redelivered task
            send_order_email.apply_async(args=[event.payload["order_id"]], task_id=str(event.id))
        OutboxEvent.objects.filter(id__in=[e.id for e in events]).update(published_at=timezone.now())
        return len(events)
