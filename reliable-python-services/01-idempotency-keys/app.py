"""POST /charges, protected by an Idempotency-Key (FastAPI + psycopg 3 + PostgreSQL).

Run: uvicorn app:app --port 8000
"""
import os
import time
from collections.abc import Iterator

import psycopg
from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from idempotency import Result, fingerprint, run_once

DSN = os.environ.get("IDEM_DSN", "postgresql://patterns:patterns@localhost:55432/idem")
CHARGE_DELAY = float(os.environ.get("CHARGE_DELAY", "0.05"))  # seconds

app = FastAPI()


class ChargeIn(BaseModel):
    amount: int
    currency: str = "usd"


def get_conn() -> Iterator[psycopg.Connection]:
    # One connection per request keeps the example short; use psycopg_pool in production.
    with psycopg.connect(DSN, autocommit=True) as conn:
        yield conn


def parse_key(raw: str | None) -> str:
    if raw is None:
        raise HTTPException(400, "Idempotency-Key header is required")
    key = raw.strip()
    if len(key) >= 2 and key[0] == key[-1] == '"':  # the IETF draft sends a quoted string
        key = key[1:-1]
    if not 1 <= len(key) <= 255:
        raise HTTPException(400, "Idempotency-Key must be 1-255 characters")
    return key


def charge(conn: psycopg.Connection, client_id: str, amount: int,
           currency: str) -> tuple[int, dict]:
    time.sleep(CHARGE_DELAY)  # stand-in for the call to the card network
    (charge_id,) = conn.execute(
        "INSERT INTO charges (client_id, amount, currency) VALUES (%s, %s, %s) RETURNING id",
        (client_id, amount, currency),
    ).fetchone()
    return 201, {"id": charge_id, "amount": amount, "currency": currency}


def to_response(result: Result) -> JSONResponse:
    headers = {"Idempotent-Replayed": "true"} if result.replayed else None
    return JSONResponse(result.body, status_code=result.code, headers=headers)


@app.post("/charges")
def create_charge(
    body: ChargeIn,
    client_id: str = Header(alias="X-Client-Id"),  # stand-in for your auth layer
    # Optional on purpose: a missing required header would be FastAPI's own 422.
    idempotency_key: str | None = Header(default=None),
    conn: psycopg.Connection = Depends(get_conn),
) -> JSONResponse:
    key = parse_key(idempotency_key)
    fp = fingerprint("POST", "/charges", body.model_dump())
    result = run_once(conn, client_id, key, fp,
                      lambda c: charge(c, client_id, body.amount, body.currency))
    return to_response(result)
