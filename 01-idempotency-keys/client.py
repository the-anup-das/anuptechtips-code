"""The client's half: make the key once per operation, then reuse it on every retry."""
import hashlib
import random
import time
import uuid

import httpx

RETRY_STATUSES = {409, 429, 502, 503, 504}


def new_key() -> str:
    return str(uuid.uuid4())  # random: store it with the operation before the first attempt


def key_for(action: str, *ids: object) -> str:
    """Deterministic: the same business action always gets the same key."""
    return hashlib.sha256(":".join([action, *map(str, ids)]).encode()).hexdigest()


def post_with_retries(http: httpx.Client, url: str, body: dict, key: str,
                      attempts: int = 5, base_delay: float = 0.2) -> httpx.Response:
    # The key is a parameter, so this loop can't make a new one per attempt.
    headers = {"Idempotency-Key": key}
    attempt = 1
    while True:
        try:
            resp = http.post(url, json=body, headers=headers)
            if resp.status_code not in RETRY_STATUSES or attempt == attempts:
                return resp
        except httpx.TransportError:  # timed out or reset: it may or may not have run
            if attempt == attempts:
                raise
        time.sleep(base_delay * 2 ** (attempt - 1) * random.uniform(0.5, 1.0))
        attempt += 1
