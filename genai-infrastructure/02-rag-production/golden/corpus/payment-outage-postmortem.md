# Payment outage postmortem

Checkout was down for 47 minutes on 12 May 2026.

## Root cause

A retry storm from the mobile app exhausted the database connection pool.

## Fix

The client now backs off with jitter, and the pool has a hard limit per service.
