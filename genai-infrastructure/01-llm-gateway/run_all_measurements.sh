#!/bin/sh
# Runs every measurement in turn (they share Redis DB 11, so never in parallel).
export PYTHONIOENCODING=utf-8 PYTHONUNBUFFERED=1
python measure_retry_stacking.py --runs 20 > results/m3.log 2>&1; echo "m3 exit $?"
python measure_budget_race.py --runs 20 > results/m4.log 2>&1; echo "m4 exit $?"
python measure_failover.py --runs 10 > results/m2.log 2>&1; echo "m2 exit $?"
python measure_latency.py --rounds 10 > results/m1.log 2>&1; echo "m1 exit $?"
python measure_burst.py --runs 10 > results/m1b.log 2>&1; echo "m1b exit $?"
python measure_pool_burst.py > results/m1c.log 2>&1; echo "m1c exit $?"
