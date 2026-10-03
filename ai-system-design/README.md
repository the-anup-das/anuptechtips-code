# AI System Design Case Studies

Part of [anuptechtips-code](../README.md), the code behind the articles on anuptechtips.com.

Runnable code, tests and measurements for the **AI System Design Case Studies** series on
[anuptechtips.com](https://anuptechtips.com/system-design/). Each post takes a public incident
from a real company, rebuilds the failure on a small scale, then builds and tests the design
that stops it.

Every code block in the posts comes from these folders, and every number in the posts comes
from the `measure_*.py` scripts here. The raw results are in each folder's `results/`.
The diagrams and charts are published on the blog only.

| Part | Folder | Post |
|---|---|---|
| 1 | [01-chatbot-hallucination](01-chatbot-hallucination/) | [Air Canada Chatbot Hallucination: Why Support Bots Invent Policy, and the Design That Stops It](https://anuptechtips.com/air-canada-chatbot-hallucination/) |
| 2 | [02-agent-deleted-database](02-agent-deleted-database/) | [AI Agent Deleted the Production Database: 3 Real Cases and the Design That Stops It](https://anuptechtips.com/ai-agent-deleted-production-database/) |
| 3 | [03-fail-open-fail-closed](03-fail-open-fail-closed/) | [Fail Open vs Fail Closed: What Cloudflare and OpenAI Learned When Generated Config Broke](https://anuptechtips.com/fail-open-vs-fail-closed/) |
| 4 | [04-asyncio-cancellation](04-asyncio-cancellation/) | [The ChatGPT Redis Bug: How One Cancelled Request Leaked Another User's Data](https://anuptechtips.com/chatgpt-redis-bug-asyncio-cancellation/) |
| 5 | [05-silent-llm-regressions](05-silent-llm-regressions/) | [Silent LLM Regressions: Anthropic and OpenAI Postmortems](https://anuptechtips.com/silent-llm-regressions/) |

Until a post is published, its link returns 404.

No model, no API key and no cloud account is needed. The models and agents are seeded,
deterministic stand-ins, and the destructive steps in part 2 are `DROP TABLE` statements on
throwaway test tables. Each folder's README says exactly what is simulated.

## Run it

You need Docker and Python 3.12+. Every part uses the services from
[`../reliable-python-services/`](../reliable-python-services/) (`docker compose up -d` there):

- PostgreSQL: `postgresql://patterns:patterns@localhost:55432`. Local test credentials only.
- Redis: `localhost:56379`.
- Kafka: `localhost:59092`.

Each part uses its own database, Redis DB index or topic prefix, and its README lists what to
`pip install`, its tests and its measurements. Part 4 can also run an old, vulnerable redis-py
(4.5.1) to reproduce the bug: install it only in a throwaway virtual environment, as its README
explains.

`docker compose down -v` removes the services and their data.

## About the numbers

The posts' measurements ran on one machine:
- AMD Ryzen 9 7950X3D (16 cores/32 threads), 64 GB RAM;
- Windows 11, Docker Desktop 28.5 (WSL2);
- the services in Docker, Python on the host.

Your timings will differ. The counts are what matter: wrong answers sent, rows deleted,
requests let through, data handed to the wrong user.

Timing runs take a cross-process lock (`common/measure_lock.py`), so two benchmarks never overlap.

Found a case that breaks one of these? Open an issue or a pull request.
