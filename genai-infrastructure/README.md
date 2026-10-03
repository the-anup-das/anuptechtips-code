# GenAI infrastructure

Part of [anuptechtips-code](../README.md), the code behind the articles on anuptechtips.com.

Runnable code, tests and measurements for the GenAI infrastructure posts on
[anuptechtips.com](https://anuptechtips.com/system-design/): the services that sit between
your application and a language model, built and tested in Python with Redis and PostgreSQL.

Every code block in the posts comes from these folders, and every number in the posts comes
from the `measure_*.py` scripts here. The raw results are in each folder's `results/`.
The diagrams and charts are published on the blog only.

| Folder | Post |
|---|---|
| [01-llm-gateway](01-llm-gateway/) | [LLM Gateway Architecture: Routing, Caching, Fallbacks and Budgets](https://anuptechtips.com/llm-gateway-architecture/) |
| [02-rag-production](02-rag-production/) | [RAG Architecture in Production: A Practical Guide for Engineers](https://anuptechtips.com/rag-architecture-production/) |

Until a post is published, its link returns 404.

No model and no API key is needed. In the gateway folder a mock plays every provider and fails
on command; the RAG folder uses a deterministic hashing stand-in for the embedding model and
seeded synthetic vectors for its measurements.

## Run it

You need Docker and Python 3.12+.

- **LLM gateway:** Redis on `localhost:56379`, from `docker compose up -d` in
  [`../reliable-python-services/`](../reliable-python-services/). It uses Redis DB 11.
- **RAG:** PostgreSQL 18 with pgvector 0.8.6 on `localhost:55433`, database `rag`, from
  `docker compose up -d` in this folder ([docker-compose.yml](docker-compose.yml)).
  Local test credentials only.

Each folder's README lists what to `pip install`, its tests and its measurements.
`docker compose down -v` removes the services and their data.

## About the numbers

The posts' measurements ran on one machine:
- AMD Ryzen 9 7950X3D (16 cores/32 threads), 64 GB RAM;
- Windows 11, Docker Desktop 28.5 (WSL2);
- the services in Docker, Python on the host.

Your absolute latencies will differ. The comparisons are what matter: calls made or refused,
money spent against a budget, which setting answers faster.

Timing runs take a cross-process lock (`common/measure_lock.py`), so two benchmarks never overlap.

Found a case that breaks one of these? Open an issue or a pull request.
