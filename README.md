# anuptechtips-code

Runnable code, tests and measurements behind the articles on [anuptechtips.com](https://anuptechtips.com/).

Every code block in an article comes from the folder listed for it here, and every number comes
from a script you can rerun. Each series has its own folder, with a README that says what you
need and how to run it.

| Series | Folder | What's inside |
|---|---|---|
| Reliable Python services | [reliable-python-services](reliable-python-services/) | Idempotency keys, rate limiting, the transactional outbox, idempotent consumers and Redis locks, built and tested on PostgreSQL, Redis and Kafka |
| GenAI infrastructure | [genai-infrastructure](genai-infrastructure/) | An LLM gateway (routing, retries, circuit breaker, streaming fallback, per-tenant budgets) and a production RAG pipeline on pgvector |
| AI System Design Case Studies | [ai-system-design](ai-system-design/) | Five public AI incidents (Air Canada's chatbot, agents deleting databases, fail open vs fail closed, the ChatGPT Redis bug, silent LLM regressions), each rebuilt and fixed |

More series are added as their articles are written.

Found a case that breaks one of these? Open an issue or a pull request.
