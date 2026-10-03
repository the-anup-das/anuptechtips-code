"""Token counting for the mock providers: one token per whitespace-separated word.
A real gateway calls the provider's tokenizer or its token-counting endpoint here."""


def count_tokens(text: str) -> int:
    return len(text.split())


def count_message_tokens(messages: list[dict]) -> int:
    return sum(count_tokens(str(m.get("content") or "")) for m in messages)
