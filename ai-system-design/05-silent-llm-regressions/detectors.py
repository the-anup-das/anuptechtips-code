"""A detector for replies that contain letters from a script the prompt didn't use."""
import unicodedata


def scripts(text: str) -> set[str]:
    """The scripts of the letters in `text`, e.g. {'LATIN'} or {'LATIN', 'THAI'}."""
    return {unicodedata.name(ch, "UNKNOWN").split()[0] for ch in text if ch.isalpha()}


def unexpected_scripts(prompt: str, reply: str) -> set[str]:
    """Scripts in the reply that the prompt never used. Digits and punctuation don't count.
    A request to translate into another language is flagged too, so watch the rate, per slice."""
    return scripts(reply) - scripts(prompt)
