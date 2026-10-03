"""Structure-aware chunking: split on Markdown headings, pack whole paragraphs up to a size
limit, and put the title and section path in front of every chunk."""
import re

HEADING = re.compile(r"^(#{1,6})\s+(.+)$")
SENTENCE_END = re.compile(r"(?<=[.!?])\s+")


def chunk(title: str, text: str, max_words: int = 150) -> list[str]:
    """max_words is a stand-in for tokens: 150 words is roughly 200 tokens of English."""
    path: list[tuple[int, str]] = []  # (level, heading) for each heading above the paragraph
    parts: list[str] = []             # paragraphs (or sentences) collected for one chunk
    chunks: list[str] = []

    def flush() -> None:
        if parts:
            prefix = " / ".join([title, *(heading for _, heading in path)])
            chunks.append(f"{prefix}: {' '.join(parts)}")
            parts.clear()

    for block in re.split(r"\n\s*\n", text.strip()):
        lines = block.strip().splitlines()
        while lines and (match := HEADING.match(lines[0])):
            flush()  # a heading closes the chunk before it
            level = len(match.group(1))
            while path and path[-1][0] >= level:
                path.pop()
            path.append((level, match.group(2).strip()))
            lines.pop(0)
        paragraph = " ".join(" ".join(lines).split())
        if not paragraph:
            continue
        # A paragraph that is too long on its own is packed sentence by sentence.
        too_long = len(paragraph.split()) > max_words
        for piece in SENTENCE_END.split(paragraph) if too_long else [paragraph]:
            size = sum(len(part.split()) for part in parts)
            if parts and size + len(piece.split()) > max_words:
                flush()
            parts.append(piece)
    flush()
    return chunks
