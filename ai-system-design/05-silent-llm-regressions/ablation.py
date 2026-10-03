"""Prompt ablation: run the suite on the full system prompt, then again with each line
removed, and report the lines the prompt scores better without."""
from collections.abc import Sequence

from evals import evaluate
from fake_model import Model
from gate import Rate, worse


def ablate(model: Model, system_prompt: Sequence[str],
           sessions: Sequence[str]) -> dict[str | None, dict[str, Rate]]:
    """Scores per removed line; the key None is the full prompt."""
    prompt = list(system_prompt)
    results: dict[str | None, dict[str, Rate]] = {None: evaluate(model, prompt, sessions)}
    for i, line in enumerate(prompt):
        results[line] = evaluate(model, prompt[:i] + prompt[i + 1:], sessions)
    return results


def harmful_lines(results: dict[str | None, dict[str, Rate]]) -> list[tuple[str, str]]:
    """(line, suite) pairs where the full prompt is worse than the prompt without that line."""
    full = results[None]
    return [(line, suite) for line, scores in results.items() if line is not None
            for suite in scores if worse(full[suite], scores[suite])]
