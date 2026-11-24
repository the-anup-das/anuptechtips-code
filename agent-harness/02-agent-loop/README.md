# Part 2: build the agent loop in Python

Code for "Build an Agent Loop in Python" (Agent harness series, part 2, anuptechtips.com).

A minimal agent harness: one local model in LM Studio, three tools (`run_tests`, `read_file`, `write_file`), a 10-turn limit, a check that keeps the agent inside the project folder, and a yes/no question before every write. Standard library only.

| File | What it is |
|---|---|
| `agent.py` | The harness: tool descriptions, the tools, the model call and the `run()` loop |
| `sample_project/` | A tiny project with one bug in `calc.py` and one failing test |
| `measure.py` | Runs the real agent N times on a fresh copy and writes `results/runs.json` |
| `tests/test_agent.py` | 9 tests with a fake model, no LM Studio needed |
| `results/runs.json` | The 10 runs used in the article (gpt-oss-20b) |
| `results/runs-gemma-*.json` | The same 10 runs on two Gemma models |
| `results/runs-say-no-once.json` | 10 runs where the first write is refused |

## Run it

Start LM Studio's server with a model it marks as trained for tool use (the article used `openai/gpt-oss-20b`; change `MODEL` in `agent.py` for another one).

```
python agent.py "Fix the failing test."          # asks before each write
python agent.py --yes "Fix the failing test."    # no questions
python measure.py 3
python measure.py 3 --say-no-once                 # refuse the first write
python measure.py 10 google/gemma-4-12b-qat       # another model loaded in LM Studio
python -m unittest discover -s tests -v
```

## Results in the article

10 runs, `openai/gpt-oss-20b`: 10 of 10 fixed the bug, all in 5 turns (run tests, read calc.py, write the fix, run tests, answer), median 13.7 s. Prompt tokens per turn: 235, 421, 527, 645, 690.

Same job, other models: `google/gemma-4-12b-qat` 10 of 10 fixed, 5 to 7 turns, median 21.3 s. `google/gemma-4-e2b` 10 of 10 fixed, 5 or 6 turns, median 24.1 s, but it ran the tests again after the fix in only 3 of 10 runs.

First write refused (`--say-no-once`, gpt-oss-20b): 4 of 10 retried and fixed it, 6 of 10 stopped with status `done` and the test still failing. `done` means the model stopped calling tools, not that the job worked.

## Be careful

`run_tests` runs the project's code, and `write_file` lets the model change that code. The yes/no question before each write is what stops the model from running code you didn't see. Keep it on. Working in a copy (`workdir/`) doesn't help, because the tests run with your access to the whole machine. `measure.py` says yes to every write, so only run it on this sample project.
