"""Run the real agent several times on the same bug and save what happened.

Needs LM Studio's server running with the model loaded.

    python measure.py                                  # 10 runs, writes results/runs.json
    python measure.py 3                                # 3 runs
    python measure.py 10 google/gemma-4-12b-qat        # another model
    python measure.py 10 --say-no-once                 # refuse the first write, allow the rest
"""
import json
import pathlib
import statistics
import sys
import time

import agent

args = [a for a in sys.argv[1:] if a != "--say-no-once"]
SAY_NO_ONCE = "--say-no-once" in sys.argv
RUNS = int(args[0]) if args else 10
DEFAULT_MODEL = agent.MODEL
if len(args) > 1:
    agent.MODEL = args[1]


def tests_pass():
    out = agent.run_tests()
    return out.strip().endswith("OK")


def approver():
    asked = []

    def approve(question):
        asked.append(question)
        return not (SAY_NO_ONCE and len(asked) == 1)
    return approve


def main():
    runs = []
    for i in range(1, RUNS + 1):
        agent.fresh_workdir()
        start = time.perf_counter()
        try:
            result = agent.run("Fix the failing test.", approve=approver(), log=lambda *a: None)
        except Exception as error:                 # the harness itself broke
            result = {"status": f"crashed: {type(error).__name__}: {error}"[:200], "turns": None, "trace": []}
        seconds = round(time.perf_counter() - start, 1)
        passed = tests_pass()
        tools = [step["tool"] for step in result["trace"] if "tool" in step]
        tokens = [step["prompt_tokens"] for step in result["trace"]]
        runs.append({"run": i, "status": result["status"], "turns": result["turns"], "tests_pass": passed,
                     "tool_calls": tools, "prompt_tokens_per_turn": tokens, "seconds": seconds,
                     "answer": result.get("answer")})
        print(f"run {i}: {result['status']}, {result['turns']} turns, tests pass: {passed}, "
              f"tools: {tools}, prompt tokens: {tokens}, {seconds}s")

    done = [r for r in runs if r["turns"]]
    summary = {
        "model": agent.MODEL, "runs": RUNS, "max_turns": agent.MAX_TURNS, "say_no_once": SAY_NO_ONCE,
        "fixed": sum(r["tests_pass"] for r in runs),
        "said_done_but_tests_fail": sum(r["status"] == "done" and not r["tests_pass"] for r in runs),
        "hit_max_turns": sum(r["status"] == "max_turns" for r in runs),
        "crashed": sum(r["status"].startswith("crashed") for r in runs),
        "turns_median": statistics.median(r["turns"] for r in done) if done else None,
        "seconds_median": statistics.median(r["seconds"] for r in runs),
    }
    name = "runs"
    if agent.MODEL != DEFAULT_MODEL:
        name += "-" + agent.MODEL.split("/")[-1]
    if SAY_NO_ONCE:
        name += "-say-no-once"
    out = pathlib.Path(__file__).resolve().parent / "results"
    out.mkdir(exist_ok=True)
    (out / f"{name}.json").write_text(json.dumps({"summary": summary, "runs": runs}, indent=1))
    print(json.dumps(summary, indent=1))


if __name__ == "__main__":
    main()
