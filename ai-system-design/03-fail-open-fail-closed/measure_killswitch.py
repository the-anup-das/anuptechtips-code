"""M5: the kill-switch matrix. Every action, with the switch off and on, through the naive
engine and the fixed one. Pure Python, no services.

Writes results/m5_killswitch_matrix.csv.
"""
import csv
import pathlib

from killswitch import Action, Rule, evaluate, evaluate_naive

HERE = pathlib.Path(__file__).resolve().parent


def rulesets(action: Action) -> dict[str, list[Rule]]:
    target = "test-rules" if action is Action.EXECUTE else None
    return {"root": [Rule("r1", action, ruleset=target)],
            "test-rules": [Rule("t1", Action.LOG)]}


def outcome(engine, action: Action, killed: bool) -> str:
    try:
        engine(rulesets(action), "root", {"r1"} if killed else set())
        return "ok"
    except Exception as exc:
        return f"{type(exc).__name__}: {exc}"


def main() -> None:
    rows = [{"action": action.value, "kill_switch": "on" if killed else "off",
             "naive_engine": outcome(evaluate_naive, action, killed),
             "fixed_engine": outcome(evaluate, action, killed)}
            for action in Action for killed in (False, True)]
    out = HERE / "results"
    out.mkdir(exist_ok=True)
    with open(out / "m5_killswitch_matrix.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    for row in rows:
        print(f"{row['action']:8} kill switch {row['kill_switch']:3}  "
              f"naive: {row['naive_engine']:62} fixed: {row['fixed_engine']}")
    broken = [row for row in rows if row["naive_engine"] != "ok"]
    print(f"naive engine: {len(broken)} of {len(rows)} cells fail; "
          f"fixed engine: {sum(row['fixed_engine'] != 'ok' for row in rows)} of {len(rows)}")


if __name__ == "__main__":
    main()
