"""Tests for the loop with a fake model: no LM Studio, no network.

    python -m unittest discover -s tests -v
"""
import json
import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
import agent  # noqa: E402


def tool_call(name, **args):
    return {"id": f"call_{name}", "type": "function",
            "function": {"name": name, "arguments": json.dumps(args)}}


def reply(content="", calls=None, prompt_tokens=100):
    message = {"role": "assistant", "content": content}
    if calls:
        message["tool_calls"] = calls
    return {"choices": [{"message": message}], "usage": {"prompt_tokens": prompt_tokens}}


class FakeModel:
    """Returns scripted replies in order and keeps every request it was sent."""

    def __init__(self, *replies):
        self.replies = list(replies)
        self.requests = []

    def __call__(self, messages):
        self.requests.append([dict(m) for m in messages])
        return self.replies.pop(0)


class AgentLoopTest(unittest.TestCase):
    def setUp(self):
        agent.fresh_workdir()

    def run_agent(self, model, **kw):
        return agent.run("Fix the failing test.", model=model, approve=lambda q: True, log=lambda *a: None, **kw)

    def test_stops_when_the_model_replies_without_a_tool_call(self):
        model = FakeModel(reply(calls=[tool_call("run_tests")]), reply("All good."))
        result = self.run_agent(model)
        self.assertEqual(result["status"], "done")
        self.assertEqual(result["turns"], 2)
        self.assertEqual(result["answer"], "All good.")

    def test_the_harness_stops_a_model_that_never_stops(self):
        model = FakeModel(*[reply(calls=[tool_call("run_tests")]) for _ in range(5)])
        result = self.run_agent(model, max_turns=3)
        self.assertEqual(result["status"], "max_turns")
        self.assertEqual(len(model.requests), 3)

    def test_tool_results_go_back_with_the_same_id(self):
        model = FakeModel(reply(calls=[tool_call("run_tests")]), reply("done"))
        self.run_agent(model)
        second_request = model.requests[1]
        self.assertEqual(second_request[-1]["role"], "tool")
        self.assertEqual(second_request[-1]["tool_call_id"], "call_run_tests")
        self.assertIn("FAILED", second_request[-1]["content"])

    def test_the_history_grows_every_turn(self):
        model = FakeModel(reply(calls=[tool_call("run_tests")]),
                          reply(calls=[tool_call("read_file", path="calc.py")]), reply("done"))
        self.run_agent(model)
        self.assertEqual([len(r) for r in model.requests], [2, 4, 6])

    def test_arguments_that_are_not_json_go_back_as_an_error(self):
        bad = {"id": "call_bad", "type": "function",
               "function": {"name": "read_file", "arguments": '{"path": "calc.py"'}}
        model = FakeModel(reply(calls=[bad]), reply("ok"))
        result = self.run_agent(model)
        self.assertEqual(result["status"], "done")
        self.assertIn("Error: JSONDecodeError", model.requests[1][-1]["content"])

    def test_paths_outside_the_project_are_refused(self):
        model = FakeModel(reply(calls=[tool_call("read_file", path="../agent.py")]), reply("ok"))
        self.run_agent(model)
        self.assertIn("PermissionError", model.requests[1][-1]["content"])

    def test_a_failing_tool_goes_back_to_the_model_as_an_error(self):
        model = FakeModel(reply(calls=[tool_call("read_file", path="missing.py")]), reply("ok"))
        result = self.run_agent(model)
        self.assertEqual(result["status"], "done")
        self.assertIn("Error: FileNotFoundError", model.requests[1][-1]["content"])

    def test_a_refused_write_does_not_change_the_file(self):
        before = (agent.WORKDIR / "calc.py").read_text()
        model = FakeModel(reply(calls=[tool_call("write_file", path="calc.py", content="x = 1\n")]), reply("ok"))
        agent.run("Fix it.", model=model, approve=lambda q: False, log=lambda *a: None)
        self.assertEqual((agent.WORKDIR / "calc.py").read_text(), before)
        self.assertIn("did not approve", model.requests[1][-1]["content"])

    def test_the_fixed_code_passes_the_tests(self):
        fixed = (agent.WORKDIR / "calc.py").read_text().replace("subtotal - discount", "subtotal * (1 - discount)")
        model = FakeModel(reply(calls=[tool_call("write_file", path="calc.py", content=fixed)]),
                          reply(calls=[tool_call("run_tests")]), reply("fixed"))
        self.run_agent(model)
        self.assertIn("OK", model.requests[2][-1]["content"])


if __name__ == "__main__":
    unittest.main()
