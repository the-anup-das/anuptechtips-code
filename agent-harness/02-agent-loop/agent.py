"""A minimal agent harness: one local model, three tools, a turn limit.

The model runs in LM Studio (OpenAI-compatible server on localhost:1234).
Standard library only. Run it with:

    python agent.py "Fix the failing test."

The agent works on a fresh copy of sample_project/ in workdir/, so every run
starts from the same bug.
"""
import json
import pathlib
import shutil
import subprocess
import sys
import urllib.request

URL = "http://localhost:1234/v1/chat/completions"
MODEL = "openai/gpt-oss-20b"
MAX_TURNS = 10
HERE = pathlib.Path(__file__).resolve().parent
WORKDIR = HERE / "workdir"

SYSTEM = ("You are a coding agent working in a small Python project. "
          "Use the tools to look at the code and run the tests. "
          "Run the tests again after you change a file. "
          "When the job is done, reply with a short summary and no tool call.")

TOOLS = [
    {"type": "function", "function": {
        "name": "run_tests",
        "description": "Run the project's unit tests and return the output.",
        "parameters": {"type": "object", "properties": {}}}},
    {"type": "function", "function": {
        "name": "read_file",
        "description": "Read a file in the project. Path is relative to the project root.",
        "parameters": {"type": "object",
                       "properties": {"path": {"type": "string"}},
                       "required": ["path"]}}},
    {"type": "function", "function": {
        "name": "write_file",
        "description": "Replace the whole content of a file in the project.",
        "parameters": {"type": "object",
                       "properties": {"path": {"type": "string"}, "content": {"type": "string"}},
                       "required": ["path", "content"]}}},
]


# ---- the tools (the harness runs them, never the model) ----

def inside_project(path):
    """Permission check: the agent may only touch files inside workdir/."""
    target = (WORKDIR / path).resolve()
    if not target.is_relative_to(WORKDIR.resolve()):
        raise PermissionError(f"{path} is outside the project")
    return target


def run_tests():
    result = subprocess.run([sys.executable, "-m", "unittest", "-q"], cwd=WORKDIR,
                            capture_output=True, text=True, timeout=60)
    return (result.stdout + result.stderr)[-2000:]


def read_file(path):
    return inside_project(path).read_text(encoding="utf-8")


def write_file(path, content, approve=None):
    target = inside_project(path)
    if approve and not approve(f"Write {len(content)} characters to {path}?"):
        return "The user did not approve this write."
    target.write_text(content, encoding="utf-8")
    return f"Wrote {path}."


def ask_user(question):
    return input(f"  {question} [y/N] ").strip().lower() == "y"


# ---- the model ----

def chat(messages):
    body = json.dumps({"model": MODEL, "messages": messages, "tools": TOOLS}).encode()
    request = urllib.request.Request(URL, body, {"Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=300) as response:
        return json.load(response)


# ---- the loop ----

def run(task, model=chat, max_turns=MAX_TURNS, approve=ask_user, log=print):
    tools = {"run_tests": run_tests, "read_file": read_file,
             "write_file": lambda **a: write_file(**a, approve=approve)}
    messages = [{"role": "system", "content": SYSTEM},
                {"role": "user", "content": task}]
    trace = []
    for turn in range(1, max_turns + 1):
        reply = model(messages)                    # 1-2. send the request, get a reply
        message = reply["choices"][0]["message"]
        calls = message.get("tool_calls") or []
        text = message.get("content") or ""
        tokens = reply.get("usage", {}).get("prompt_tokens")
        messages.append({"role": "assistant", "content": text, "tool_calls": calls}
                        if calls else {"role": "assistant", "content": text})
        if not calls:                              # no tool call: the model is done
            log(f"turn {turn} ({tokens} prompt tokens): answer")
            trace.append({"turn": turn, "prompt_tokens": tokens, "answer": text})
            return {"status": "done", "turns": turn, "answer": text, "trace": trace}
        for call in calls:                         # 3. the harness runs each tool
            name, args = call["function"]["name"], {}
            try:
                args = json.loads(call["function"]["arguments"] or "{}")
                result = tools[name](**args)
            except Exception as error:             # a failed tool goes back too
                result = f"Error: {type(error).__name__}: {error}"
            log(f"turn {turn} ({tokens} prompt tokens): {name}({', '.join(args)})"
                f" -> {result.splitlines()[-1][:60] if result else ''}")
            trace.append({"turn": turn, "prompt_tokens": tokens, "tool": name,
                          "args": list(args), "result": result[-300:]})
            messages.append({"role": "tool", "tool_call_id": call["id"],
                             "content": result})
    log(f"stopped: hit the limit of {max_turns} turns")   # 4. the turn limit
    return {"status": "max_turns", "turns": max_turns, "trace": trace}


def fresh_workdir():
    if WORKDIR.exists():
        shutil.rmtree(WORKDIR)
    shutil.copytree(HERE / "sample_project", WORKDIR)


if __name__ == "__main__":
    fresh_workdir()
    yes = "--yes" in sys.argv
    task = " ".join(a for a in sys.argv[1:] if a != "--yes") or "Fix the failing test."
    result = run(task, approve=(lambda q: True) if yes else ask_user)
    print("\n" + (result.get("answer") or result["status"]))
