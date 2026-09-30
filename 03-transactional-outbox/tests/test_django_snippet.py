"""Django and Celery aren't installed here, so the Django example is only compiled."""
import ast
import pathlib

SOURCE = (pathlib.Path(__file__).resolve().parent.parent / "django_outbox.py").read_text()


def test_django_example_compiles():
    compile(SOURCE, "django_outbox.py", "exec")


def test_django_relay_uses_skip_locked_inside_atomic():
    tree = ast.parse(SOURCE)
    relay = next(f for f in tree.body if isinstance(f, ast.FunctionDef) and f.name == "relay_batch")
    calls = {n.func.attr: n for n in ast.walk(relay)
             if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)}
    assert "atomic" in calls
    assert any(k.arg == "skip_locked" and k.value.value is True
               for k in calls["select_for_update"].keywords)
    assert any(k.arg == "task_id" for k in calls["apply_async"].keywords)
