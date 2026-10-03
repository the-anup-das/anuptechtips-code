"""Three layers that each retry three times: 27 calls for one click. The app's loop and
the openai SDK are the real ones; only the SDK's backoff is shortened to keep this fast.
(measure_retry_stacking.py repeats it with the SDK's own timings, 20 runs each.)"""
import openai
import pytest

from helpers import PROMPT, calls, control


def one_click(gateway_url: str, app_tries: int, **sdk_options) -> None:
    """What application code does around an SDK call: try, catch, try again."""
    client = openai.OpenAI(base_url=f"{gateway_url}/v1", api_key="vk-demo-refunds-acme",
                           **sdk_options)
    for _ in range(app_tries):
        try:
            client.chat.completions.create(model="summarize",
                                           messages=[{"role": "user", "content": PROMPT}])
            break
        except openai.APIStatusError:
            continue
    client.close()


@pytest.fixture
def down(mock_url, monkeypatch):
    monkeypatch.setattr(openai._base_client, "INITIAL_RETRY_DELAY", 0.01)
    control(mock_url, "alpha", "status", error="overloaded_503")


def test_app_times_sdk_times_gateway_is_27_upstream_calls(gateway, mock_url, down):
    gw = gateway(breaker=False, retry_hint=False)  # a gateway that leaves clients to it
    one_click(gw.url, app_tries=3)                 # the SDK's default: 2 retries
    assert calls(mock_url)["alpha"] == 27


def test_sdk_retries_off_leaves_9(gateway, mock_url, down):
    gw = gateway(breaker=False, retry_hint=False)
    one_click(gw.url, app_tries=3, max_retries=0)
    assert calls(mock_url)["alpha"] == 9


def test_the_gateway_alone_retrying_is_3(gateway, mock_url, down):
    gw = gateway(breaker=False, retry_hint=False)
    one_click(gw.url, app_tries=1, max_retries=0)
    assert calls(mock_url)["alpha"] == 3


def test_the_retry_hint_takes_out_the_sdk_layer_without_touching_the_client(gateway, mock_url, down):
    gw = gateway(breaker=False)                    # x-should-retry: false on the 503
    one_click(gw.url, app_tries=3)                 # an untouched, default SDK client
    assert calls(mock_url)["alpha"] == 9
