import httpx
import pytest

from pipeline.http import get_json


def mock_client(handler) -> httpx.Client:
    return httpx.Client(transport=httpx.MockTransport(handler), base_url="http://test")


def sequence_handler(*outcomes):
    """Returns a handler that plays one outcome per call: a status code or an exception."""
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        outcome = outcomes[min(len(calls), len(outcomes) - 1)]
        calls.append(request)
        if isinstance(outcome, Exception):
            raise outcome
        if outcome == 200:
            return httpx.Response(200, json={"ok": True})
        return httpx.Response(outcome)

    handler.calls = calls
    return handler


def test_success_needs_no_sleep():
    handler = sequence_handler(200)
    delays = []
    assert get_json(mock_client(handler), "/x", sleep=delays.append) == {"ok": True}
    assert len(handler.calls) == 1
    assert delays == []


def test_retries_503_then_succeeds():
    handler = sequence_handler(503, 503, 200)
    delays = []
    result = get_json(mock_client(handler), "/x", sleep=delays.append)
    assert result == {"ok": True}
    assert len(handler.calls) == 3
    assert len(delays) == 2


def test_backoff_delays_are_bounded_by_exponential_cap():
    handler = sequence_handler(503, 503, 503, 200)
    delays = []
    get_json(mock_client(handler), "/x", base_delay=1.0, sleep=delays.append)
    assert len(delays) == 3
    for attempt, delay in enumerate(delays, start=1):
        assert 0 <= delay <= 2 ** (attempt - 1)


def test_raises_runtime_error_after_max_attempts():
    handler = sequence_handler(503)
    delays = []
    with pytest.raises(RuntimeError, match="failed after 3 attempts"):
        get_json(mock_client(handler), "/x", attempts=3, sleep=delays.append)
    assert len(handler.calls) == 3
    assert len(delays) == 2  # no sleep after the final attempt


def test_does_not_retry_404():
    handler = sequence_handler(404)
    delays = []
    with pytest.raises(httpx.HTTPStatusError):
        get_json(mock_client(handler), "/x", sleep=delays.append)
    assert len(handler.calls) == 1
    assert delays == []


def test_retries_connect_error():
    handler = sequence_handler(httpx.ConnectError("refused"), 200)
    delays = []
    assert get_json(mock_client(handler), "/x", sleep=delays.append) == {"ok": True}
    assert len(handler.calls) == 2
    assert len(delays) == 1


def test_persistent_connect_error_raises_runtime_error():
    handler = sequence_handler(httpx.ConnectError("refused"))
    with pytest.raises(RuntimeError, match="ConnectError"):
        get_json(mock_client(handler), "/x", attempts=2, sleep=lambda delay: None)


def test_passes_params():
    seen = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(dict(request.url.params))
        return httpx.Response(200, json=[])

    get_json(mock_client(handler), "/x", params={"limit": 5})
    assert seen == [{"limit": "5"}]
