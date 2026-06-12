"""
Tests for zerodb-dagster ZeroDBSensor.

Uses mocked HTTP responses -- no real API calls.
"""

import os
from unittest.mock import MagicMock, patch

import pytest

os.environ.setdefault("ZERODB_API_KEY", "test-key-000")
os.environ.setdefault("ZERODB_PROJECT_ID", "test-project-000")

from zerodb_dagster.sensor import ZeroDBSensor, ZeroDBEvent


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_session():
    with patch("zerodb_dagster.sensor.requests.Session") as MockSession:
        session = MagicMock()
        MockSession.return_value = session
        yield session


@pytest.fixture
def sensor(mock_session):
    return ZeroDBSensor(
        event_type="zerodb.table.row_inserted",
        api_key="test-key",
        project_id="test-project",
    )


# ---------------------------------------------------------------------------
# ZeroDBEvent
# ---------------------------------------------------------------------------

class TestZeroDBEvent:
    def test_from_dict_standard(self):
        d = {
            "event_type": "zerodb.table.row_inserted",
            "event_id": "evt-1",
            "data": {"row": {"id": 1}},
            "timestamp": "2026-06-10T00:00:00Z",
            "project_id": "proj-1",
            "metadata": {"source": "test"},
        }
        event = ZeroDBEvent.from_dict(d)
        assert event.event_type == "zerodb.table.row_inserted"
        assert event.event_id == "evt-1"
        assert event.data == {"row": {"id": 1}}

    def test_from_dict_alternate_keys(self):
        d = {"type": "zerodb.file.uploaded", "id": "evt-2", "payload": {"path": "a.csv"}}
        event = ZeroDBEvent.from_dict(d)
        assert event.event_type == "zerodb.file.uploaded"
        assert event.event_id == "evt-2"
        assert event.data == {"path": "a.csv"}

    def test_from_dict_empty(self):
        event = ZeroDBEvent.from_dict({})
        assert event.event_type == ""
        assert event.data == {}

    def test_to_dict(self):
        event = ZeroDBEvent(event_type="zerodb.custom", event_id="e1", data={"k": "v"})
        d = event.to_dict()
        assert d["event_type"] == "zerodb.custom"
        assert d["data"] == {"k": "v"}

    def test_to_run_request(self):
        event = ZeroDBEvent(event_type="zerodb.table.insert", event_id="evt-run")
        rr = event.to_run_request()
        assert rr["run_key"] == "evt-run"
        assert "run_config" in rr
        assert rr["run_config"]["ops"]["zerodb_event"]["config"]["event_type"] == "zerodb.table.insert"

    def test_defaults(self):
        event = ZeroDBEvent()
        assert event.event_type == ""
        assert event.data == {}
        assert event.metadata == {}


# ---------------------------------------------------------------------------
# Constructor
# ---------------------------------------------------------------------------

class TestConstructor:
    def test_event_type(self, sensor):
        assert sensor.event_type == "zerodb.table.row_inserted"

    def test_not_running_initially(self, sensor):
        assert sensor.is_running is False

    def test_no_handlers(self, sensor):
        assert sensor.handler_count == 0

    def test_no_last_event_id(self, sensor):
        assert sensor.last_event_id is None

    def test_custom_poll_interval(self, mock_session):
        s = ZeroDBSensor(event_type="x", api_key="k", project_id="p", poll_interval=15)
        assert s._poll_interval == 15

    def test_custom_batch_size(self, mock_session):
        s = ZeroDBSensor(event_type="x", api_key="k", project_id="p", batch_size=25)
        assert s._batch_size == 25

    def test_custom_base_url(self, mock_session):
        s = ZeroDBSensor(event_type="x", api_key="k", project_id="p",
                         base_url="https://custom.example.com")
        assert s._base_url == "https://custom.example.com"


# ---------------------------------------------------------------------------
# Handler registration
# ---------------------------------------------------------------------------

class TestHandlerRegistration:
    def test_on_event_decorator(self, sensor):
        @sensor.on_event
        def handler(event):
            pass
        assert sensor.handler_count == 1

    def test_add_handler(self, sensor):
        sensor.add_handler(lambda e: None)
        assert sensor.handler_count == 1

    def test_multiple_handlers(self, sensor):
        sensor.add_handler(lambda e: None)
        sensor.add_handler(lambda e: None)
        sensor.add_handler(lambda e: None)
        assert sensor.handler_count == 3

    def test_decorator_returns_original(self, sensor):
        def handler(event):
            return "x"
        assert sensor.on_event(handler) is handler


# ---------------------------------------------------------------------------
# Polling
# ---------------------------------------------------------------------------

class TestPolling:
    def test_poll_returns_events(self, sensor, mock_session):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "events": [
                {"event_type": "zerodb.table.row_inserted", "event_id": "e1", "data": {"id": 1}},
            ]
        }
        mock_resp.raise_for_status = MagicMock()
        mock_session.get.return_value = mock_resp

        events = sensor.poll()
        assert len(events) == 1
        assert events[0].event_id == "e1"

    def test_poll_updates_cursor(self, sensor, mock_session):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "events": [{"event_type": "x", "event_id": "cursor-42", "data": {}}]
        }
        mock_resp.raise_for_status = MagicMock()
        mock_session.get.return_value = mock_resp

        sensor.poll()
        assert sensor.last_event_id == "cursor-42"

    def test_poll_empty(self, sensor, mock_session):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"events": []}
        mock_resp.raise_for_status = MagicMock()
        mock_session.get.return_value = mock_resp

        assert sensor.poll() == []

    def test_poll_error_returns_empty(self, sensor, mock_session):
        import requests
        mock_session.get.side_effect = requests.RequestException("timeout")
        assert sensor.poll() == []

    def test_poll_list_format(self, sensor, mock_session):
        mock_resp = MagicMock()
        mock_resp.json.return_value = [
            {"event_type": "x", "event_id": "e-list", "data": {}}
        ]
        mock_resp.raise_for_status = MagicMock()
        mock_session.get.return_value = mock_resp

        events = sensor.poll()
        assert len(events) == 1
        assert events[0].event_id == "e-list"

    def test_poll_sends_after_param(self, sensor, mock_session):
        sensor._last_event_id = "prev-cursor"
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"events": []}
        mock_resp.raise_for_status = MagicMock()
        mock_session.get.return_value = mock_resp

        sensor.poll()
        params = mock_session.get.call_args[1]["params"]
        assert params["after"] == "prev-cursor"

    def test_poll_multiple_updates_cursor_to_last(self, sensor, mock_session):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "events": [
                {"event_type": "x", "event_id": "e1", "data": {}},
                {"event_type": "x", "event_id": "e2", "data": {}},
            ]
        }
        mock_resp.raise_for_status = MagicMock()
        mock_session.get.return_value = mock_resp

        events = sensor.poll()
        assert len(events) == 2
        assert sensor.last_event_id == "e2"


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------

class TestDispatch:
    def test_dispatch_sync(self, sensor):
        results_capture = []

        @sensor.on_event
        def handler(event):
            results_capture.append(event.event_id)
            return "ok"

        event = ZeroDBEvent(event_type="x", event_id="d1")
        results = sensor._dispatch_event(event)
        assert results_capture == ["d1"]
        assert results == ["ok"]

    def test_dispatch_async(self, sensor):
        results_capture = []

        @sensor.on_event
        async def handler(event):
            results_capture.append(event.event_id)
            return "async-ok"

        event = ZeroDBEvent(event_type="x", event_id="d2")
        results = sensor._dispatch_event(event)
        assert results_capture == ["d2"]
        assert results == ["async-ok"]

    def test_dispatch_error_returns_none(self, sensor):
        @sensor.on_event
        def bad(event):
            raise RuntimeError("fail")

        event = ZeroDBEvent(event_type="x", event_id="d3")
        results = sensor._dispatch_event(event)
        assert results == [None]

    def test_dispatch_multiple_handlers(self, sensor):
        @sensor.on_event
        def h1(event):
            return 1

        @sensor.on_event
        def h2(event):
            return 2

        event = ZeroDBEvent(event_type="x", event_id="d4")
        results = sensor._dispatch_event(event)
        assert results == [1, 2]


# ---------------------------------------------------------------------------
# Webhook
# ---------------------------------------------------------------------------

class TestWebhook:
    def test_matching(self, sensor):
        @sensor.on_event
        def handler(event):
            return event.data

        payload = {
            "event_type": "zerodb.table.row_inserted",
            "event_id": "wh1",
            "data": {"row": 1},
        }
        results = sensor.process_webhook(payload)
        assert results == [{"row": 1}]

    def test_non_matching(self, sensor):
        @sensor.on_event
        def handler(event):
            return "nope"

        payload = {"event_type": "zerodb.other", "event_id": "wh2", "data": {}}
        results = sensor.process_webhook(payload)
        assert results == []


# ---------------------------------------------------------------------------
# Start / Stop
# ---------------------------------------------------------------------------

class TestStartStop:
    def test_start_sets_running(self, sensor):
        sensor.start()
        assert sensor.is_running is True
        sensor.stop()

    def test_stop_clears_running(self, sensor):
        sensor.start()
        sensor.stop()
        assert sensor.is_running is False

    def test_start_idempotent(self, sensor):
        sensor.start()
        sensor.start()
        assert sensor.is_running is True
        sensor.stop()

    def test_start_returns_self(self, sensor):
        assert sensor.start() is sensor
        sensor.stop()
