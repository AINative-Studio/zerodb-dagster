"""
Tests for zerodb-dagster ZeroDBIOManager.

Uses mocked HTTP responses -- no real API calls.
"""

import json
import os
from unittest.mock import MagicMock, patch

import pytest

os.environ.setdefault("ZERODB_API_KEY", "test-key-000")
os.environ.setdefault("ZERODB_PROJECT_ID", "test-project-000")

from zerodb_dagster.io_manager import ZeroDBIOManager


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_session():
    with patch("zerodb_dagster.io_manager.requests.Session") as MockSession:
        session = MagicMock()
        MockSession.return_value = session
        yield session


@pytest.fixture
def io_mgr(mock_session):
    return ZeroDBIOManager(api_key="test-key", project_id="test-project")


def _make_context(asset_key=None, name=None, step_key=None):
    """Create a mock Dagster context."""
    ctx = MagicMock()
    ctx.asset_key = asset_key
    ctx.name = name
    ctx.step_key = step_key
    return ctx


def _make_asset_key(path):
    """Create a mock Dagster AssetKey."""
    key = MagicMock()
    key.path = path
    return key


# ---------------------------------------------------------------------------
# Constructor
# ---------------------------------------------------------------------------

class TestConstructor:
    def test_default_table(self, io_mgr):
        assert io_mgr.table == "dagster_io"

    def test_custom_table(self, mock_session):
        io = ZeroDBIOManager(api_key="k", project_id="p", table="my_assets")
        assert io.table == "my_assets"

    def test_custom_base_url(self, mock_session):
        io = ZeroDBIOManager(api_key="k", project_id="p", base_url="https://custom.api")
        assert io._base_url == "https://custom.api"


# ---------------------------------------------------------------------------
# _get_asset_key
# ---------------------------------------------------------------------------

class TestGetAssetKey:
    def test_asset_key_with_path(self, io_mgr):
        ctx = _make_context(asset_key=_make_asset_key(["group", "my_asset"]))
        assert io_mgr._get_asset_key(ctx) == "group/my_asset"

    def test_asset_key_single(self, io_mgr):
        ctx = _make_context(asset_key=_make_asset_key(["single"]))
        assert io_mgr._get_asset_key(ctx) == "single"

    def test_asset_key_string(self, io_mgr):
        ctx = _make_context(asset_key="string_key")
        assert io_mgr._get_asset_key(ctx) == "string_key"

    def test_name_fallback(self, io_mgr):
        ctx = _make_context(asset_key=None, name="my_op")
        assert io_mgr._get_asset_key(ctx) == "my_op"

    def test_step_key_fallback(self, io_mgr):
        ctx = _make_context(asset_key=None, name=None, step_key="step_1")
        assert io_mgr._get_asset_key(ctx) == "step_1"

    def test_default_fallback(self, io_mgr):
        ctx = _make_context(asset_key=None, name=None, step_key=None)
        assert io_mgr._get_asset_key(ctx) == "default"


# ---------------------------------------------------------------------------
# handle_output
# ---------------------------------------------------------------------------

class TestHandleOutput:
    def test_handle_dict(self, io_mgr, mock_session):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"inserted": 1}
        mock_resp.raise_for_status = MagicMock()
        mock_session.post.return_value = mock_resp

        ctx = _make_context(asset_key=_make_asset_key(["my_asset"]))
        result = io_mgr.handle_output(ctx, {"status": "done", "count": 42})
        assert result == {"inserted": 1}

        body = mock_session.post.call_args[1]["json"]
        assert body["table"] == "dagster_io"
        assert body["rows"][0]["asset_key"] == "my_asset"
        assert json.loads(body["rows"][0]["content"]) == {"status": "done", "count": 42}
        assert body["rows"][0]["content_type"] == "dict"

    def test_handle_string(self, io_mgr, mock_session):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"inserted": 1}
        mock_resp.raise_for_status = MagicMock()
        mock_session.post.return_value = mock_resp

        ctx = _make_context(asset_key=_make_asset_key(["text_asset"]))
        io_mgr.handle_output(ctx, "hello world")
        body = mock_session.post.call_args[1]["json"]
        assert body["rows"][0]["content"] == "hello world"
        assert body["rows"][0]["content_type"] == "str"

    def test_handle_bytes(self, io_mgr, mock_session):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"inserted": 1}
        mock_resp.raise_for_status = MagicMock()
        mock_session.post.return_value = mock_resp

        ctx = _make_context(asset_key=_make_asset_key(["bin"]))
        io_mgr.handle_output(ctx, b"raw data")
        body = mock_session.post.call_args[1]["json"]
        assert body["rows"][0]["content"] == "raw data"
        assert body["rows"][0]["content_type"] == "bytes"

    def test_handle_list(self, io_mgr, mock_session):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"inserted": 1}
        mock_resp.raise_for_status = MagicMock()
        mock_session.post.return_value = mock_resp

        ctx = _make_context(asset_key=_make_asset_key(["list_asset"]))
        io_mgr.handle_output(ctx, [1, 2, 3])
        body = mock_session.post.call_args[1]["json"]
        assert json.loads(body["rows"][0]["content"]) == [1, 2, 3]
        assert body["rows"][0]["content_type"] == "list"

    def test_handle_int(self, io_mgr, mock_session):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"inserted": 1}
        mock_resp.raise_for_status = MagicMock()
        mock_session.post.return_value = mock_resp

        ctx = _make_context(asset_key=_make_asset_key(["num"]))
        io_mgr.handle_output(ctx, 42)
        body = mock_session.post.call_args[1]["json"]
        assert body["rows"][0]["content"] == "42"
        assert body["rows"][0]["content_type"] == "int"

    def test_handle_raises_on_http_error(self, io_mgr, mock_session):
        import requests
        mock_resp = MagicMock()
        mock_resp.raise_for_status.side_effect = requests.HTTPError("500")
        mock_session.post.return_value = mock_resp

        ctx = _make_context(asset_key=_make_asset_key(["fail"]))
        with pytest.raises(requests.HTTPError):
            io_mgr.handle_output(ctx, {"x": 1})


# ---------------------------------------------------------------------------
# load_input
# ---------------------------------------------------------------------------

class TestLoadInput:
    def test_load_dict(self, io_mgr, mock_session):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "rows": [{"asset_key": "a", "content": '{"status": "done"}', "content_type": "dict"}]
        }
        mock_resp.raise_for_status = MagicMock()
        mock_session.post.return_value = mock_resp

        ctx = _make_context(asset_key=_make_asset_key(["a"]))
        result = io_mgr.load_input(ctx)
        assert result == {"status": "done"}

    def test_load_list(self, io_mgr, mock_session):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "rows": [{"asset_key": "b", "content": "[1,2]", "content_type": "list"}]
        }
        mock_resp.raise_for_status = MagicMock()
        mock_session.post.return_value = mock_resp

        ctx = _make_context(asset_key=_make_asset_key(["b"]))
        assert io_mgr.load_input(ctx) == [1, 2]

    def test_load_string(self, io_mgr, mock_session):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "rows": [{"asset_key": "c", "content": "hello", "content_type": "str"}]
        }
        mock_resp.raise_for_status = MagicMock()
        mock_session.post.return_value = mock_resp

        ctx = _make_context(asset_key=_make_asset_key(["c"]))
        assert io_mgr.load_input(ctx) == "hello"

    def test_load_not_found(self, io_mgr, mock_session):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"rows": []}
        mock_resp.raise_for_status = MagicMock()
        mock_session.post.return_value = mock_resp

        ctx = _make_context(asset_key=_make_asset_key(["missing"]))
        assert io_mgr.load_input(ctx) is None

    def test_load_data_key_fallback(self, io_mgr, mock_session):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "data": [{"asset_key": "d", "content": "42", "content_type": "int"}]
        }
        mock_resp.raise_for_status = MagicMock()
        mock_session.post.return_value = mock_resp

        ctx = _make_context(asset_key=_make_asset_key(["d"]))
        assert io_mgr.load_input(ctx) == 42

    def test_load_invalid_json_returns_raw(self, io_mgr, mock_session):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "rows": [{"asset_key": "e", "content": "bad{json", "content_type": "dict"}]
        }
        mock_resp.raise_for_status = MagicMock()
        mock_session.post.return_value = mock_resp

        ctx = _make_context(asset_key=_make_asset_key(["e"]))
        assert io_mgr.load_input(ctx) == "bad{json"

    def test_load_bool(self, io_mgr, mock_session):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "rows": [{"asset_key": "f", "content": "true", "content_type": "bool"}]
        }
        mock_resp.raise_for_status = MagicMock()
        mock_session.post.return_value = mock_resp

        ctx = _make_context(asset_key=_make_asset_key(["f"]))
        assert io_mgr.load_input(ctx) is True

    def test_load_float(self, io_mgr, mock_session):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "rows": [{"asset_key": "g", "content": "3.14", "content_type": "float"}]
        }
        mock_resp.raise_for_status = MagicMock()
        mock_session.post.return_value = mock_resp

        ctx = _make_context(asset_key=_make_asset_key(["g"]))
        assert io_mgr.load_input(ctx) == 3.14


# ---------------------------------------------------------------------------
# delete_asset
# ---------------------------------------------------------------------------

class TestDeleteAsset:
    def test_delete(self, io_mgr, mock_session):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"deleted": 1}
        mock_resp.raise_for_status = MagicMock()
        mock_session.post.return_value = mock_resp

        result = io_mgr.delete_asset("my_asset")
        assert result == {"deleted": 1}
        body = mock_session.post.call_args[1]["json"]
        assert body["filters"][0]["value"] == "my_asset"


# ---------------------------------------------------------------------------
# list_assets
# ---------------------------------------------------------------------------

class TestListAssets:
    def test_list(self, io_mgr, mock_session):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "rows": [{"asset_key": "a"}, {"asset_key": "b"}]
        }
        mock_resp.raise_for_status = MagicMock()
        mock_session.post.return_value = mock_resp

        assets = io_mgr.list_assets()
        assert assets == ["a", "b"]

    def test_list_empty(self, io_mgr, mock_session):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"rows": []}
        mock_resp.raise_for_status = MagicMock()
        mock_session.post.return_value = mock_resp

        assert io_mgr.list_assets() == []
