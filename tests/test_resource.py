"""
Tests for zerodb-dagster ZeroDBResource.
"""

import os
from unittest.mock import MagicMock, patch

import pytest

os.environ.setdefault("ZERODB_API_KEY", "test-key-000")
os.environ.setdefault("ZERODB_PROJECT_ID", "test-project-000")

from zerodb_dagster.resource import ZeroDBResource


@pytest.fixture
def resource():
    return ZeroDBResource(api_key="test-key", project_id="test-project")


class TestResource:
    def test_api_key(self, resource):
        assert resource.api_key == "test-key"

    def test_project_id(self, resource):
        assert resource.project_id == "test-project"

    def test_base_url_default(self, resource):
        assert "ainative.studio" in resource.base_url

    def test_custom_base_url(self):
        r = ZeroDBResource(api_key="k", project_id="p", base_url="https://custom.api")
        assert r.base_url == "https://custom.api"

    def test_get_client_returns_session(self, resource):
        client = resource.get_client()
        assert hasattr(client, "get")
        assert hasattr(client, "post")
        assert "Bearer test-key" in client.headers.get("Authorization", "")

    def test_get_client_has_project_header(self, resource):
        client = resource.get_client()
        assert client.headers.get("X-Project-ID") == "test-project"

    def test_get_headers(self, resource):
        headers = resource.get_headers()
        assert headers["Authorization"] == "Bearer test-key"
        assert headers["X-Project-ID"] == "test-project"

    def test_get_client_new_session_each_call(self, resource):
        c1 = resource.get_client()
        c2 = resource.get_client()
        assert c1 is not c2


class TestResourceStoreResult:
    @patch("zerodb_dagster.resource.requests.Session")
    def test_store_result(self, MockSession):
        session = MagicMock()
        MockSession.return_value = session
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"inserted": 1}
        mock_resp.raise_for_status = MagicMock()
        session.post.return_value = mock_resp

        r = ZeroDBResource(api_key="k", project_id="p")
        result = r.store_result({"status": "done"}, table="custom_table")
        assert result == {"inserted": 1}

    @patch("zerodb_dagster.resource.requests.Session")
    def test_store_result_default_table(self, MockSession):
        session = MagicMock()
        MockSession.return_value = session
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"inserted": 1}
        mock_resp.raise_for_status = MagicMock()
        session.post.return_value = mock_resp

        r = ZeroDBResource(api_key="k", project_id="p")
        r.store_result({"x": 1})
        body = session.post.call_args[1]["json"]
        assert body["table"] == "dagster_results"


class TestResourceQueryTable:
    @patch("zerodb_dagster.resource.requests.Session")
    def test_query_table(self, MockSession):
        session = MagicMock()
        MockSession.return_value = session
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"rows": [{"id": 1}, {"id": 2}]}
        mock_resp.raise_for_status = MagicMock()
        session.post.return_value = mock_resp

        r = ZeroDBResource(api_key="k", project_id="p")
        rows = r.query_table("my_table", filters=[{"column": "id", "op": "eq", "value": 1}])
        assert len(rows) == 2

    @patch("zerodb_dagster.resource.requests.Session")
    def test_query_table_data_fallback(self, MockSession):
        session = MagicMock()
        MockSession.return_value = session
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"data": [{"id": 3}]}
        mock_resp.raise_for_status = MagicMock()
        session.post.return_value = mock_resp

        r = ZeroDBResource(api_key="k", project_id="p")
        rows = r.query_table("t")
        assert rows == [{"id": 3}]

    @patch("zerodb_dagster.resource.requests.Session")
    def test_query_table_empty(self, MockSession):
        session = MagicMock()
        MockSession.return_value = session
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"rows": []}
        mock_resp.raise_for_status = MagicMock()
        session.post.return_value = mock_resp

        r = ZeroDBResource(api_key="k", project_id="p")
        rows = r.query_table("t")
        assert rows == []
