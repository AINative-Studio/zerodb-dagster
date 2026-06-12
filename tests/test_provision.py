"""
Tests for zerodb-dagster auto-provisioning.
"""

import json
import os
from unittest.mock import MagicMock, patch

import pytest

os.environ.setdefault("ZERODB_API_KEY", "test-key-000")
os.environ.setdefault("ZERODB_PROJECT_ID", "test-project-000")

from zerodb_dagster.provision import (
    resolve_credentials,
    _load_config_file,
    _save_config_file,
    _auto_provision,
    ZERODB_API_BASE,
)


class TestResolveCredentials:
    def test_explicit_args(self):
        key, proj, url = resolve_credentials(api_key="my-key", project_id="my-proj")
        assert key == "my-key"
        assert proj == "my-proj"

    def test_env_vars(self):
        with patch.dict(os.environ, {"ZERODB_API_KEY": "env-key", "ZERODB_PROJECT_ID": "env-proj"}):
            key, proj, url = resolve_credentials()
            assert key == "env-key"
            assert proj == "env-proj"

    def test_custom_base_url_env(self):
        with patch.dict(os.environ, {
            "ZERODB_API_KEY": "k", "ZERODB_PROJECT_ID": "p",
            "ZERODB_BASE_URL": "https://custom.url",
        }):
            _, _, url = resolve_credentials()
            assert url == "https://custom.url"

    @patch("zerodb_dagster.provision._load_config_file")
    def test_config_file_fallback(self, mock_load):
        mock_load.return_value = ("file-key", "file-proj")
        env_k = os.environ.pop("ZERODB_API_KEY", None)
        env_p = os.environ.pop("ZERODB_PROJECT_ID", None)
        try:
            key, proj, _ = resolve_credentials()
            assert key == "file-key"
        finally:
            if env_k:
                os.environ["ZERODB_API_KEY"] = env_k
            if env_p:
                os.environ["ZERODB_PROJECT_ID"] = env_p

    @patch("zerodb_dagster.provision._auto_provision")
    @patch("zerodb_dagster.provision._load_config_file")
    def test_auto_provision_fallback(self, mock_load, mock_prov):
        mock_load.return_value = (None, None)
        mock_prov.return_value = ("auto-key", "auto-proj")
        env_k = os.environ.pop("ZERODB_API_KEY", None)
        env_p = os.environ.pop("ZERODB_PROJECT_ID", None)
        try:
            key, proj, _ = resolve_credentials()
            assert key == "auto-key"
            mock_prov.assert_called_once()
        finally:
            if env_k:
                os.environ["ZERODB_API_KEY"] = env_k
            if env_p:
                os.environ["ZERODB_PROJECT_ID"] = env_p


class TestAutoProvision:
    @patch("zerodb_dagster.provision._save_config_file")
    @patch("zerodb_dagster.provision.requests.post")
    def test_success(self, mock_post, mock_save):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "api_key": "new-k", "project_id": "new-p",
            "claim_url": "https://ainative.studio/claim/x",
        }
        mock_resp.raise_for_status = MagicMock()
        mock_post.return_value = mock_resp

        key, proj = _auto_provision()
        assert key == "new-k"
        assert proj == "new-p"
        mock_save.assert_called_once()

    @patch("zerodb_dagster.provision.requests.post")
    def test_http_error(self, mock_post):
        import requests
        mock_resp = MagicMock()
        mock_resp.raise_for_status.side_effect = requests.HTTPError("503")
        mock_post.return_value = mock_resp

        with pytest.raises(requests.HTTPError):
            _auto_provision()


class TestConfigFile:
    @patch("zerodb_dagster.provision.CONFIG_FILE")
    def test_load_success(self, mock_path):
        mock_path.exists.return_value = True
        mock_path.read_text.return_value = json.dumps({"api_key": "fk", "project_id": "fp"})
        key, proj = _load_config_file()
        assert key == "fk"

    @patch("zerodb_dagster.provision.CONFIG_FILE")
    def test_load_not_found(self, mock_path):
        mock_path.exists.return_value = False
        assert _load_config_file() == (None, None)

    @patch("zerodb_dagster.provision.CONFIG_FILE")
    def test_load_invalid_json(self, mock_path):
        mock_path.exists.return_value = True
        mock_path.read_text.return_value = "bad"
        assert _load_config_file() == (None, None)

    @patch("zerodb_dagster.provision.CONFIG_FILE")
    def test_load_missing_key(self, mock_path):
        mock_path.exists.return_value = True
        mock_path.read_text.return_value = json.dumps({"api_key": "k"})
        assert _load_config_file() == (None, None)
