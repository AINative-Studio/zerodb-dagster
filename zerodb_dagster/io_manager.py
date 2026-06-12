"""
ZeroDBIOManager -- Dagster I/O manager that stores assets in ZeroDB tables.

    from zerodb_dagster import ZeroDBIOManager

    io = ZeroDBIOManager()  # auto-provisions

    # In a Dagster job:
    # io.handle_output(context, obj)  -> stores to ZeroDB
    # io.load_input(context)          -> reads from ZeroDB
"""

import json
import logging
from typing import Any, Dict, Optional

import requests

from zerodb_dagster.provision import resolve_credentials

logger = logging.getLogger(__name__)


class ZeroDBIOManager:
    """Dagster I/O manager that stores and retrieves assets in ZeroDB tables.

    Stores outputs as JSON-serialized rows in a ZeroDB NoSQL table,
    keyed by asset_key derived from the Dagster context.

    Args:
        api_key: ZeroDB API key (auto-resolved if not provided).
        project_id: ZeroDB project ID.
        base_url: ZeroDB API base URL.
        table: Table name for asset storage (default 'dagster_io').
    """

    def __init__(self, api_key=None, project_id=None, base_url=None,
                 table="dagster_io"):
        self._api_key, self._project_id, self._base_url = resolve_credentials(
            api_key=api_key,
            project_id=project_id,
        )
        if base_url:
            self._base_url = base_url

        self._table = table

        self._session = requests.Session()
        self._session.headers.update({
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self._api_key}",
            "X-Project-ID": self._project_id,
        })

    @property
    def table(self):
        """The ZeroDB table used for I/O storage."""
        return self._table

    @staticmethod
    def _get_asset_key(context):
        """Extract a unique asset key from a Dagster context object.

        Args:
            context: Dagster OutputContext or InputContext (or mock).

        Returns:
            str key like 'my_asset' or 'group/my_asset'.
        """
        if hasattr(context, "asset_key") and context.asset_key is not None:
            key = context.asset_key
            if hasattr(key, "path"):
                return "/".join(key.path)
            return str(key)

        if hasattr(context, "name") and context.name:
            return context.name

        if hasattr(context, "step_key") and context.step_key:
            return context.step_key

        return "default"

    def handle_output(self, context, obj):
        """Store an output value in ZeroDB.

        Args:
            context: Dagster OutputContext (or any object with asset_key/name).
            obj: The output value to store. Must be JSON-serializable.

        Returns:
            dict with the stored row data from ZeroDB.

        Raises:
            requests.HTTPError: If the API call fails.
        """
        asset_key = self._get_asset_key(context)

        if isinstance(obj, (bytes, bytearray)):
            serialized = obj.decode("utf-8", errors="replace")
            content_type = "bytes"
        elif isinstance(obj, str):
            serialized = obj
            content_type = "str"
        else:
            serialized = json.dumps(obj)
            content_type = type(obj).__name__

        row = {
            "asset_key": asset_key,
            "content": serialized,
            "content_type": content_type,
        }

        resp = self._session.post(
            f"{self._base_url}/api/v1/public/tables/insert",
            json={"table": self._table, "rows": [row]},
        )
        resp.raise_for_status()
        logger.info("Stored output for asset '%s' in table '%s'", asset_key, self._table)
        return resp.json()

    def load_input(self, context):
        """Load an input value from ZeroDB.

        Args:
            context: Dagster InputContext (or any object with asset_key/name).

        Returns:
            The deserialized value, or None if not found.

        Raises:
            requests.HTTPError: If the API call fails (non-404).
        """
        asset_key = self._get_asset_key(context)

        body = {
            "table": self._table,
            "filters": [{"column": "asset_key", "op": "eq", "value": asset_key}],
            "limit": 1,
        }

        resp = self._session.post(
            f"{self._base_url}/api/v1/public/tables/query",
            json=body,
        )
        resp.raise_for_status()
        result = resp.json()

        rows = result.get("rows", result.get("data", []))
        if not rows:
            logger.warning("No data found for asset '%s'", asset_key)
            return None

        row = rows[0]
        raw = row.get("content", "")
        content_type = row.get("content_type", "str")

        if content_type in ("dict", "list", "int", "float", "bool"):
            try:
                return json.loads(raw)
            except (json.JSONDecodeError, TypeError):
                return raw

        return raw

    def delete_asset(self, asset_key):
        """Delete a stored asset by key.

        Args:
            asset_key: Asset key string.

        Returns:
            dict with deletion result.
        """
        resp = self._session.post(
            f"{self._base_url}/api/v1/public/tables/delete",
            json={
                "table": self._table,
                "filters": [{"column": "asset_key", "op": "eq", "value": asset_key}],
            },
        )
        resp.raise_for_status()
        return resp.json()

    def list_assets(self, limit=100):
        """List all stored asset keys.

        Args:
            limit: Max results.

        Returns:
            list of asset key strings.
        """
        body = {"table": self._table, "limit": limit}

        resp = self._session.post(
            f"{self._base_url}/api/v1/public/tables/query",
            json=body,
        )
        resp.raise_for_status()
        result = resp.json()
        rows = result.get("rows", result.get("data", []))
        return [row.get("asset_key", "") for row in rows]
