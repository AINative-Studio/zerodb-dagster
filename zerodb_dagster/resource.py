"""
ZeroDBResource -- Configurable Dagster resource for ZeroDB.

    from zerodb_dagster import ZeroDBResource

    resource = ZeroDBResource(api_key='zdb_...', project_id='proj_...')
    client = resource.get_client()
"""

import requests

from zerodb_dagster.provision import resolve_credentials


class ZeroDBResource:
    """Dagster resource providing an authenticated ZeroDB client.

    Args:
        api_key: ZeroDB API key (auto-resolved if not provided).
        project_id: ZeroDB project ID.
        base_url: ZeroDB API base URL.
    """

    def __init__(self, api_key=None, project_id=None, base_url=None):
        self._api_key, self._project_id, self._base_url = resolve_credentials(
            api_key=api_key,
            project_id=project_id,
        )
        if base_url:
            self._base_url = base_url

    @property
    def api_key(self):
        """The resolved API key."""
        return self._api_key

    @property
    def project_id(self):
        """The resolved project ID."""
        return self._project_id

    @property
    def base_url(self):
        """The ZeroDB API base URL."""
        return self._base_url

    def get_client(self):
        """Return an authenticated requests.Session for ZeroDB API calls.

        Returns:
            requests.Session with auth headers set.
        """
        session = requests.Session()
        session.headers.update({
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self._api_key}",
            "X-Project-ID": self._project_id,
        })
        return session

    def get_headers(self):
        """Return auth headers as a dict.

        Returns:
            dict of HTTP headers.
        """
        return {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self._api_key}",
            "X-Project-ID": self._project_id,
        }

    def store_result(self, data, table="dagster_results", **kwargs):
        """Store data in a ZeroDB table.

        Args:
            data: Dict of data to store.
            table: Table name (default 'dagster_results').
            **kwargs: Extra fields to merge.

        Returns:
            dict with the stored row.
        """
        row = dict(data)
        row.update(kwargs)

        session = self.get_client()
        resp = session.post(
            f"{self._base_url}/api/v1/public/tables/insert",
            json={"table": table, "rows": [row]},
        )
        resp.raise_for_status()
        return resp.json()

    def query_table(self, table, filters=None, limit=100):
        """Query rows from a ZeroDB table.

        Args:
            table: Table name.
            filters: List of filter dicts.
            limit: Max rows.

        Returns:
            list of row dicts.
        """
        body = {"table": table, "limit": limit}
        if filters:
            body["filters"] = filters

        session = self.get_client()
        resp = session.post(
            f"{self._base_url}/api/v1/public/tables/query",
            json=body,
        )
        resp.raise_for_status()
        result = resp.json()
        return result.get("rows", result.get("data", []))
