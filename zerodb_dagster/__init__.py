"""
zerodb-dagster -- Dagster I/O manager, sensor, and resource for ZeroDB.

    from zerodb_dagster import ZeroDBIOManager, ZeroDBSensor, ZeroDBResource

    io = ZeroDBIOManager()  # auto-provisions
    sensor = ZeroDBSensor(event_type='zerodb.table.row_inserted')
"""

from zerodb_dagster.io_manager import ZeroDBIOManager  # noqa: F401
from zerodb_dagster.sensor import ZeroDBSensor, ZeroDBEvent  # noqa: F401
from zerodb_dagster.resource import ZeroDBResource  # noqa: F401

__version__ = "0.1.0"
__all__ = [
    "ZeroDBIOManager",
    "ZeroDBSensor",
    "ZeroDBResource",
    "ZeroDBEvent",
]
