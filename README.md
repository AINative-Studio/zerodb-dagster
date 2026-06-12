# zerodb-dagster

**Dagster I/O manager, sensor, and resource for [ZeroDB](https://zerodb.dev).**

[![PyPI](https://img.shields.io/pypi/v/zerodb-dagster)](https://pypi.org/project/zerodb-dagster/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Why use this?

| Feature | Description |
|---------|-------------|
| **I/O Manager** | Store and load Dagster assets directly in ZeroDB tables |
| **Event sensor** | ZeroDB events trigger Dagster runs via RunRequest |
| **Resource** | Configurable Dagster resource with authenticated client |
| **Auto-provision** | No signup needed -- ZeroDB project created on first use |
| **Webhook + polling** | Two modes: poll event stream or receive webhooks |

## Installation

```bash
pip install zerodb-dagster
```

## Quick Start

### I/O Manager

```python
from zerodb_dagster import ZeroDBIOManager

io = ZeroDBIOManager()  # auto-provisions

# Store an asset output
io.handle_output(context, {'accuracy': 0.94, 'model': 'v2'})

# Load it back
data = io.load_input(context)
print(data)  # {'accuracy': 0.94, 'model': 'v2'}
```

### Event Sensor

```python
from zerodb_dagster import ZeroDBSensor

sensor = ZeroDBSensor(event_type='zerodb.table.row_inserted')

@sensor.on_event
def handle_insert(event):
    return {'run_key': event.event_id, 'data': event.data}

sensor.start()
```

### Resource

```python
from zerodb_dagster import ZeroDBResource

resource = ZeroDBResource(api_key='zdb_...', project_id='proj_...')
client = resource.get_client()  # Authenticated requests.Session
```

## API Reference

### `ZeroDBIOManager(**kwargs)`

Dagster I/O manager that stores assets in ZeroDB tables.

| Param | Default | Description |
|-------|---------|-------------|
| `api_key` | auto | ZeroDB API key |
| `project_id` | auto | ZeroDB project ID |
| `table` | `dagster_io` | Table name for storage |

**Methods:**

| Method | Description |
|--------|-------------|
| `handle_output(context, obj)` | Store output in ZeroDB |
| `load_input(context)` | Load input from ZeroDB |
| `delete_asset(asset_key)` | Delete stored asset |
| `list_assets(limit=100)` | List stored asset keys |

### `ZeroDBSensor(event_type, **kwargs)`

Poll ZeroDB events and trigger Dagster runs.

| Param | Default | Description |
|-------|---------|-------------|
| `event_type` | required | Event type to listen for |
| `api_key` | auto | ZeroDB API key |
| `poll_interval` | 5 | Seconds between polls |
| `batch_size` | 100 | Max events per poll |

**Methods:**

| Method | Description |
|--------|-------------|
| `@sensor.on_event` | Decorator to register handler |
| `sensor.poll()` | Manually poll for events |
| `sensor.start()` | Start background polling |
| `sensor.stop()` | Stop polling |
| `sensor.process_webhook(payload)` | Process webhook payload |

### `ZeroDBResource(**kwargs)`

Configurable Dagster resource for ZeroDB.

| Method | Description |
|--------|-------------|
| `get_client()` | Authenticated requests.Session |
| `get_headers()` | Auth headers as dict |
| `store_result(data, table)` | Store data in table |
| `query_table(table, filters)` | Query table rows |

## Configuration

### Environment Variables

```bash
export ZERODB_API_KEY="your-api-key"
export ZERODB_PROJECT_ID="your-project-id"
```

### Auto-Provisioning

If no credentials are found, `zerodb-dagster` automatically creates a free ZeroDB project. Credentials are saved to `~/.zerodb/config.json`.

## Use Cases

### ML Pipeline

```python
from zerodb_dagster import ZeroDBIOManager

io = ZeroDBIOManager(table='ml_assets')

# After training
io.handle_output(train_context, {
    'model_path': '/models/v2.pkl',
    'accuracy': 0.94,
})

# In evaluation
metrics = io.load_input(train_context)
```

### Event-Driven Data Pipeline

```python
from zerodb_dagster import ZeroDBSensor, ZeroDBIOManager

sensor = ZeroDBSensor(event_type='zerodb.file.uploaded')
io = ZeroDBIOManager()

@sensor.on_event
def process_upload(event):
    io.handle_output(event, {
        'source': event.data.get('path'),
        'status': 'queued',
    })
    return event.to_run_request()

sensor.start()
```

---

**Powered by [ZeroDB](https://zerodb.dev) + [AINative](https://ainative.studio)**

Free database for AI agents. Auto-provisions in 200ms. No signup required.

[Get started](https://ainative.studio) | [Documentation](https://docs.ainative.studio) | [ZeroDB](https://zerodb.dev) | [GitHub](https://github.com/AINative-Studio/zerodb-dagster)

## License

MIT
