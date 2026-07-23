# VOLTTRON Forward Historian

The Forward Historian forwards selected VOLTTRON message-bus topics from one
platform to another. When the destination is unavailable, it retains messages
in the local historian cache and forwards them when the connection recovers.

## Installation

Install the package with Poetry or pip:

```bash
poetry install
```

```bash
pip install volttron-forward-historian
```

Install and start the agent on a running VOLTTRON platform:

```bash
vctl install volttron-forward-historian --vip-identity platform.forwarder --config config.json --start
```

## Configuration

Store the agent configuration under the name `config`:

```bash
vctl config store <agent-identity> config config.json
```

The Forward Historian accepts both dash-separated and underscore-separated
destination keys. Use valid JSON without comments:

```json
{
  "destination-vip": "tcp://127.0.0.1:22916",
  "destination-serverkey": "<destination-server-key>",
  "required_target_agents": ["platform.historian"],
  "capture_device_data": true,
  "capture_analysis_data": true,
  "capture_log_data": true,
  "capture_record_data": true,
  "custom_topic_list": ["actuator", "alert"],
  "cache_only": false,
  "topic_replace_list": [
    {"from": "PNNL/BUILDING_1", "to": "PNNL/BUILDING1_ANON"}
  ],
  "message_publish_count": 10000
}
```

`destination-vip` is required for ZeroMQ destinations. Set
`required_target_agents` to an empty list when no destination agent
availability check is required.

Set `cache_only` to `true` to collect messages locally without attempting to
forward them.

## Tests

Run the test suite with:

```bash
poetry run pytest
```
