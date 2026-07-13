# VOLTTRON Forward Historian Agent

The **Forward Historian** is a specialized agent designed to send/forward data from one VOLTTRON instance to another. 

Its primary purpose is to allow a remote target instance's pubsub bus to receive and simulate data as if it were coming from a real local device. If the target instance or any "required target agents" become unavailable, the Forward Historian will queue incoming messages in its local backup cache (up to its maximum configured capacity) and publish them in bulk once the destination comes back online.

This modular version of the agent has been updated for compatibility with modern **modular VOLTTRON (VOLTTRON 11.x and volttron-core 2.x)**.

---

## Installation

To set up the Forward Historian in your virtual environment:

1. **Activate your Python environment:**
   ```bash
   source <path-to-your-venv>/bin/activate
   ```

2. **Install the Forward Historian package in editable developer mode:**
   ```bash
   pip install -e <path-to-volttron-forward-historian-directory>
   ```

3. **Install the required SQLite Historian dependency (for local caching):**
   ```bash
   pip install -e <path-to-volttron-sqlite-historian-directory>
   ```

---

## Configuration Options

The Forward Historian uses the VOLTTRON Configuration Store. It can be dynamically configured. Below is an example configuration file:

```json
{
    // Address of the target remote platform (REQUIRED)
    "destination-vip": "tcp://127.0.0.1:22916",

    // Allow checking on the remote instance to verify peer identities are connected before forwarding
    "required_target_agents": ["platform.historian"],

    // Subscription toggles
    "capture_device_data": true,
    "capture_analysis_data": true,
    "capture_log_data": true,
    "capture_record_data": true,

    // Custom topics to subscribe to locally and forward to the destination instance
    "custom_topic_list": ["actuator", "alert"],

    // Puts the forwarder in cache-only mode (doesn't attempt to publish immediately)
    "cache_only": false,

    // Replace matching parts of topics before forwarding
    "topic_replace_list": [
        {"from": "PNNL/BUILDING_1", "to": "PNNL/BUILDING1_ANON"}
    ],

    // Print progress to log after a certain number of successful bulk publishes
    "message_publish_count": 10000
}
```

---

## Setup & Running

Once installed, configure and start the agent on your VOLTTRON platform:

1. **Create your configuration file** (e.g., `config.json`).
2. **Install and start the agent on your platform:**
   ```bash
   volttron -vv &
   vctl install volttron-forward-historian --vip-identity platform.forwarder --config config.json --start
   ```

> **IMPORTANT — Configuration Store Name**
> The Forward Historian (like all base historians) only reads the configuration
> entry stored under the exact name **`config`**. If your configuration ends up
> stored under a different name (for example `config.json`), the agent will fall
> back to empty defaults and log `No destination address/vip configured`.
>
> Verify and, if necessary, correct the stored config name:
> ```bash
> # List the stored configuration names for the agent
> vctl config list <agent-vip-identity-or-name>
>
> # If it shows "config.json" instead of "config", store it under "config":
> vctl config store <agent-vip-identity-or-name> config config.json --json
>
> # (optional) remove the incorrectly named entry
> vctl config delete <agent-vip-identity-or-name> config.json
> ```
> After correcting the config store, restart the agent.

### Configuration Keys

Both dash- and underscore-separated key names are accepted
(e.g. `destination-vip` or `destination_vip`).

> **Note on `required_target_agents`**
> If you list an agent here (e.g. `platform.historian`), the forwarder will not
> publish until an agent with that identity is running on the destination
> platform. For initial connectivity testing, set this to `[]` and run a simple
> listener on the destination to confirm data is flowing.

---

## Running Tests

To run the integration and unit tests for this agent, make sure `volttron-testing` is installed:

```bash
pytest tests/
```
