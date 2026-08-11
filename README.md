# VOLTTRON Forward Historian

Forward Historian republishes selected messages from one VOLTTRON platform on a
second platform. If the destination is unreachable, messages remain in the
source platform's local historian cache until it reconnects.

```text
source platform                                      destination platform

driver or agent -> PubSub -> Forward Historian  --->  VIP/TCP -> PubSub
                         cached locally if offline
```

Forwarded messages retain their original topic and add these headers:

```text
X-Forwarded: True
X-Forwarded-From: <source instance name>
```

## Before You Start

You need two working VOLTTRON platforms. The source platform runs the Forward
Historian. The destination platform receives forwarded messages.

This example uses the fake driver to forward simulated device data. Set up the
fake driver by following its [installation instructions](https://github.com/eclipse-volttron/volttron-lib-fake-driver).

Once the fake driver is publishing, its device messages are normally below the
`devices/` topic tree. This guide forwards that entire tree.

## 1. Choose the Destination Address

Use TCP when the source and destination platforms run on different hosts. Use
the destination platform's IPC socket when both platforms run on the same host.

### Different Hosts: TCP VIP

For a destination on another host, the destination must listen on a TCP VIP
address. A platform using only its default IPC address cannot receive messages
from another host.

On the destination host, edit the platform config using `nano` or your editor:

```bash
nano YOUR_VOLTTRON_HOME/config
```

Under `[volttron]`, set an externally reachable address. For example:

```ini
[volttron]
address = tcp://0.0.0.0:22916
instance-name = YOUR_DESTINATION_INSTANCE_NAME
messagebus = zmq
auth-enabled = True
```

Verify that it is listening:

```bash
ss -lntp | grep 22916
```

From the source host, verify network access:

```bash
nc -vz YOUR_REMOTE_HOST_IP 22916
```

Do not continue until this succeeds. If it fails, correct the destination
address or firewall first.

<details>
<summary>Same Host: Use the destination IPC socket</summary>

When both platforms run on the same machine, no TCP listener or firewall rule
is needed. Use the destination platform's local VIP socket directly:

```text
ipc:///YOUR_DESTINATION_VOLTTRON_HOME/run/vip.socket
```

Confirm that the destination socket exists:

```bash
ls -l YOUR_DESTINATION_VOLTTRON_HOME/run/vip.socket
```

Use this exact value as `destination-address` in `forwarder.config`:

```json
{
  "destination-address": "ipc:///YOUR_DESTINATION_VOLTTRON_HOME/run/vip.socket",
  "required_target_agents": [],
  "capture_device_data": true,
  "capture_analysis_data": false,
  "capture_log_data": false,
  "capture_record_data": false,
  "custom_topic_list": [],
  "topic_replace_list": [],
  "cache_only": false
}
```

Do not include `destination-serverkey` for an IPC destination. The key is only
needed for a remote TCP connection.

</details>

## 2. Get the Destination Server Key

This step applies only to a TCP destination on another host. Skip it when using
the same-host IPC setup above.

On the destination host, open its platform credential file:

```bash
nano YOUR_VOLTTRON_HOME/credentials_store/platform.json
```

Copy the `publickey` value. The Forward Historian uses this as its
`destination-serverkey`.

## 3. Configure the Forwarder

Create a file named `forwarder.config` anywhere convenient on the source host.
It does not need to live in the Forward Historian source checkout:

```bash
nano forwarder.config
```

For a TCP destination, paste the following, then replace the destination IP
address and the `publickey` copied in Step 2:

```json
{
  "destination-address": "tcp://YOUR_REMOTE_HOST_IP:22916",
  "destination-serverkey": "paste-the-destination-platform-publickey-here",
  "required_target_agents": [],
  "capture_device_data": true,
  "capture_analysis_data": false,
  "capture_log_data": false,
  "capture_record_data": false,
  "custom_topic_list": [],
  "topic_replace_list": [],
  "cache_only": false
}
```

`capture_device_data: true` forwards all messages under `devices/`, including
the messages published by a correctly configured fake driver.

For a same-host IPC destination, use the configuration in the collapsible
section above instead.

Install the Forward Historian on the source platform:

```bash
vctl install volttron-forward-historian --vip-identity platform.forwarder
```

Add `forwarder.config` to the Forward Historian configuration store:

```bash
vctl config store platform.forwarder config forwarder.config
```

The install creates this credential on the source:

```text
YOUR_SOURCE_VOLTTRON_HOME/credentials_store/platform.forwarder.json
```

Open it and copy its `publickey` value:

```bash
nano YOUR_SOURCE_VOLTTRON_HOME/credentials_store/platform.forwarder.json
```

## 4. Authorize the Forwarder on the Destination

On the destination host, create a credential file for the source forwarder:

```bash
nano YOUR_DESTINATION_VOLTTRON_HOME/credentials_store/platform.forwarder.json
```

Paste the following and replace the public key with the source forwarder's
public key copied in Step 3:

```json
{"identity":"platform.forwarder","publickey":"paste-the-source-forwarder-publickey-here","secretkey":"",
"domain":"",
"address":""}
```
