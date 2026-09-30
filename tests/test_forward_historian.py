"""Focused forwarding tests for the modular Forward Historian.

These tests deliberately exercise the historian's real capture and publish
methods.  They use a small VIP-shaped destination double rather than the old
``PlatformWrapper`` fixture: that fixture currently manufactures UUID strings
as Curve keys and cannot start a modular TCP platform in this checkout.
"""

from types import SimpleNamespace

import gevent
import pytest

from historian.forwarder.forwarder import ForwardHistorian
from volttron.client.messaging import headers as headers_mod


class _CompletedCall:
    def get(self, timeout=None):
        return True


class _DestinationPubSub:
    def __init__(self):
        self.calls = []

    def publish(self, *, peer, topic, headers, message):
        self.calls.append(
            {"peer": peer, "topic": topic, "headers": headers, "message": message}
        )
        return _CompletedCall()


class _Destination:
    def __init__(self):
        self.vip = SimpleNamespace(pubsub=_DestinationPubSub())


def _forwarder(**overrides):
    """Build only the runtime state used by the forwarding methods.

    Calling ``__init__`` would start a real Agent and its config subsystem.  It
    is not needed here: the methods below are the production forwarding path,
    and this fixture supplies their documented runtime collaborators.
    """

    forwarder = ForwardHistorian.__new__(ForwardHistorian)
    forwarder._event_queue = gevent.queue.Queue()
    forwarder._topic_replace_map = {}
    forwarder.topic_replace_list = []
    forwarder.gather_timing_data = False
    forwarder.core = SimpleNamespace(agent_uuid=None, identity="forwarder")
    forwarder.instance_name = "source-platform"
    forwarder._last_timeout = 0
    forwarder._num_failures = 0
    forwarder.cache_only = False
    forwarder._target_platform = _Destination()
    forwarder.required_target_agents = []
    forwarder.destination_vip = "tcp://127.0.0.1:22916"
    forwarder.destination_serverkey = "destination-server-key"
    forwarder.destination_address = None
    forwarder.vip = SimpleNamespace(
        health=SimpleNamespace(
            set_status=lambda *args, **kwargs: None,
            send_alert=lambda *args, **kwargs: None,
            get_status_json=lambda: "{}",
        )
    )
    forwarder._device_data_filter = {}
    forwarder.report_handled = lambda records: setattr(
        forwarder, "handled", list(records) if isinstance(records, list) else [records]
    )
    forwarder.historian_teardown = lambda: setattr(forwarder, "_target_platform", None)
    forwarder._remote_connection_alive = lambda: True
    forwarder.historian_setup = lambda: None
    for name, value in overrides.items():
        setattr(forwarder, name, value)
    return forwarder


def _publish_one(forwarder, topic, message, headers=None):
    headers = dict(headers or {})
    forwarder.capture_data("peer", "sender", "pubsub", topic, headers, message)
    record = forwarder._event_queue.get_nowait()
    forwarder.publish_to_historian(
        [{"_id": 1, "topic": record["topic"], "value": record["readings"][0][1]}]
    )
    return forwarder._target_platform.vip.pubsub.calls[-1]


@pytest.mark.forwarder
def test_device_topic_is_forwarded_with_forwarding_headers():
    forwarder = _forwarder()
    original_headers = {
        headers_mod.DATE: "2026-09-23T12:00:00.000000Z",
        "Origin": "source-platform",
        "Destination": "old-destination",
    }
    message = [{"Temperature": 72.5}, {"Temperature": {"units": "F"}}]

    # Use the production device capture path, not a hand-built event.
    forwarder._capture_device_data(
        "peer",
        "driver",
        "pubsub",
        "devices/campus/building/unit/all",
        original_headers,
        message,
    )
    record = forwarder._event_queue.get_nowait()
    forwarder.publish_to_historian(
        [{"_id": 1, "topic": record["topic"], "value": record["readings"][0][1]}]
    )

    call = forwarder._target_platform.vip.pubsub.calls[0]
    assert call["topic"] == "devices/campus/building/unit/all"
    assert call["message"] == message
    assert call["headers"]["X-Forwarded"] is True
    assert call["headers"]["X-Forwarded-From"] == "source-platform"
    assert "Origin" not in call["headers"]
    assert "Destination" not in call["headers"]


@pytest.mark.forwarder
def test_custom_topic_payload_and_existing_forward_chain_are_preserved():
    forwarder = _forwarder()
    headers = {"X-Forwarded-From": ["upstream-platform"], "custom": "value"}
    call = _publish_one(forwarder, "foo/grid_signal", 78.5, headers)

    assert call["topic"] == "foo/grid_signal"
    assert call["message"] == 78.5
    assert call["headers"]["custom"] == "value"
    assert call["headers"]["X-Forwarded"] is True
    assert call["headers"]["X-Forwarded-From"] == [
        "upstream-platform",
        "source-platform",
    ]


@pytest.mark.forwarder
def test_topic_replacement_is_applied_before_publish():
    forwarder = _forwarder(
        topic_replace_list=[{"from": "BUILDING_1", "to": "BUILDING1_ANON"}]
    )
    call = _publish_one(
        forwarder,
        "devices/PNNL/BUILDING_1/Device/all",
        [{"Temperature": 70}, {"Temperature": {"units": "F"}}],
    )

    assert call["topic"] == "devices/PNNL/BUILDING1_ANON/Device/all"
