"""Configuration and reconnect behavior of the Forward Historian."""

from types import SimpleNamespace

import pytest

from historian.forwarder.forwarder import ForwardHistorian


class _Future:
    def get(self, timeout=None):
        return True


class _PubSub:
    def __init__(self):
        self.subscribed = []

    def subscribe(self, **kwargs):
        self.subscribed.append(kwargs)
        return _Future()

    def unsubscribe(self, **kwargs):
        return _Future()


def _configurable_forwarder():
    forwarder = ForwardHistorian.__new__(ForwardHistorian)
    forwarder.destination_address = None
    forwarder.destination_vip = "old-vip"
    forwarder.destination_serverkey = "old-key"
    forwarder.required_target_agents = []
    forwarder.topic_replace_list = []
    forwarder.cache_only = False
    forwarder._topic_replace_map = {"old": "old"}
    forwarder._current_custom_topics = set()
    forwarder._last_timeout = 123
    forwarder._target_platform = object()
    forwarder.vip = SimpleNamespace(pubsub=_PubSub())
    forwarder.historian_teardown_calls = 0

    def teardown():
        forwarder.historian_teardown_calls += 1
        forwarder._target_platform = None

    forwarder.historian_teardown = teardown
    return forwarder


@pytest.mark.forwarder
def test_config_update_replaces_destination_and_reconnects():
    forwarder = _configurable_forwarder()

    forwarder.configure(
        {
            "destination-address": "tcp://127.0.0.1:22917",
            "destination-serverkey": "new-key",
            "custom_topic_list": ["foo"],
            "topic-replace-list": [{"from": "old", "to": "new"}],
            "cache-only": True,
        }
    )

    assert forwarder.destination_address == "tcp://127.0.0.1:22917"
    assert forwarder.destination_serverkey == "new-key"
    assert forwarder.topic_replace_list == [{"from": "old", "to": "new"}]
    assert forwarder.cache_only is True
    assert forwarder.historian_teardown_calls == 1
    assert forwarder._target_platform is None
    assert forwarder._last_timeout == 0
    assert forwarder._topic_replace_map == {}
    assert [item["prefix"] for item in forwarder.vip.pubsub.subscribed] == ["foo"]


@pytest.mark.forwarder
def test_config_update_removes_custom_topic_subscription():
    forwarder = _configurable_forwarder()
    forwarder._current_custom_topics = {"foo", "bar"}

    forwarder.configure({"custom_topic_list": ["bar"]})

    assert forwarder._current_custom_topics == {"bar"}
