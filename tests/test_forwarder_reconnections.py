"""Configuration and reconnect behavior of the Forward Historian."""

from types import SimpleNamespace

import gevent
import pytest

from historian.forwarder import forwarder as forwarder_module
from historian.forwarder.forwarder import ForwardHistorian
from volttron.types.auth.auth_credentials import PKICredentials


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


class _FakeRemoteCore:
    """Stands in for the remote Agent's core: optionally never connects."""

    def __init__(self, connects, stop_result=True):
        self.connects = connects
        self.stop_result = stop_result
        self.stop_timeouts = []
        self.greenlet = None

    def run(self, event):
        self.greenlet = gevent.getcurrent()
        if self.connects:
            event.set()
        gevent.sleep(3600)

    def stop(self, timeout=None):
        self.stop_timeouts.append(timeout)
        if self.stop_result:
            self.greenlet.kill(block=False)
        return self.stop_result


class _FakeRemoteAgent:
    connects = False
    stop_result = True
    instances = []

    def __init__(self, address=None, credentials=None):
        self.address = address
        self.credentials = credentials
        self.core = _FakeRemoteCore(self.connects, self.stop_result)
        _FakeRemoteAgent.instances.append(self)


def _connecting_forwarder(monkeypatch, connects, stop_result=True):
    forwarder = ForwardHistorian.__new__(ForwardHistorian)
    forwarder.destination_address = "tcp://127.0.0.1:22916"
    forwarder.destination_vip = None
    forwarder.destination_serverkey = "destination-server-key"
    forwarder._target_platform = None
    forwarder.connection_timeout = 0.05
    forwarder.core = SimpleNamespace(identity="forwarder")
    forwarder.instance_name = "source-platform"
    forwarder.get_credentials = lambda identity: PKICredentials(identity=identity, publickey="pk", secretkey="sk")
    forwarder.statuses = []
    forwarder.vip = SimpleNamespace(
        health=SimpleNamespace(set_status=lambda status, context=None: forwarder.statuses.append(status))
    )
    _FakeRemoteAgent.connects = connects
    _FakeRemoteAgent.stop_result = stop_result
    _FakeRemoteAgent.instances = []
    monkeypatch.setattr(forwarder_module, "Agent", _FakeRemoteAgent)
    return forwarder


@pytest.mark.forwarder
def test_setup_timeout_stops_the_orphaned_remote_core(monkeypatch):
    """A core that misses the deadline must be stopped, not left running.

    Left alone it can still connect when a late welcome arrives and then hold the
    forwarder's identity at the destination for the life of the process, after
    which every retry is silently dropped by the destination router."""
    forwarder = _connecting_forwarder(monkeypatch, connects=False)

    forwarder.historian_setup()

    (remote,) = _FakeRemoteAgent.instances
    assert forwarder._target_platform is None
    assert remote.core.stop_timeouts == [0.05]
    gevent.sleep(0)
    assert remote.core.greenlet.dead
    assert forwarder.statuses == ["BAD"]


@pytest.mark.forwarder
def test_setup_timeout_kills_the_core_when_stop_does_not_finish(monkeypatch):
    forwarder = _connecting_forwarder(monkeypatch, connects=False, stop_result=False)

    forwarder.historian_setup()

    (remote,) = _FakeRemoteAgent.instances
    assert forwarder._target_platform is None
    assert remote.core.stop_timeouts == [0.05]
    assert remote.core.greenlet.dead


@pytest.mark.forwarder
def test_setup_keeps_a_core_that_connects_in_time(monkeypatch):
    forwarder = _connecting_forwarder(monkeypatch, connects=True)

    forwarder.historian_setup()

    (remote,) = _FakeRemoteAgent.instances
    assert forwarder._target_platform is remote
    assert remote.core.stop_timeouts == []
    assert not remote.core.greenlet.dead
    assert forwarder.statuses == ["GOOD"]
    remote.core.greenlet.kill()


@pytest.mark.forwarder
def test_remote_connection_uses_the_instance_qualified_identity(monkeypatch):
    """The agent's own credentials with only the identity changed to <source instance>.<identity>,
    as Monolithic's connect_remote_platform does; otherwise every site's forwarder would claim
    the same identity at a shared destination."""
    forwarder = _connecting_forwarder(monkeypatch, connects=True)

    forwarder.historian_setup()

    (remote,) = _FakeRemoteAgent.instances
    assert remote.credentials == PKICredentials(identity="source-platform.forwarder", publickey="pk", secretkey="sk")
    assert "publickey=pk" in remote.address and "secretkey=sk" in remote.address
    assert remote.address.startswith("tcp://127.0.0.1:22916?")
    remote.core.greenlet.kill()


class _FakeQuery:
    result = None
    error = None

    def __init__(self, core):
        self.core = core

    def query(self, name):
        assert name == "instance-name"
        if self.error:
            raise self.error
        return SimpleNamespace(get=lambda timeout=None: self.result)


@pytest.mark.forwarder
def test_remote_identity_queries_the_instance_name_when_not_yet_known(monkeypatch):
    forwarder = _connecting_forwarder(monkeypatch, connects=True)
    forwarder.instance_name = None
    _FakeQuery.result, _FakeQuery.error = "queried-platform", None
    monkeypatch.setattr(forwarder_module, "Query", _FakeQuery)

    assert forwarder.remote_identity() == "queried-platform.forwarder"


@pytest.mark.forwarder
def test_remote_identity_falls_back_to_the_bare_identity(monkeypatch):
    forwarder = _connecting_forwarder(monkeypatch, connects=True)
    forwarder.instance_name = None
    _FakeQuery.result, _FakeQuery.error = None, RuntimeError("no platform")
    monkeypatch.setattr(forwarder_module, "Query", _FakeQuery)

    assert forwarder.remote_identity() == "forwarder"
