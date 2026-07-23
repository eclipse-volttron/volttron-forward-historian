# -*- coding: utf-8 -*- {{{
# ===----------------------------------------------------------------------===
#
#                 Component of Eclipse VOLTTRON
#
# ===----------------------------------------------------------------------===
#
# Copyright 2023 Battelle Memorial Institute
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy
# of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
#
# ===----------------------------------------------------------------------===
# }}}

import datetime
import logging
import os
import sys
import time
import traceback
from urllib.parse import urlparse

import gevent

from volttron.client.vip.agent import Agent, Unreachable
from volttron import utils
from historian.base import BaseHistorian
from volttron.client.messaging import headers as headers_mod

from volttron.client.vip.agent.subsystems.health import (STATUS_BAD,
                                                STATUS_GOOD, Status)
from zmq.green import ZMQError, ENOTSOCK

FORWARD_TIMEOUT_KEY = 'FORWARD_TIMEOUT_KEY'
_log = logging.getLogger(__name__)
__version__ = '2.0.0rc0'


def historian(config_path, **kwargs):
    if isinstance(config_path, dict):
        config = config_path
    else:
        config = utils.load_config(config_path)
    custom_topic_list = config.pop('custom_topic_list', [])
    topic_replace_list = config.pop('topic_replace_list', [])
    # Support both dash and underscore separated keys.
    destination_vip = config.pop('destination-vip', None) or config.pop('destination_vip', None)
    service_topic_list = config.pop('service_topic_list', None)
    destination_serverkey = None
    destination_address = config.pop('destination-address', None) or config.pop('destination_address', None)
    if service_topic_list is not None:
        w = "Deprecated service_topic_list.  Use capture_device_data " \
            "capture_log_data, capture_analysis_data or capture_record_data " \
            "instead!"
        _log.warning(w)

        # Populate the new values for the kwargs based upon the old data.
        kwargs['capture_device_data'] = True if ("device" in service_topic_list  or "all" in service_topic_list) else False
        kwargs['capture_log_data'] = True if ("datalogger" in service_topic_list or "all" in service_topic_list) else False
        kwargs['capture_record_data'] = True if ("record" in service_topic_list or "all" in service_topic_list) else False
        kwargs['capture_analysis_data'] = True if ("analysis" in service_topic_list or "all" in service_topic_list) else False

    if destination_vip:
        # In modular VOLTTRON the serverkey/authentication is negotiated natively
        # by the message bus.  A serverkey may still be supplied via config for
        # explicit CURVE authentication if desired.
        destination_serverkey = config.pop('destination-serverkey', None) or \
            config.pop('destination_serverkey', None)

    required_target_agents = config.pop('required_target_agents', [])
    cache_only = config.pop('cache_only', False)

    utils.update_kwargs_with_config(kwargs, config)

    return ForwardHistorian(destination_vip, destination_serverkey,
                            custom_topic_list=custom_topic_list,
                            topic_replace_list=topic_replace_list,
                            required_target_agents=required_target_agents,
                            cache_only=cache_only,
                            destination_address=destination_address,
                            **kwargs)


class ForwardHistorian(BaseHistorian):
    """
    This historian forwards data to another instance as if it was published
    originally to the second instance.
    """

    def __init__(self, destination_vip, destination_serverkey,
                 custom_topic_list=[],
                 topic_replace_list=[],
                 required_target_agents=[],
                 cache_only=False,
                 destination_address=None,
                 **kwargs):
        kwargs["process_loop_in_greenlet"] = True
        super(ForwardHistorian, self).__init__(**kwargs)

        # will be available in both threads.
        self._topic_replace_map = {}
        self.topic_replace_list = topic_replace_list
        self._num_failures = 0
        self._last_timeout = 0
        self._target_platform = None
        self._current_custom_topics = set()
        self.destination_vip = destination_vip
        self.destination_serverkey = destination_serverkey
        self.required_target_agents = required_target_agents
        self.cache_only = cache_only
        self.destination_address = destination_address
        config = {
            "custom_topic_list": custom_topic_list,
            "topic_replace_list": self.topic_replace_list,
            "required_target_agents": self.required_target_agents,
            "destination_vip": self.destination_vip,
            "destination_serverkey": self.destination_serverkey,
            "cache_only": self.cache_only,
            "destination_address": self.destination_address
        }

        self.update_default_config(config)

        # We do not support the insert RPC call.
        self.no_insert = True
        # We do not support the query RPC call.
        self.no_query = True

    @staticmethod
    def _config_get(configuration, *keys, default=None):
        """Return the first meaningful value among the provided keys, allowing
        both dash and underscore separated key conventions.

        A key whose value is ``None`` or an empty string is skipped so that an
        empty default (e.g. ``destination_vip``) does not shadow the populated
        alternate key (e.g. ``destination-vip``)."""
        for key in keys:
            if key in configuration:
                value = configuration[key]
                if value is not None and value != "":
                    return value
        return default

    def configure(self, configuration):
        custom_topic_set = set(configuration.get('custom_topic_list', []))
        destination_vip = self._config_get(configuration, 'destination_vip', 'destination-vip', default="")
        self.destination_vip = str(destination_vip) if destination_vip else ""
        destination_serverkey = self._config_get(configuration, 'destination_serverkey',
                                                 'destination-serverkey', default="")
        self.destination_serverkey = str(destination_serverkey) if destination_serverkey else ""
        self.required_target_agents = self._config_get(configuration, 'required_target_agents',
                                                       'required-target-agents', default=[])
        self.topic_replace_list = self._config_get(configuration, 'topic_replace_list',
                                                   'topic-replace-list', default=[])
        self.cache_only = self._config_get(configuration, 'cache_only', 'cache-only', default=False)
        self.destination_address = self._config_get(configuration, 'destination_address',
                                                    'destination-address', default=None)
        # Reset the replace map.
        self._topic_replace_map = {}

        # Topics to add.
        new_topics = custom_topic_set - self._current_custom_topics
        # Topics to remove
        old_topics = self._current_custom_topics - custom_topic_set

        for prefix in new_topics:
            _log.info("Subscribing to {}".format(prefix))
            try:
                self.vip.pubsub.subscribe(peer='pubsub',
                                          prefix=prefix,
                                          callback=self.capture_data).get(timeout=5.0)
                self._current_custom_topics.add(prefix)
            except (gevent.Timeout, Exception) as e:
                _log.error("Failed to subscribe to {}: {}".format(prefix, repr(e)))

        for prefix in old_topics:
            _log.info("unsubscribing from {}".format(prefix))
            try:
                self.vip.pubsub.unsubscribe(peer='pubsub',
                                            prefix=prefix,
                                            callback=self.capture_data).get(timeout=5.0)
                self._current_custom_topics.remove(prefix)
            except (gevent.Timeout, Exception) as e:
                _log.error("Failed to unsubscribe from {}: {}".format(prefix, repr(e)))

    # Stop the BaseHistorian from setting the health status
    def _update_status(self, *args, **kwargs):
        pass

    def _send_alert(self, *args, **kwargs):
        pass

    # Redirect the normal capture functions to capture_data.
    def _capture_device_data(self, peer, sender, bus, topic, headers, message):
        parts = topic.split('/')
        device = '/'.join(parts[1:-1])
        # msg = [{data},{meta}] format
        msg = [{}, {}]
        try:
            # If the filter is empty pass all data.
            if self._device_data_filter:
                for _filter, point_list in self._device_data_filter.items():
                    # If filter is not empty only topics that contain the key
                    # will be kept.
                    if _filter in device:
                        for point in point_list:
                            # devices all publish
                            if isinstance(message, list):
                                # Only points in the point list will be added to the message payload
                                if point in message[0]:
                                    msg[0][point] = message[0][point]
                                    msg[1][point] = message[1][point]
                            else:
                                # other devices publish (devices/campus/building/device/point)
                                msg = None
                                if point in device:
                                    msg = message
                                    # if the point in in the parsed topic then exit for loop
                                    break
                if (isinstance(msg, list) and not msg[0]) or \
                        (isinstance(msg, (float, int, str)) and msg is None):
                    _log.debug("Topic: {} - is not in configured to be forwarded".format(topic))
                    return
            else:
                msg = message
        except Exception as e:
            _log.debug("Error handling device_data_filter. {}".format(e))
            msg = message
        self.capture_data(peer, sender, bus, topic, headers, msg)

    def _capture_log_data(self, peer, sender, bus, topic, headers, message):
        self.capture_data(peer, sender, bus, topic, headers, message)

    def _capture_analysis_data(self, peer, sender, bus, topic, headers, message):
        self.capture_data(peer, sender, bus, topic, headers, message)

    def _capture_record_data(self, peer, sender, bus, topic, headers, message):
        self.capture_data(peer, sender, bus, topic, headers, message)

    def timestamp(self):
        return time.mktime(datetime.datetime.now().timetuple())

    def capture_data(self, peer, sender, bus, topic, headers, message):

        # Grab the timestamp string from the message (we use this as the
        # value in our readings at the end of this method)
        timestamp_string = headers.get(headers_mod.DATE, None)

        data = message
        try:
            # 2.0 agents compatability layer makes sender = pubsub.compat
            # so we can do the proper thing when it is here
            # _log.debug("message in capture_data {}".format(message))
            if sender == 'pubsub.compat':
                # data = jsonapi.loads(message[0])
                data = compat.unpack_legacy_message(headers, message)
                #_log.debug("data in capture_data {}".format(data))
            if isinstance(data, dict):
                data = data
            elif isinstance(data, (int, float)):
                data = data
                # else:
                #     data = data[0]
        except ValueError as e:
            log_message = "message for {topic} bad message string:" \
                          "{message_string}"
            _log.error(log_message.format(topic=topic,
                                          message_string=message[0]))
            raise

        if self.topic_replace_list:
            original_topic = topic
            if topic in self._topic_replace_map.keys():
                topic = self._topic_replace_map[original_topic]
            else:
                self._topic_replace_map[topic] = original_topic
                temptopics = {}
                for x in self.topic_replace_list:
                    if x['from'] in topic:
                        new_topic = temptopics.get(topic, topic)
                        temptopics[topic] = new_topic.replace(
                            x['from'], x['to'])

                for k, v in temptopics.items():
                    self._topic_replace_map[k] = v
                topic = self._topic_replace_map[topic]

            # if the topic wasn't changed then we don't forward anything for
            # it unless topic_replace_list was empty. Since topic_replace_list
            # is populated, we only drop it if we explicitly want strict anonymization.
            # However, standard VOLTTRON behavior is to forward regardless unless specified.
            # To preserve standard forwarding behavior, we can log a debug message or skip dropping.
            _log.debug("Topic {} topic_replace_list was matched but no replacement occurred for this topic.".format(original_topic))

        if self.gather_timing_data:
            add_timing_data_to_header(headers, self.core.agent_uuid or self.core.identity, "collected")

        payload = {'headers': headers, 'message': data}

        self._event_queue.put({'source': "forwarded",
                               'topic': topic,
                               'readings': [(timestamp_string, payload)]})

    def publish_to_historian(self, to_publish_list):
        if self.cache_only:
            _log.warning("cache_only enabled")
            return

        handled_records = []

        _log.debug("publish_to_historian number of items: {}"
                   .format(len(to_publish_list)))
        parsed = urlparse(self.core.address)
        next_dest = urlparse(self.destination_vip)
        current_time = self.timestamp()
        last_time = self._last_timeout
        _log.debug('Lasttime: {} currenttime: {}'.format(last_time,
                                                         current_time))
        timeout_occurred = False
        if self._last_timeout:
            # if we failed we need to wait 60 seconds before we go on.
            if self.timestamp() < self._last_timeout + 60:
                _log.debug('Not allowing send < 60 seconds from failure')
                return

        if not self._target_platform:
            self.historian_setup()
        if not self._target_platform:
            _log.error('Could not connect to targeted historian dest_vip {} dest_address {}'.format(
                self.destination_vip, self.destination_address))
            return

        for vip_id in self.required_target_agents:
            try:
                self._target_platform.vip.ping(vip_id).get()
            except Unreachable:
                skip = "Skipping publish: Target platform not running " \
                       "required agent {}".format(vip_id)
                _log.warning(skip)
                self.vip.health.set_status(
                    STATUS_BAD, skip)
                return
            except Exception as e:
                err = "Unhandled error publishing to target platform."
                _log.error(err)
                _log.error(traceback.format_exc())
                self.vip.health.set_status(
                    STATUS_BAD, err)
                return

        for x in to_publish_list:
            topic = x['topic']
            value = x['value']
            # payload = jsonapi.loads(value)
            payload = value
            headers = payload['headers']
            headers['X-Forwarded'] = True
            if 'X-Forwarded-From' in headers:
                if not isinstance(headers['X-Forwarded-From'], list):
                    headers['X-Forwarded-From'] = [headers['X-Forwarded-From']]
                headers['X-Forwarded-From'].append(self.instance_name)
            else:
                headers['X-Forwarded-From'] = self.instance_name

            try:
                del headers['Origin']
            except KeyError:
                pass
            try:
                del headers['Destination']
            except KeyError:
                pass

            if self.gather_timing_data:
                add_timing_data_to_header(headers,
                                          self.core.agent_uuid or self.core.identity,
                                          "forwarded")

            if timeout_occurred:
                _log.error(
                    'A timeout has occurred so breaking out of publishing')
                break
            with gevent.Timeout(30):
                try:
                    self._target_platform.vip.pubsub.publish(
                        peer='pubsub',
                        topic=topic,
                        headers=headers,
                        message=payload['message']).get()
                except gevent.Timeout:
                    _log.debug("Timeout occurred email should send!")
                    timeout_occurred = True
                    self._last_timeout = self.timestamp()
                    self._num_failures += 1
                    # Stop the current platform from attempting to
                    # connect
                    self.historian_teardown()
                    self.vip.health.set_status(
                        STATUS_BAD, "Timeout occured")
                except Unreachable:
                    _log.error("Target not reachable. Wait till it's ready!")
                except ZMQError as exc:
                    if exc.errno == ENOTSOCK:
                        # Stop the current platform from attempting to
                        # connect
                        _log.error("Target disconnected. Stopping target platform agent")
                        self.historian_teardown()
                        self.vip.health.set_status(
                            STATUS_BAD, "Target platform disconnected")
                except Exception as e:
                    err = "Unhandled error publishing to target platfom."
                    _log.error(err)
                    _log.error(traceback.format_exc())
                    self.vip.health.set_status(
                        STATUS_BAD, err)
                    # Before returning lets mark any that weren't errors
                    # as sent.
                    self.report_handled(handled_records)
                    return
                else:
                    handled_records.append(x)

        _log.debug("handled: {} number of items".format(
            len(to_publish_list)))
        self.report_handled(handled_records)

        if timeout_occurred:
            _log.debug('Sending alert from the ForwardHistorian')
            status = Status.from_json(self.vip.health.get_status_json())
            self.vip.health.send_alert(FORWARD_TIMEOUT_KEY,
                                       status)
        else:
             self.vip.health.set_status(
                STATUS_GOOD,"published {} items".format(
                    len(to_publish_list)))

    def historian_setup(self):
        address = self.destination_address or self.destination_vip
        if not address:
            _log.warning("No destination address/vip configured. Skipping forward setup.")
            return

        _log.debug("Setting up to forward to {}".format(address))

        try:
            # Reuse this agent's own credentials (provisioned by the platform in
            # VOLTTRON_HOME/credentials_store) to build an outbound connection to
            # the remote platform. The message bus negotiates authentication natively.
            credentials = self.get_credentials(self.core.identity)

            # Force inclusion of the remote server key directly in the connection address query params
            from urllib.parse import urlsplit, urlunsplit, parse_qs
            url = list(urlsplit(address))
            query_dict = parse_qs(url[3])
            if self.destination_serverkey:
                query_dict['serverkey'] = [self.destination_serverkey]
            if credentials:
                query_dict['publickey'] = [credentials.publickey]
                query_dict['secretkey'] = [credentials.secretkey]

            # Rebuild query string
            import urllib.parse
            url[3] = urllib.parse.urlencode(query_dict, doseq=True)
            address = urlunsplit(url)

            remote_agent = Agent(address=address, credentials=credentials)

            # Spawn the remote agent's core event loop and wait for it to connect.
            event = gevent.event.Event()
            gevent.spawn(remote_agent.core.run, event)
            with gevent.Timeout(30):
                event.wait()

        except gevent.Timeout:
            _log.error("Couldn't connect to address. gevent timeout: ({})".format(address))
            self.vip.health.set_status(STATUS_BAD, "Timeout in setup of agent")
        except Exception as ex:
            _log.error("Error connecting to remote platform {}: {}".format(address, ex))
            self.vip.health.set_status(STATUS_BAD, "Error message: {}".format(ex))
        else:
            self._target_platform = remote_agent
            self.vip.health.set_status(
                STATUS_GOOD, "Connected to address ({})".format(address))

    def historian_teardown(self):
        # Kill the forwarding agent if it is currently running.
        if self._target_platform is not None:
            try:
                self._target_platform.core.stop()
            except Exception as ex:
                _log.debug("Error stopping target platform connection: {}".format(ex))
            self._target_platform = None

    def stopping(self, sender, **kwargs):
        """
        Release the message bus subscriptions on shutdown.

        Guards the unsubscribe against a torn-down/unconnected socket so that a
        failed startup or an already-closed connection does not raise during
        shutdown (the modular message bus asserts on a live socket).
        """
        # Ensure any remote connection is cleaned up first.
        self.historian_teardown()

        if getattr(self, "_readonly", False):
            return

        try:
            self.stop_process_thread()
        except Exception as ex:
            _log.debug("Error stopping process thread: {}".format(ex))

        # Only attempt to unsubscribe if our core connection is still live.
        if not self.core.connected:
            _log.debug("Core not connected; skipping pubsub unsubscribe on stop.")
            return

        try:
            self.vip.pubsub.unsubscribe(peer='pubsub', prefix=None, callback=None)
        except (KeyError, AssertionError, ZMQError) as ex:
            # KeyError: subscriptions never completed setup.
            # AssertionError/ZMQError: the socket was already closed.
            _log.debug("Skipping unsubscribe during shutdown: {}".format(repr(ex)))

    def version(self):
        return __version__

    def query_historian(self, *args, **kwargs):
        raise NotImplementedError("ForwardHistorian does not support querying.")

    def query_topics_metadata(self, *args, **kwargs):
        raise NotImplementedError("ForwardHistorian does not support querying.")

    def query_topics_by_pattern(self, *args, **kwargs):
        raise NotImplementedError("ForwardHistorian does not support querying.")

    def query_topic_list(self, *args, **kwargs):
        raise NotImplementedError("ForwardHistorian does not support querying.")

    def query_aggregate_topics(self, *args, **kwargs):
        raise NotImplementedError("ForwardHistorian does not support querying.")


def main(argv=sys.argv):
    """Main method called by the aip."""
    try:
        utils.vip_main(historian, version=__version__)
    except Exception as e:
        print(e)
        _log.exception('unhandled exception')


if __name__ == '__main__':
    # Entry point for script
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        pass
