# Copyright 2017 Cloudbase Solutions Srl
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import abc

from oslo_log import log as logging
import redis
import six

from arestor.common import exception
from arestor import config as arestor_config

CONFIG = arestor_config.CONFIG
LOG = logging.getLogger(__name__)


@six.add_metaclass(abc.ABCMeta)
class _Connector(object):

    """The contact class for all the connectors."""

    def __init__(self, auto_connect=False):
        self._connection = None
        if auto_connect:
            self.refresh()

    @abc.abstractmethod
    def _connect(self):
        """Try establishing a connection."""
        pass

    @abc.abstractmethod
    def is_alive(self):
        """Check if the current connector is still alive."""
        pass

    @abc.abstractmethod
    def refresh(self):
        """Re-establish the connection only if is dropped."""
        pass

    @abc.abstractmethod
    def get(self, resource_id, model_name=None):
        """Retrieves the required resources."""
        pass

    @abc.abstractmethod
    def get_all(self, model_name):
        """Return all the resources from the received model."""
        pass

    @abc.abstractmethod
    def remove(self, resource_id, model_name=None):
        """Delete the required resource."""
        pass

    @abc.abstractmethod
    def set(self, model):
        """Create the required resource."""
        pass


class RedisConnector(_Connector):

    """Redis database connector."""

    def __init__(self, auto_connect=True):
        """Instantiates objects able to store and retrieve data."""
        self._host = CONFIG.redis.host
        self._port = CONFIG.redis.port
        self._db = CONFIG.redis.database
        self._model_cache = {}
        super(RedisConnector, self).__init__(auto_connect=auto_connect)

    def is_alive(self):
        """Check if the current connector is still alive."""
        try:
            if self._connection and self._connection.ping():
                return True
        except redis.ConnectionError as exc:
            # Note(alexcoman): Failed to establish a redis connection
            LOG.debug("Redis connection error: %s", exc)
        return False

    def _dump_content(self, root, prefix=""):
        content = {}
        for key, value in root.items():
            content_key = key if not prefix else "{}.{}".format(prefix, key)
            if isinstance(value, dict):
                child_content = self._process_content(value, content_key)
                content.update(child_content)
            else:
                content[content_key] = value
        return content

    def _load_content(self, data):
        content = {}
        for key, value in data.items():
            entities = key.split(".")
            container_key = entities.pop()
            container = content
            while entities:
                container = container.setdefault(entities.pop(0), {})
            container[container_key] = value
        return content

    def _connect(self):
        """Try establishing a connection until succeeds."""
        try:
            self._connection = redis.StrictRedis(self._host, self._port,
                                                 self._db)
            # Return the connection only if is valid and reachable
            if not self.is_alive():
                return True
        except (redis.ConnectionError, redis.RedisError) as exc:
            LOG.error("Failed to connect to Redis Server: %s", exc)
            return False

    def _set_field(self, model_name, resource_id, field, value):
        field_key = "{resource}.{field}".format(resource=resource_id, field=field)
        self._connection.hset(model_name, field_key, json.dumps(value))

    def _get_field(self, model_name, resource_id, field):
        field_key = "{resource}.{field}".format(resource=resource_id, field=field)
        value = self._connection.hfet(model_name, field_key)
        return json.dumps(value)

    def _add_resource(self, model_name, resource_id, content):
        # Add the resource_id of the current model to the models container
        models_key = "models.{model}".format(model=model_name)
        self._connection.sadd(models_key, resource_id)

        # Save all the available fields from the current model in
        # order to ease the model reconstruction
        schema_key = "{resource}.fields".format(resource=resource_id)
        self._connection.hset("schema", schema_key, ",".join(content.keys()))

        # Save all the fields of the current model to the database
        for field, value in content.items():
            self._set_field(model_name, resource_id, field, value)

    def _get_resource(self, model_name, resource_id):
        resource = {}

        models_key = "models.{model}".format(model=model_name)
        if not self._connection.sismember(models_key, resource_id):
            # TODO(alexcoman): The required resources doesn't exists
            pass

        schema_key = "{resource}.fields".format(resource=resource_id)
        resource_keys = self._connection.hget("schema", schema_key)
        for key in resource_keys.split(","):
            field = self._get_field(model_name, resource_id, field)
            resource[key] = field
        return resource

    def _remove_resource(self, model_name, resource_id):
        models_key = "models.{model}".format(model=model_name)
        self._connection.srem(models_key, resource_id)

        schema_key = "{resource}.fields".format(resource=resource_id)
        resource_keys = self._connection.hget("schema", schema_key)
        self._connection.hdel("schema", schema_key)

        # TODO(alexcoman): Remove all the informaation regarding fields.

    def refresh(self):
        """Re-establish the connection only if is dropped."""
        for _ in range(CONFIG.retry_count):
            if self.is_alive() or self._connect():
                break
        else:
            raise exception.ArestorException(
                "Failed to connect to Redis Server.")

        return True

    def get(self, resource_id, model_name=None):
        """Retrieves the required resources."""
        # Check if the connection with the database is stil alive
        self.refresh()

        # Get the raw representation of the current resource
        content = self._get_resource(resource_id=resource_id,
                                     model_name=model_name)

        # Return all the information related to the required resource
        return self._load_content(content)

    def get_all(self, model_name):
        """Return all the resources from the received model."""
        # Check if the connection with the database is stil alive
        self.refresh()

        resources = []
        models_key = "models.{model}".format(model=model_name)
        for resource_id in self._connection.smembers(models_key):
            # Get the raw representation of the current resource
            content = self._get_resource(resource_id=resource_id,
                                         model_name=model_name)
            resources.append(self._load_content(content))
        return resources

    @abc.abstractmethod
    def remove(self, resource_id, model_name=None):
        """Delete the required resource."""
        # Check if the connection with the database is stil alive
        self.refresh()





    def set(self, model):
        """Create the required resource."""
        # Check if the connection with the database is stil alive
        self.refresh()

        # TODO(alexcoman): If the resource should be created (the resource_id
        # is not present in the `models.model_type` set) then all the content
        # should be used (`model.dump()`) otherwise we can use only the content
        # from the `_change` internal dictionary.

        # Proces the content of the current model in order to be
        # ease the data representation in Redis Database.
        content = self._dump_content(model.dump())

        # Save all the fields value for the current model
        self._add_resource(model_name=model.__class__.__name__,
                           resource_id=model.resource_id,
                           content=content)
