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

"""This module contains data models used across the project."""

# pylint: disable=protected-access

import copy
import uuid

from oslo_log import log as logging
import six

from arestor.common import exception

LOG = logging.getLogger(__name__)


class _FieldDescriptor(object):

    """Descriptor for all the available fields for a model.

    Fields are exposed as descriptors in order to control access to the
    underlying raw data.
    """

    def __init__(self, field):
        self._field = field
        self._attribute = field.key

    @property
    def field(self):
        """Expose the received field object."""
        return self._field

    def __get__(self, instance, instance_type=None):
        if instance is not None:
            return instance._data.get(self._attribute)
        return self._field

    def __set__(self, instance, value):
        if not isinstance(value, self.field.field_type):
            raise TypeError("Invalid type of value (should be %r)" %
                            self.field.field_type)

        if instance.provision_done:
            if self._field.is_read_only:
                raise TypeError("%r does not support item assignment" %
                                self._field.name)

        instance._changes[self._attribute] = value
        instance._data[self._attribute] = value


class Field(object):

    """Meta information regarding the data components.

    :param name:          The name of the current piece of information.
    :param key:           The internal name for the current piece of
                          information.
    :param default:       Default value for the current field.
                          (default: `None`)
    :param is_required:   Whether the current piece of information is required
                          for the container object or can be missing.
                          (default: `False`)
    :param is_read_only:     Whether the current piece of information can
                          be updated. (Default: `False`)
    """

    def __init__(self, name, key, field_type=str, default=None,
                 is_required=False, is_read_only=False):
        self._name = name
        self._key = key
        self._fields_type = field_type
        self._default = default
        self._is_required = is_required
        self._is_read_only = is_read_only

    @property
    def name(self):
        """The name of the current field."""
        return self._name

    @property
    def key(self):
        """The internal name of the current field."""
        return self._key

    @property
    def field_type(self):
        """The type of the current field."""
        return self._fields_type

    @property
    def default(self):
        """Default value for the current field."""
        return self._default

    @property
    def is_required(self):
        """Whether the current field is required or can be missing."""
        return self._is_required

    @property
    def is_read_only(self):
        """Whether the current field can be updated."""
        return self._is_read_only

    def add_to_class(self, model_class):
        """Replace the `Field` attribute with a named `_FieldDescriptor`.

        .. note::
            This method is called  during construction of the `Model`.
        """
        model_class._meta.add_field(self)
        setattr(model_class, self.name, _FieldDescriptor(self))


class _ModelOptions(object):

    """Container for all the model options.

    .. note::
        The current object will be created by the model metaclass.
    """

    def __init__(self, cls):
        self._model_class = cls
        self._name = cls.__name__

        self._fields = {}
        self._defaults = {}
        self._default_callables = {}

    @property
    def fields(self):
        """All the available fields for the current model."""
        return self._fields

    def add_field(self, field):
        """Add the received field to the model."""
        self.remove_field(field.name)
        self._fields[field.name] = field

        if field.default is not None:
            if six.callable(field.default):
                self._default_callables[field.key] = field.default
            else:
                self._defaults[field.key] = field.default

    def remove_field(self, field_name):
        """Remove the field with the received field name from model."""
        field = self._fields.pop(field_name, None)
        if field is not None and field.default is not None:
            if six.callable(field.default):
                self._default_callables.pop(field.name, None)
            else:
                self._defaults.pop(field.name, None)

    def get_defaults(self):
        """Get a dictionary that contains all the available defaults."""
        defaults = self._defaults.copy()
        for field_name, default in self._default_callables.items():
            defaults[field_name] = default()
        return defaults


class _BaseModel(type):

    """Metaclass used for properly setting up a new model."""

    def __new__(mcs, name, bases, attrs):
        # The inherit is made by deep copying the underlying field into
        # the attributes of the new model.
        for base in bases:
            for key, attribute in base.__dict__.items():
                if key not in attrs and isinstance(attribute,
                                                   _FieldDescriptor):
                    attrs[key] = copy.deepcopy(attribute.field)

        # Initialize the new class and set the magic attributes
        cls = super(_BaseModel, mcs).__new__(mcs, name, bases, attrs)

        # Create the _ModelOptions object and inject it in the new class
        setattr(cls, "_meta", _ModelOptions(cls))

        # Get all the available fields for the current model.
        for name, field in list(cls.__dict__.items()):
            if isinstance(field, Field) and not name.startswith("_"):
                field.add_to_class(cls)

        # Create string representation for the current model before finalizing
        setattr(cls, '__str__', lambda self: '%s' % cls.__name__)
        return cls


@six.add_metaclass(_BaseModel)
class BaseModel(object):

    """Container for meta information regarding the data structure."""

    def __init__(self, **fields):
        self._data = self._meta.get_defaults()
        self._changes = {}

        self._cache = {}

        self._provision_done = False
        self._set_fields(fields)
        self._provision_done = True

    def _unpack(self, value):
        """Obtain the raw representation of the received object."""
        if isinstance(value, BaseModel):
            return value.dump()

        if isinstance(value, list):
            container_list = []
            for item in value:
                container_list.append(self._unpack(item))
            return container_list

        if isinstance(value, dict):
            container_dict = {}
            for key, item in value.items():
                container_dict[key] = self._unpack(item)
            return container_dict

        return value

    def _set_fields(self, fields):
        """Set or update the fields value."""
        for field in self._meta.fields.values():
            value = fields.pop(field.name, None)
            if field.key not in self._data or value:
                setattr(self, field.name, value)

        if fields:
            LOG.debug("Unrecognized fields: %r", fields)

    @classmethod
    def process_raw_data(cls, raw_data):
        """Process the received data in order to be understood by the model."""
        content = {}
        for field_name, field in cls._meta.fields.items():
            value = raw_data.pop(field.key, None)
            content[field_name] = value

        if raw_data:
            LOG.debug("Unrecognized fields: %r", raw_data)

        return content

    @classmethod
    def from_raw_data(cls, raw_data):
        """Create a new model using raw API response."""
        content = cls.process_raw_data(raw_data)
        return cls(**content)

    @property
    def provision_done(self):
        """Whether the creation of the model is complete."""
        return self._provision_done

    def validate(self):
        """Check if the current model was properly created."""
        for field_name, field in self._meta.fields.items():
            if field.is_required and self._data.get(field.name) is None:
                raise exception.DataProcessingError(
                    "The required field %(field)r is missing.",
                    field=field_name)

    def update(self, fields):
        """Update the value of one or more fields."""
        if not fields:
            return

        for field_name, field in self._meta.fields.items():
            if field_name in fields:
                self._changes[field.key] = fields[field_name]
        self._data.update(self._changes)

    def commit(self):
        """Apply all the changes on the current model."""
        self._data.update(self._changes)
        self._changes.clear()

    def dump(self, include_read_only=True):
        """Create a dictionary with the content of the current model."""
        content = {}
        for field in self._meta.fields.values():
            if field.is_read_only and not include_read_only:
                continue

            value = self._unpack(self._data.get(field.key))
            if not field.is_required and value is None:
                # The value of this field is not relevant
                continue

            content[field.key] = value

        return content


class ArestorModel(BaseModel):

    resource_id = Field(name="resource_id", key="resource_id",
                        default=lambda: str(uuid.uuid1()))
    """The resource ID for the resource. The value MUST be unique in
    the context of the resource if it is a top-level resource, or in the
    context of the direct parent resource if it is a child resource."""

    def __init__(self, connector, **fields):
        super(ArestorModel, self).__init__(**fields)
        self._connector = connector

    def commit(self):
        """Apply all the changes on the current model."""
        self._connector.set(self, self.__class__.__name__)
        super(ArestorModel, self).commit()

    @classmethod
    def get(cls, connector, resource_id=None):
        """Retrieves the required resources.

        :param resource_id:      The identifier for the specific resource
                                 within the resource type.
        """
        if resource_id:
            resource = connector.get(cls.__name__, resource_id)
            content = cls.process_raw_data(resource)
            return cls(connector, **content)
        else:
            resources = []
            for resource in connector.get_all(cls.__name__):
                content = cls.process_raw_data(resource)
                resources.append(cls(connector, **content))
            return resources

    @classmethod
    def remove(cls, connector, resource_id):
        """Delete the required resource.

        :param resource_id:   The identifier for the specific resource
                              within the resource type.
        """
        return connector.remove(cls.__name__, resource_id)
