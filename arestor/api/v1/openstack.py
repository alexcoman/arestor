# Copyright 2016 Cloudbase Solutions Srl
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

"""Arestor API endpoint for OpenStack Mocked Metadata."""

import cherrypy
import json

from oslo_log import log as logging

from arestor.api import base as base_api
from arestor.common import exception
from arestor.common import util as arestor_util


LOG = logging.getLogger(__name__)

KEY_FORMAT = "{namespace}/{user}/{name}"

class _Content(base_api.Resource):

    """Content resource for the OpenStack Endpoint."""

    exposed = True

    def GET(self):
        """The representation of the content resource."""
        pass

class _DatabaseResource(base_api.Resource):

    """Database accessible resource for OpenStack Endpoint."""

    def _get_openstack_data(self, name, field=None):
        """Retrieve the required resource from the Openstack namespace."""
        data = ""
        try:
            data = self._get_data(namespace="openstack",
                                  name=name, field=field)
            data = json.loads(data)
        except (ValueError, TypeError):
            return data
        except exception.NotFound:
            return None
        return data

    def _set_openstack_data(self, name, field=None, value = None):
        """Set the required resource from the Openstack namespace."""
        try:
            self._set_data(namespace="openstack",
                                  name=name, field=field, value=value)
        except (ValueError, TypeError):
            return data
        except exception.NotFound:
            return None

class _MetadataResource(_DatabaseResource):

    """Metadata resource for OpenStack Endpoint."""

    exposed = True

    @cherrypy.tools.json_out()
    def GET(self):
        """The representation of the metadata resource."""
        meta_data = {
            "random_seed": self._get_openstack_data("random_seed", "data"),
            "uuid": self._get_openstack_data("uuid", "data"),
            "availability_zone": self._get_openstack_data("availability_zone",
                                                          "data"),
            "hostname": self._get_openstack_data("hostname", "data"),
            "meta": self._get_openstack_data("metadata", "data"),
            "launch_index": self._get_openstack_data("launch_index", "data"),
            "project_id": self._get_openstack_data("project_id", "data"),
            "name": self._get_openstack_data("name", "data"),
            "keys": self._get_openstack_data("keys", "data"),
            "public_keys": self._get_openstack_data("public_keys", "data"),
        }
        return meta_data

class _UserdataResource(_DatabaseResource):

    exposed = True

    """Userdata resource for OpenStack Endpoint."""
    def GET(self):
        """The representation of userdata resource."""
        return self._get_openstack_data("userdata", "data")


class _PasswordResource(_DatabaseResource):

    exposed = True

    """Password resource for OpenStack Endpoint."""
    def GET(self, resource_id=None, namespace=None, client_id=None,
            resource=None):
        return self._get_openstack_data("password", "data")

    def POST(self):
        """Create a new resource."""
        #import pdb;pdb.set_trace()
        self._set_openstack_data("password", "data", str(cherrypy.request.body.read()))
        return {"meta": {"status": True, "verbose": "Ok"}, "content": None}



class _LatestVersion(base_api.BaseAPI):

    """Container for all the resources from the latest version of the API."""

    exposed = True
    resources = [
        ("user_data", _UserdataResource),
        ("meta_data_json", _MetadataResource),
    ]


class _LegacyVersion(base_api.BaseAPI):

    """Containe for all the resources from the legacy version of the API."""

    exposed = True
    resources = [
        ("password", _PasswordResource),
        ("user_data", _UserdataResource),
        ("meta_data_json", _MetadataResource),

    ]


class OpenStackEndpoint(base_api.BaseAPI):

    """Arestor API endpoint for OpenStack Mocked Metadata."""

    resources = [
        ("2013-04-04", _LegacyVersion),
        ("latest", _LatestVersion),
        ("content", _Content),
    ]
    """A list that contains all the resources (endpoints) available for the
    current metadata service."""

    exposed = True
    """Whether this application should be available for clients."""

    def __getattr__(self, name):
        """Handle for invalid resource name or alias."""

        # Note(alexcoman): The cherrypy MethodDispatcher will replace the
        # `-` from the resource / endpoint name with `_`. In order to avoid
        # any problems we will try to avoid this scenario.
        if "_" in name:
            return self.__dict__.get(name.replace("_", "-"))

        raise AttributeError("%r object has no attribute %r" %
                             (self.__class__.__name__, name))
