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

"""A collection of utilities used across the project."""

import base64
import hashlib

from Crypto.Cipher import AES
from Crypto import Random
from oslo_log import log as logging

from arestor.common import exception
from arestor import config as arestor_config

CONFIG = arestor_config.CONFIG
LOG = logging.getLogger(__name__)


def get_attribute(root, attribute):
    """Search for the received attribute name in the object tree.

    :param root: the root object
    :param attribute: the name of the required attribute
    """
    command_tree = [root]
    while command_tree:
        current_object = command_tree.pop()
        if hasattr(current_object, attribute):
            return getattr(current_object, attribute)

        parent = getattr(current_object, "parent", None)
        if parent:
            command_tree.append(parent)

    raise exception.ArestorException("The %(attribute)r attribute is "
                                     "missing from the object tree.",
                                     attribute=attribute)


class AESCipher(object):

    """Wrapper over AES Cipher."""

    def __init__(self, key):
        """Setup the new instance."""
        self._block_size = AES.block_size
        self._key = hashlib.sha256(key.encode()).digest()

    def encrypt(self, message):
        """Encrypt the received message."""
        message = self._padding(message, self._block_size)
        initialization_vector = Random.new().read(self._block_size)
        cipher = AES.new(self._key, AES.MODE_CBC, initialization_vector)
        return base64.b64encode(initialization_vector +
                                cipher.encrypt(message))

    def decrypt(self, message):
        """Decrypt the received message."""
        message = base64.b64decode(message)
        initialization_vector = message[:self._block_size]
        cipher = AES.new(self._key, AES.MODE_CBC, initialization_vector)
        raw_message = cipher.decrypt(message[self._block_size:])
        return self._remove_padding(raw_message).decode('utf-8')

    @staticmethod
    def _padding(message, block_size):
        """Add padding."""
        return (message + (block_size - len(message) % block_size) *
                chr(block_size - len(message) % block_size))

    @staticmethod
    def _remove_padding(message):
        """Remove the padding."""
        return message[:-ord(message[len(message) - 1:])]
