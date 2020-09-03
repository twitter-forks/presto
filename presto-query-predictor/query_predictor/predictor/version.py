# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
This module contains the class to represent versions.
"""
from typing import Tuple
from typing import Union

from .exceptions import VersionException


class Version:
    """
    The class to represent the version of a model

    :param version_info: The input version. If it is a tuple like (1, 0, 1), its
    length should be 3. It can also be a string like "1.0.1".
    """

    def __init__(self, version_info: Union[Tuple, str]) -> None:
        if type(version_info) is tuple:
            self.version_info = version_info
            self.version = ".".join([str(v) for v in self.version_info])
        elif type(version_info) is str:
            self.version = version_info
            self.version_info = tuple(map(int, version_info.split(".")))

        if len(self.version_info) > 3:
            raise VersionException(
                "A version info tuple should be smaller than or equal to 3"
            )

    def __str__(self) -> str:
        args = [f"version_info={self.version_info}", f"version={self.version}"]
        return "<{} {}>".format(type(self).__name__, ", ".join(args))
