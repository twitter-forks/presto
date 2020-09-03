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
from query_predictor.predictor.exceptions import VersionException
from query_predictor.predictor.version import Version

import pytest


def test_version():
    version = Version((1, 0, 1))

    assert version.version == "1.0.1", "Version should be 1.0.1"
    assert version.version_info == (
        1,
        0,
        1,
    ), "Version info should be (1, 0, 1)"

    version = Version("1.0.1")
    assert version.version == "1.0.1", "Version should be 1.0.1"
    assert version.version_info == (
        1,
        0,
        1,
    ), "Version info should be (1, 0, 1)"

    with pytest.raises(VersionException) as err:
        Version("1.0.0.2")
    assert (
        "A version info tuple should be smaller than or equal to 3"
        in err.value.args[0]
    ), "A version exception is expected"
