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
from query_predictor.predictor.constant import CPU_CATEGORY
from query_predictor.predictor.metadata import Metadata
from query_predictor.predictor.metadata import MetadataException
from query_predictor.predictor.version import Version

import pytest


def test_metadata():
    name = "XGBoost-CPU-Model"
    description = (
        "An XGBoost model to predict the CPU time range of SQL queries"
    )
    version = Version("1.0.1")
    category = CPU_CATEGORY

    metadata = Metadata(
        name=name, description=description, version=version, category=category
    )

    assert metadata.name == name, "Metadata name should be the same"
    assert (
        metadata.description == description
    ), "Metadata description should be the same"
    assert metadata.version == version, "Metadata version should be the same"
    assert (
        metadata.category == category
    ), "Metadata category should be the same"


def test_get_category_from_str():
    category_str = "cpu_time_label"
    category = Metadata.get_category_from_str(category_str)
    assert category == CPU_CATEGORY, "CPU category should be obtained"

    category_str = "unknown_label"
    with pytest.raises(MetadataException) as err:
        Metadata.get_category_from_str(category_str)
    assert (
        "Unknown category" in err.value.args[0]
    ), "A metadata exception is expected"
