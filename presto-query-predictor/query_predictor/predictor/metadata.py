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
This module contains the class to store metadata.
"""
from .constant import CATEGORIES
from .exceptions import MetadataException
from .version import Version


class Metadata:
    """
    The class to represent the metadata of a model.

    :param name: Name of the model. e.g. XGBoost-CPU-Model
    :param description: Description of the model. e.g. An XGBoost model to
    predict the CPU time range of SQL queries.
    :param version: Version of the model.
    :param category: Category of the model: cpu or memory.

    To use:
    >>> from query_predictor.predictor.metadata import Metadata
    >>> from query_predictor.predictor.version import Version
    >>>
    >>> metadata = Metadata(
    >>>     name="XGBoost-CPU-Model",
    >>>     description="An XGBoost model to predict the CPU time range of SQL queries",
    >>>     version=Version("1.0.1"),
    >>>     category="cpu"
    >>> )
    >>> print(metadata)
    """

    def __init__(
        self, name: str, description: str, version: Version, category: str
    ):
        if category not in CATEGORIES:
            raise MetadataException(f"Unknown category: {category}")

        self.name = name
        self.description = description
        self.version = version
        self.category = category

    def __str__(self) -> str:
        args = [
            f"name={self.name}",
            f"description={self.description}",
            f"version={self.version.version}",
            f"category={self.category}",
        ]
        return "<{} {}>".format(type(self).__name__, ", ".join(args))

    @staticmethod
    def get_category_from_str(category_str: str) -> str:
        """
        Gets the category from a long name. e.g. Returns "cpu" from "cpu_model".

        :param category_str: The input long name.
        :return: The category: cpu or memory.
        """
        category = category_str[: category_str.find("_")]
        if category not in CATEGORIES:
            raise MetadataException(f"Unknown category: {category}")

        return category
