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
This module contains the components to validate configs.
"""
from abc import ABC
from abc import abstractmethod
from typing import Dict
from typing import NoReturn
from typing import Optional

from ..constant import DEFAULT_PERSIST
from ..constant import DEFAULT_TEST_SIZE
from ..exceptions import ConfigValidationException
from ..logging_utils import get_module_logger

_logger = get_module_logger(__name__)


class ConfigValidator(ABC):
    """
    The base class for configuration validation.

    :param config: The config dictionary for validation.
    """

    def __init__(self, config: Optional[Dict] = None):
        if config is None:
            config = {}
        self.config = config

    @abstractmethod
    def validate(self) -> NoReturn:
        """
        The main entry point to validate configs.

        :return: ``None`` in the base class.
        """
        return NotImplementedError("To be overridden")


class TransformerConfigValidator(ConfigValidator):
    """
    The class to validate a transformer config which holds the config for
    transformations like ``DataTransformer.drop_failed_queries``.

    :param config: The config dictionary for validation.
    """

    #: The fields required in a transformer config.
    REQUIRED_FIELDS = ["transformers"]

    def __init__(self, config: Optional[Dict] = None):
        super().__init__(config)

    def validate(self):
        """
        The main entry point to validate transformer configs. It ensures the
        exist of required fields and fill some optional fields with default
        values if not provided.

        :return: ``None``.
        """
        for field in self.REQUIRED_FIELDS:
            if field not in self.config:
                raise ConfigValidationException(
                    f"{field} is required but not provided"
                )

        if "persist" not in self.config:
            self.config["persist"] = DEFAULT_PERSIST

        _logger.info("Transformer config validation passed")


class TrainerConfigValidator(ConfigValidator):
    """
    The class to validate a trainer config which holds the config for training.

    :param config: The config dictionary for validation.
    """

    #: The fields required in a trainer config.
    REQUIRED_FIELDS = ["label", "feature", "vectorizer", "classifier"]

    def __init__(self, config: Optional[Dict] = None):
        super().__init__(config)

    def validate(self):
        """
        The main entry point to validate trainer configs. It ensures the
        exist of required fields and fill some optional fields with default
        values if not provided.

        :return: ``None``.
        """
        for field in self.REQUIRED_FIELDS:
            if field not in self.config:
                raise ConfigValidationException(
                    f"{field} is required but not provided"
                )

        if "persist" not in self.config["vectorizer"]:
            self.config["vectorizer"]["persist"] = DEFAULT_PERSIST
        if "persist" not in self.config["classifier"]:
            self.config["classifier"]["persist"] = DEFAULT_PERSIST

        if "test_size" not in self.config:
            self.config["test_size"] = DEFAULT_TEST_SIZE

        _logger.info("Trainer config validation passed")
