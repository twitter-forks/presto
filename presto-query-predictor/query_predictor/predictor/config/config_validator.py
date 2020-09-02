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

    def __init__(self, config: Optional[Dict] = None) -> None:
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

    @staticmethod
    def validate_persist(config: Dict) -> Dict:
        """
        Validate the ``persist`` and ``persist_path`` fields. If ``persist`` is
        not in the config, it will be set to the default value. If ``persist`` is
        True, a ``persist_path`` must be provided.

        :param config: The config dictionary for validation.
        :return: The config dictionary after validation.
        :raise ConfigValidationException: If ``persist`` is True, but ``persist_path``
        is not provided.
        """
        if "persist" not in config:
            config["persist"] = DEFAULT_PERSIST

        if config["persist"] and "persist_path" not in config:
            raise ConfigValidationException(
                "A persist path is required when persist is set true"
            )
        return config


class TransformerConfigValidator(ConfigValidator):
    """
    The class to validate a transformer config which holds the config for
    transformations like ``DataTransformer.drop_failed_queries``.

    :param config: The config dictionary for validation.
    """

    #: The fields required in a transformer config.
    REQUIRED_FIELDS = ["transformers"]

    def __init__(self, config: Optional[Dict] = None) -> None:
        super().__init__(config)

    def validate(self) -> Dict:
        """
        The main entry point to validate transformer configs. It ensures the
        exist of required fields and fill some optional fields with default
        values if not provided.

        :return: A dictionary of configs after the validation.
        :raise ConfigValidationException: If a required field is not provided.
        """
        for field in self.REQUIRED_FIELDS:
            if field not in self.config:
                raise ConfigValidationException(
                    f"{field} is required but not provided"
                )

        self.validate_persist(self.config)

        _logger.info("Transformer config validation passed")
        return self.config


class TrainerConfigValidator(ConfigValidator):
    """
    The class to validate a trainer config which holds the config for training.

    :param config: The config dictionary for validation.
    """

    #: The fields required in a trainer config.
    REQUIRED_FIELDS = ["label", "feature", "vectorizer", "classifier"]

    def __init__(self, config: Optional[Dict] = None) -> None:
        super().__init__(config)

    def validate(self) -> Dict:
        """
        The main entry point to validate trainer configs. It ensures the
        exist of required fields and fill some optional fields with default
        values if not provided.

        :return: A dictionary of configs after the validation.
        :raise ConfigValidationException: If a required field is not provided.
        """
        for field in self.REQUIRED_FIELDS:
            if field not in self.config:
                raise ConfigValidationException(
                    f"{field} is required but not provided"
                )

        self.validate_persist(self.config["vectorizer"])
        self.validate_persist(self.config["classifier"])

        if "test_size" not in self.config:
            self.config["test_size"] = DEFAULT_TEST_SIZE

        _logger.info("Trainer config validation passed")
        return self.config


class ServingConfigValidator(ConfigValidator):
    """
    The class to validate a serving config which holds the config for model
    serving.

    :param config: The config dictionary for validation.
    """

    #: The fields required in a serving config.
    REQUIRED_FIELDS = ["models", "vectorizers"]

    #: The fields required in a model values.
    MODEL_REQUIRED_FIELDS = [
        "label",
        "feature",
        "type",
        "path",
        "name",
        "description",
        "version",
    ]

    #: The fields required in a vectorizer fields.
    VECTORIZER_REQUIRED_FIELDS = [
        "feature",
        "type",
        "path",
        "name",
        "description",
        "version",
    ]

    def __init__(self, config: Optional[Dict] = None) -> None:
        super().__init__(config)

    def validate(self) -> Dict:
        """
        The main entry point to validate serving configs. It ensures the
        exist of required fields.

        :return: A dictionary of configs after the validation.
        :raise ConfigValidationException: If a required field is not provided.
        """
        for field in self.REQUIRED_FIELDS:
            if field not in self.config:
                raise ConfigValidationException(
                    f"{field} is required but not provided"
                )

        models = self.config["models"]
        for config in models.values():
            for field in self.MODEL_REQUIRED_FIELDS:
                if field not in config:
                    raise ConfigValidationException(
                        f"{field} is required for a classifier but not provided"
                    )

        vectorizers = self.config["vectorizers"]
        for config in vectorizers.values():
            for field in self.VECTORIZER_REQUIRED_FIELDS:
                if field not in config:
                    raise ConfigValidationException(
                        f"{field} is required for a vectorizer but not provided"
                    )

        _logger.info("Serving config validation passed")
        return self.config
