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
This module contains the component to manager configurations for the machine
learning pipeline.
"""
from pathlib import Path
from typing import Dict
from typing import Optional

import yaml

from ..exceptions import ConfigManagerException
from ..logging_utils import get_module_logger
from .config_validator import ConfigValidator

_logger = get_module_logger(__name__)


class ConfigManager:
    """
    The config manager class helps to manage, e.g. saving and loading configurations.

    Singleton pattern is applied to this class such that there's only one
    ``ConfigManager`` instance.
    """

    #: The singleton instance of the ``ConfigManager``.
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ConfigManager, cls).__new__(cls)
        return cls._instance

    def __init__(self) -> None:
        #: A dictionary to hold the last loaded configurations.
        self.config: Dict = {}

        #: A dictionary to hold all configurations loaded.
        #: config_path (absolute) => config
        self.configs: Dict = {}

    def get_config(self, config_path: str) -> Dict:
        """
        Gets the config dictionary with a config path.

        :param config_path: The path for the config looked for.
        :return: If the path exists in the configs dictionary, the concrete config
        in a dictionary will be returned. Otherwise, it will return ``None``.
        """
        return self.configs.get(Path(config_path).resolve(), None)

    def load_config(self, config_path: str) -> Dict:
        """
        Loads the YAML config from a path. The file content will be parsed to
        YAML in a Python dictionary.

        :param config_path: The path for the config loaded.
        :return: The configuration dictionary.
        :raise ConfigManagerException: If it fails to load the file to YAML.
        :raise FileNotFoundError: If the file does not exist.
        """
        config_path = Path(config_path).resolve()
        if config_path in self.configs:
            return self.configs[config_path]

        _logger.info("Loading config from %s", config_path)
        try:
            with open(config_path, "r") as yaml_file:
                try:
                    parsed_yaml = yaml.load(yaml_file, Loader=yaml.FullLoader)
                    _logger.info("Config loaded: %s", parsed_yaml)
                    self.config = parsed_yaml
                except yaml.YAMLError as err:
                    err_msg = f"Error in loading/parsing {config_path}: {err}"
                    _logger.error(err_msg)
                    raise ConfigManagerException(err_msg)
        except FileNotFoundError as err:
            err_msg = f"Error in reading {config_path}: {err}"
            _logger.error(err_msg)
            raise ConfigManagerException(err_msg)
        self.configs[config_path] = self.config

        return self.config

    def save_config(self, config_path: str) -> None:
        """
        Saves/Dumps the YAML config to a file.

        :param config_path: The target path to save.
        :return: ``None``
        """
        _logger.info("Saving config to %s", config_path)
        with open(config_path, "w") as yaml_file:
            yaml.dump(self.config, yaml_file)
        _logger.info("Config saved: %s", self.config)

    def serialize_yaml(self) -> str:
        """
        Serialize a Python dictionary to YAML format string.

        :return: A string in YAML format
        """
        return yaml.dump(self.config)

    def validate(
        self,
        config_validator: ConfigValidator,
        config_path: Optional[str] = None,
    ) -> None:
        """
        Validates the correctness of the formats of the configuration.

        :param config_validator: A ``ConfigValidator`` instance for validation.
        :param config_path: Path for the configuration. If this parameter is
        provided, the method will first load the configuration.
        :return: ``None``.
        """
        if config_path:
            self.load_config(config_path)

        config_validator.config = self.config
        config_validator.validate()
