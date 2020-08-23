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
This module contains the utility functions for logging.
"""
import logging
from logging.config import fileConfig
from pathlib import Path


log_conf_path = Path(__file__).parent.parent / "conf/logging.conf"
fileConfig(log_conf_path, disable_existing_loggers=False)


def get_module_logger(module_name: str) -> logging.Logger:
    """
    Gets a logger with the name provided.

    :param module_name: The name of the module used for logging.
    :return: A Logger instance.
    """
    return logging.getLogger(module_name)
