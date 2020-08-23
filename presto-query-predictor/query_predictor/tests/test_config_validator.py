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
from query_predictor.predictor.config.config_validator import (
    TrainerConfigValidator,
)
from query_predictor.predictor.config.config_validator import (
    TransformerConfigValidator,
)
from query_predictor.predictor.exceptions import ConfigValidationException

import pytest


def test_transformer_config_validator(example_transformer_config):
    config_validator = TransformerConfigValidator(example_transformer_config)
    config_validator.validate()
    config = config_validator.config

    assert (
        "transformers" in config
    ), "transformers should be in the config fields"
    assert "persist" in config, "persist should be in the config fields"
    assert not config["persist"], "default persist should be False"


def test_error_transformer_config_validator(error_transformer_config):
    config_validator = TransformerConfigValidator(error_transformer_config)
    with pytest.raises(ConfigValidationException) as err:
        config_validator.validate()

    assert (
        "required but not provided" in err.value.args[0]
    ), "exceptions of required fields are expected"


def test_trainer_config_validator(example_trainer_config):
    config_validator = TrainerConfigValidator(example_trainer_config)
    config_validator.validate()
    config = config_validator.config

    for field in config_validator.REQUIRED_FIELDS:
        assert field in config, f"{field} should be in the config fields"
    assert not config["classifier"][
        "persist"
    ], "default classifier persist should be False"
    assert config["test_size"] == 0.2, "default test_size should be 0.2"


def test_error_trainer_config_validator(error_trainer_config):
    config_validator = TrainerConfigValidator(error_trainer_config)
    with pytest.raises(ConfigValidationException) as err:
        config_validator.validate()

    assert (
        "required but not provided" in err.value.args[0]
    ), "exceptions of required fields are expected"
