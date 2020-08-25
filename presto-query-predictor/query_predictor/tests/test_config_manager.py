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
from query_predictor.predictor.config import ConfigManager
from query_predictor.predictor.config import TransformerConfigValidator


def test_config_manager(create_transformer_tmp_config):
    config_manager = ConfigManager()
    config = config_manager.load_config(create_transformer_tmp_config)

    assert isinstance(config, dict), "Config loaded should be a dictionary"

    config_manager.validate(TransformerConfigValidator())

    yaml_str = config_manager.serialize_yaml()

    assert isinstance(yaml_str, str), "Config serialized should be a str"
