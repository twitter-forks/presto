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
import pytest


@pytest.fixture()
def example_transformer_config():
    return {
        "transformers": [
            "drop_failed_queries",
            "create_labels",
            "to_lower_queries",
            "select_training_columns",
        ]
    }


@pytest.fixture()
def error_transformer_config():
    return {}


@pytest.fixture()
def example_trainer_config():
    return {
        "label": "cpu_time_label",
        "feature": "query",
        "vectorizer": {
            "type": "tfidf",
            "params": {"max_features": 100},
            "persist": True,
            "persist_path": "vec.bin",
        },
        "classifier": {"type": "XGBoost", "params": {"max_depth": 2}},
    }


@pytest.fixture()
def error_trainer_config():
    return {
        "label": "cpu_time_label",
        "feature": "query",
        "vectorizer": {"type": "tfidf"},
    }


@pytest.fixture()
def create_tmp_dir(tmp_path):
    config_tmp_folder = tmp_path / "conf"
    config_tmp_folder.mkdir()

    return config_tmp_folder.absolute()


@pytest.fixture()
def create_transformer_tmp_config(create_tmp_dir, example_transformer_config):
    config_tmp_path = create_tmp_dir / "transformer.yaml"
    config_tmp_path.write_text(str(example_transformer_config))

    return config_tmp_path


@pytest.fixture()
def create_trainer_tmp_config(create_tmp_dir, example_trainer_config):
    config_tmp_path = create_tmp_dir / "trainer-cpu.yaml"
    config_tmp_path.write_text(str(example_trainer_config))

    return config_tmp_path
