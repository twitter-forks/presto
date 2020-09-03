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
from query_predictor.predictor.classifier import ClassifierFactory
from query_predictor.predictor.constant import CPU_CATEGORY
from query_predictor.predictor.metadata import Metadata
from query_predictor.predictor.model import MLModel
from query_predictor.predictor.model import MLModelFactory
from query_predictor.predictor.model import RandomForestModel
from query_predictor.predictor.model import SkLearnModel
from query_predictor.predictor.version import Version


def test_ml_model_factory():
    classifier = ClassifierFactory.create_classifier("RandomForest")
    model = MLModelFactory.create_ml_model(
        model_type="RandomForest",
        model=classifier,
        metadata=Metadata(
            name="temp_name",
            description="model_description",
            version=Version("1.0.1"),
            category=CPU_CATEGORY,
        ),
        label="cpu_time_ms",
    )

    assert isinstance(
        model, MLModel
    ), "The model created should be an MLModel instance"
    assert isinstance(
        model, SkLearnModel
    ), "The model created should be a SkLearnModel instance"
    assert isinstance(
        model, RandomForestModel
    ), "The model created should be a RandomForestModel instance"
