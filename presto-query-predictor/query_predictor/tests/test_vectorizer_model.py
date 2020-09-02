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
from query_predictor.predictor.data_vectorizer import VectorizerFactory
from query_predictor.predictor.metadata import Metadata
from query_predictor.predictor.model import TfidfModel
from query_predictor.predictor.model import VectorizerModel
from query_predictor.predictor.model import VectorizerModelFactory
from query_predictor.predictor.version import Version


def test_vectorizer_model_factory():
    factory = VectorizerFactory()
    vectorizer = factory.create_vectorizer("tfidf")
    model = VectorizerModelFactory.create_vectorizer_model(
        model_type="tfidf",
        model=vectorizer,
        metadata=Metadata(
            name="temp_name",
            description="model_description",
            version=Version("1.0.1"),
            category=CPU_CATEGORY,
        ),
    )
    metadata = model.get_metadata()

    assert isinstance(
        model, VectorizerModel
    ), "The model created should be a VectorizerModel instance"
    assert isinstance(
        model, TfidfModel
    ), "The model created should be a TfidfModel instance"
    assert isinstance(metadata, dict), "The model's metadata is a dictionary"
    assert all(
        key in metadata
        for key in ["name", "description", "category", "version"]
    ), "The metadata should have keys: name, description, category, and version"
