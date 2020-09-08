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
from query_predictor.predictor.assembler import ClassifierAssembler
from query_predictor.predictor.assembler import TransformerAssembler
from query_predictor.predictor.assembler import VectorizerAssembler
from query_predictor.predictor.classifier import XGBoostClassifier
from query_predictor.predictor.data_transformer import DataTransformer
from query_predictor.predictor.data_vectorizer import DataVectorizer


def test_transformer_assembler(example_transformer_config):
    assembler = TransformerAssembler(example_transformer_config)
    data_transformer = assembler.assemble(example_transformer_config)

    assert (
        not assembler.persist_required
    ), "The default persist_required should be False"
    assert isinstance(
        data_transformer, DataTransformer
    ), "A DataTransformer instance should be returned"
    assert (
        data_transformer.func_strs
        == example_transformer_config["transformers"]
    ), "The transformer list should be the same as the config passed"


def test_vectorizer_assembler(example_trainer_config):
    assembler = VectorizerAssembler(example_trainer_config)
    data_vectorizer = assembler.assemble()

    assert assembler.persist_required, "The persist_required should be True"
    assert isinstance(
        data_vectorizer, DataVectorizer
    ), "A DataVectorizer instance should be returned"
    assert (
        data_vectorizer.type == "tfidf"
    ), "The data vectorizer type should be tfidf"
    assert (
        data_vectorizer.params
        == example_trainer_config["vectorizer"]["params"]
    ), "The vectorizer params should be the same as the config passed"


def test_classifier_assembler(example_trainer_config):
    assembler = ClassifierAssembler(example_trainer_config)
    classifier = assembler.assemble()

    assert (
        not assembler.persist_required
    ), "The default persist_required should be False"
    assert isinstance(
        classifier, XGBoostClassifier
    ), "A XGBoost classifier should be returned"
    assert (
        classifier.params == example_trainer_config["classifier"]["params"]
    ), "The classifier params should be the same as the config passed"
