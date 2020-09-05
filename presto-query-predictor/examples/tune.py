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
This file contains examples to tune machine learning models.
"""
import pprint
from query_predictor.datasets import load_tpch
from query_predictor.predictor.classifier import RandomForestClassifier
from query_predictor.predictor.data_transformer import DataTransformer
from query_predictor.predictor.data_vectorizer import DataTfidfVectorizer

from sklearn import model_selection

if __name__ == "__main__":
    # The example utilizes some low-level APIs to rune machine learning models,
    # instead of the higher-level APIs like those in the class ``Pipeline``.
    dataset = load_tpch()
    transformer_list = [
        "drop_failed_queries",
        "create_labels",
        "to_lower_queries",
        "select_training_columns",
    ]
    data_transformer = DataTransformer(dataset, transformer_list)
    dataset = data_transformer.transform()

    labels = dataset["cpu_time_label"]
    features = dataset["query"]

    (x_train, x_test, y_train, y_test) = model_selection.train_test_split(
        features, labels, test_size=0.2, stratify=dataset["cpu_time_label"]
    )

    vec_params = {"max_features": 100, "min_df": 1, "max_df": 0.9}
    vectorizer = DataTfidfVectorizer(vec_params)
    x_train = vectorizer.vectorize(x_train)

    param_grid = {"n_estimators": [50, 60], "max_depth": [5, 10]}
    classifier = RandomForestClassifier()
    results = classifier.tune(
        train_data=x_train, train_labels=y_train, param_grid=param_grid
    )

    pp = pprint.PrettyPrinter()
    pp.pprint(results)
