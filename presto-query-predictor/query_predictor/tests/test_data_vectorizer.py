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
from query_predictor.predictor.data_vectorizer import DataCountVectorizer
from query_predictor.predictor.data_vectorizer import DataTfidfVectorizer

import pytest

params = {"max_features": 10, "min_df": 1, "max_df": 0.9}
data = [
    "select * from tpch.orders limit 10",
    "select count(*) as count_order from tpch.lineitem",
]


def test_count_vectorizer():
    vectorizer = DataCountVectorizer(params)
    vectorized_data = vectorizer.vectorize(data)

    assert vectorized_data is not None, "vectorized data should not be None"
    assert vectorized_data.shape[0] == len(
        data
    ), "vectorized data should be in the same shape with input data"


def test_count_vectorizer_empty():
    empty_data = []
    vectorizer = DataCountVectorizer(params)

    with pytest.raises(ValueError) as e:
        vectorizer.vectorize(empty_data)
    assert "empty vocabulary" in str(e.value)


def test_tfidf_vectorizer():
    vectorizer = DataTfidfVectorizer(params)
    vectorized_data = vectorizer.vectorize(data)

    assert vectorized_data is not None, "vectorized data should not be None"
    assert vectorized_data.shape[0] == len(
        data
    ), "vectorized data should be in the same shape with input data"


def test_tfidf_vectorizer_empty():
    empty_data = []
    vectorizer = DataTfidfVectorizer(params)

    with pytest.raises(ValueError) as e:
        vectorizer.vectorize(empty_data)
    assert "empty vocabulary" in str(e.value)
