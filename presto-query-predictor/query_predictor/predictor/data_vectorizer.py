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
This module contains components to vectorize data.
"""
from abc import ABC
from abc import abstractmethod
from pathlib import Path
from typing import Dict
from typing import NoReturn
from typing import Optional
from typing import Union

import joblib
import numpy as np

from .exceptions import DataVectorizationException
from .logging_utils import get_module_logger

_logger = get_module_logger(__name__)


class DataVectorizer(ABC):
    """
    The class to vectorize data, aka converting string statements to vectors of
    numbers for training.

    :param params: The parameters for vectorization.
    """

    def __init__(self, params: Optional[Dict] = None):
        #: Holds the type string of the vectorizer.
        self.type = ""

        if params is None:
            params = {}
        #: The dictionary to hold parameters for vectorization.
        self.params = params

        #: The concrete vectorizer.
        self.vectorizer = None

        #: The vector of numbers after the vectorization.
        self.vectors = None

        #: Holds the data for vectorization
        self.data = None

    @abstractmethod
    def vectorize(self, data: np.ndarray) -> Union[NoReturn, np.ndarray]:
        """
        Entry point to vectorize data.

        :param data: The target data for vectorization.
        :return: ``None`` here in the base class. The vector of numbers after the
        vectorization is returned in the inherited classes.
        """
        return NotImplementedError("To be overridden")

    @abstractmethod
    def transform(self, data: np.ndarray) -> Union[NoReturn, np.ndarray]:
        """
        Entry point to transform data with the trained vectorizer.

        :param data: The target data for transformation.
        :return: ``None`` here in the base class. The vector of numbers after the
        transformation is returned in the inherited classes.
        """
        return NotImplementedError("To be overridden")

    def save(self, path: str) -> None:
        """
        Saves the trained vectorizer.

        :param path: Target saved vectorizer path.
        :return: ``None``
        :raise DataVectorizationException: If the vectorizer hasn't been created.
        """
        if self.vectorizer is None:
            raise DataVectorizationException("Vectorizer does not exist")

        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)

        joblib.dump(self.vectorizer, path)

    def load(self, path: str):
        """
        Loads a trained vectorizer.

        :param path: Loaded vectorizer path.
        :return:  A vectorizer.
        :raise DataVectorizationException: If the vectorizer path does not exist.
        """
        if not Path(path).exists():
            raise DataVectorizationException(f"File {path} does not exist")

        self.vectorizer = joblib.load(path)
        return self.vectorizer


class DataCountVectorizer(DataVectorizer):
    """
    The class to vectorize data with a CountVectorizer. It applies a token count
    approach.
    """

    def __init__(self, params: Optional[Dict] = None) -> None:
        super().__init__(params)
        self.type = "count"

    def vectorize(self, data: np.ndarray) -> np.ndarray:
        from sklearn.feature_extraction.text import CountVectorizer

        _logger.info("Vectorizing data with a count vectorizer...")
        vectorizer = CountVectorizer(**self.params)
        self.data = data
        if not self.vectorizer:
            self.vectorizer = vectorizer.fit(self.data)
        self.vectors = self.vectorizer.transform(self.data)

        return self.vectors

    def transform(self, data: np.ndarray) -> np.ndarray:
        return self.vectorizer.transform(data)


class DataTfidfVectorizer(DataVectorizer):
    """
    The class to vectorize data with a TfidfVectorizer. It applies a TF-IDF
    (term frequency-inverse document frequency) approach.
    """

    def __init__(self, params: Optional[Dict] = None) -> None:
        super().__init__(params)
        self.type = "tfidf"

    def vectorize(self, data: np.ndarray) -> np.ndarray:
        from sklearn.feature_extraction.text import TfidfVectorizer

        _logger.info("Vectorizing data with a TF-IDF vectorizer...")
        vectorizer = TfidfVectorizer(**self.params)
        self.data = data
        if not self.vectorizer:
            self.vectorizer = vectorizer.fit(self.data)
        self.vectors = self.vectorizer.transform(self.data)

        return self.vectors

    def transform(self, data: np.ndarray) -> np.ndarray:
        return self.vectorizer.transform(data)


class VectorizerFactory:
    """
    The factory class to create vectorizers.

    Each type of vectorizers can only have one instance (singleton), controlled
    by a dictionary of vectorizers.
    """

    def __init__(self):
        #: Holds a dictionary of vectoriziers, {type => vectorizer instance}
        self.vectorizers = {}

    def create_vectorizer(
        self, vec_type: str, params: Optional[Dict] = None
    ) -> DataVectorizer:
        """
        Creates a vectorizer with a type.

        :param vec_type: Type of the vectorizer. e.g. count
        :param params: Parameters for the vectorizer. These will be passed to
        specific vectorizer created.
        :return: A ``DataVectorizer`` instance.
        :raise DataVectorizationException: If the type is not recognized.
        """
        if vec_type in self.vectorizers:
            return self.vectorizers[vec_type]

        if vec_type == "count":
            self.vectorizers[vec_type] = DataCountVectorizer(params)
        elif vec_type == "tfidf":
            self.vectorizers[vec_type] = DataTfidfVectorizer(params)
        else:
            raise DataVectorizationException(
                f"Unknown vectorizer type {vec_type}"
            )

        return self.vectorizers[vec_type]
