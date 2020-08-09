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
import logging
from abc import ABC
from abc import abstractmethod
from logging.config import fileConfig
from typing import Dict
from typing import NoReturn
from typing import Optional
from typing import Union

import numpy as np

fileConfig("../conf/logging.conf", disable_existing_loggers=False)
_logger = logging.getLogger(__name__)


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
    def vectorize(self, data: np.array) -> Union[NoReturn, np.array]:
        """
        Entry point to vectorize data.

        :param data: The target data for vectorization.
        :return: ``None`` here in the base class. The vector of numbers after the
        vectorization is returned in the inherited classes.
        """
        return NotImplementedError("To be overridden")

    @abstractmethod
    def transform(self, data: np.array) -> Union[NoReturn, np.array]:
        """
        Entry point to transform data with the trained vectorizer.

        :param data: The target data for transformation.
        :return: ``None`` here in the base class. The vector of numbers after the
        transformation is returned in the inherited classes.
        """
        return NotImplementedError("To be overridden")


class DataCountVectorizer(DataVectorizer):
    """
    The class to vectorize data with a CountVectorizer. It applies a token count
    approach.
    """

    def __init__(self, params):
        super().__init__(params)
        self.type = "count"

    def vectorize(self, data: np.array) -> np.array:
        from sklearn.feature_extraction.text import CountVectorizer

        vectorizer = CountVectorizer(**self.params)
        if not self.vectorizer:
            self.vectorizer = vectorizer.fit(self.data)
        self.vectors = self.vectorizer.transform(self.data)

        return self.vectors

    def transform(self, data: np.array) -> np.array:
        return self.vectorizer.transform(data)


class DataTfidfVectorizer(DataVectorizer):
    """
    The class to vectorize data with a TfidfVectorizer. It applies a TF-IDF
    (term frequency-inverse document frequency) approach.
    """

    def __init__(self, params):
        super().__init__(params)
        self.type = "tfidf"

    def vectorize(self, data: np.array) -> np.array:
        from sklearn.feature_extraction.text import TfidfVectorizer

        vectorizer = TfidfVectorizer(**self.params)
        if not self.vectorizer:
            self.vectorizer = vectorizer.fit(self.data)
        self.vectors = self.vectorizer.transform(self.data)

        return self.vectors

    def transform(self, data: np.array) -> np.array:
        return self.vectorizer.transform(data)
