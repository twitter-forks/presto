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
This module contains the classes to store vectorizers.
"""
from abc import ABC
from abc import abstractmethod
from typing import Dict
from typing import NoReturn

import numpy as np

from ..data_vectorizer import DataVectorizer
from ..exceptions import VectorizerModelException
from ..metadata import Metadata


class VectorizerModel(ABC):
    """
    The base class for vectorizer models. This will be inherited by each specific
    type of vectorizer.

    :param model: The vectorizer stored in the model.
    :param metadata: The metadata for the model.
    """

    def __init__(self, model: DataVectorizer, metadata: Metadata) -> None:
        self.model = model
        self.metadata = metadata

    def get_metadata(self) -> Dict:
        """
        Returns the metadata of the model in a dictionary.

        :return: Metadata of the model.
        """
        return {
            "name": self.metadata.name,
            "description": self.metadata.description,
            "category": self.metadata.category,
            "version": self.metadata.version.version,
        }

    @abstractmethod
    def transform(self, data: np.ndarray) -> NoReturn:
        """
        Entry point to transform data with the trained vectorizer.

        :param data: The target data for transformation.
        :return: ``None`` here in the base class.
        """
        return NotImplementedError("To be overridden")


class TfidfModel(VectorizerModel):
    """
    The representation of TF-IDF models.
    """

    def __init__(self, model: DataVectorizer, metadata: Metadata) -> None:
        super().__init__(model, metadata)

    def transform(self, data: np.ndarray) -> np.ndarray:
        return self.model.transform(data)


class CountModel(VectorizerModel):
    """
    The representation of CountVectorizer models.
    """

    def __init__(self, model: DataVectorizer, metadata: Metadata) -> None:
        super().__init__(model, metadata)

    def transform(self, data: np.ndarray) -> np.ndarray:
        return self.model.transform(data)


class VectorizerModelFactory:
    """
    The factory class to create vectorizer models based on the types passed in.
    """

    def __init__(self) -> None:
        pass

    @staticmethod
    def create_vectorizer_model(
        model_type: str, model: DataVectorizer, metadata: Metadata
    ) -> VectorizerModel:
        """
        Creates a specific type of vectorizer model.

        :param model_type: The type of the vectorizer model to create.
        :param model: The vectorizer stored in the model.
        :param metadata: The metadata for the model.
        :return: A ``VectorizerModel`` instance.
        """
        if model_type == "count":
            return CountModel(model, metadata)
        elif model_type == "tfidf":
            return TfidfModel(model, metadata)
        else:
            raise VectorizerModelException(f"Unknown model type {model_type}")
