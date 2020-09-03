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
This module contains the classes for ML models.
"""
from abc import ABC
from abc import abstractmethod
from typing import Dict
from typing import NoReturn

import numpy as np

from ..classifier import Classifier
from ..exceptions import MLModelException
from ..metadata import Metadata


class MLModel(ABC):
    """
    The base class for machine learning models. This will inherited by each
    specific type of models.

    :param model: The classifier used for the ML model.
    :param metadata: The metadata of the model.
    :param label: The prediction label of the model.
    """

    def __init__(
        self, model: Classifier, metadata: Metadata, label: str
    ) -> None:
        self.model = model
        self.metadata = metadata
        self.label = label

    def get_metadata(self) -> Dict:
        """
        Returns the metadata of the model in a dictionary.

        :return: Metadata of the model.
        """
        return {
            "name": self.metadata.name,
            "description": self.metadata.description,
            "label": self.label,
            "category": self.metadata.category,
            "version": self.metadata.version.version,
        }

    @abstractmethod
    def predict(self, data: np.ndarray) -> NoReturn:
        """
        Predicts classes of the testing inputs.

        :param data: Testing data input samples.
        :return: ``None`` in the base class.
        """
        return NotImplementedError("To be overridden")


class SkLearnModel(MLModel):
    """
    The representation of scikit-learn models.
    """

    def __init__(
        self, model: Classifier, metadata: Metadata, label: str
    ) -> None:
        super().__init__(model, metadata, label)

    def predict(self, data: np.ndarray) -> np.ndarray:
        return self.model.predict(data)


class RandomForestModel(SkLearnModel):
    """
    The representation of Random Forest models.
    """

    def __init__(
        self, model: Classifier, metadata: Metadata, label: str
    ) -> None:
        super().__init__(model, metadata, label)


class LogisticRegressionModel(SkLearnModel):
    """
    The representation of Logistic Regression models.
    """

    def __init__(
        self, model: Classifier, metadata: Metadata, label: str
    ) -> None:
        super().__init__(model, metadata, label)


class XGBoostModel(MLModel):
    """
    The representation of XGBoost models
    """

    def __init__(
        self, model: Classifier, metadata: Metadata, label: str
    ) -> None:
        super().__init__(model, metadata, label)

    def predict(self, data: np.ndarray) -> np.ndarray:
        return self.model.predict(data)


class MLModelFactory:
    """
    The factory class to create ML models based on the types passed in.
    """

    def __init__(self) -> None:
        pass

    @staticmethod
    def create_ml_model(
        model_type: str, model: Classifier, metadata: Metadata, label: str
    ) -> MLModel:
        """
        Creates a specific type of ML model.

        :param model_type: The type of the model to create.
        :param model: The classifier stored in the model.
        :param metadata: The metadata for the model.
        :param label: The prediction label of the model.
        :return: A ``MLModel`` instance.
        """
        if model_type == "RandomForest":
            return RandomForestModel(
                model=model, metadata=metadata, label=label
            )
        elif model_type == "LogisticRegression":
            return LogisticRegressionModel(
                model=model, metadata=metadata, label=label
            )
        elif model_type == "XGBoost":
            return XGBoostModel(model=model, metadata=metadata, label=label)
        else:
            raise MLModelException(f"Unknown model type {model_type}")
