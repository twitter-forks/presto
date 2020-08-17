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
This module contains the base class for classification.
"""
from abc import ABC
from abc import abstractmethod
from typing import Dict
from typing import List
from typing import NoReturn
from typing import Optional
from typing import Union

import numpy as np


class Classifier(ABC):
    """
    The base class for classification tasks. An inherited class needs to override
    the abstract methods here. A specific classifier may be from sklearn, pytorch,
    tensorflow, xgboost, etc.

    :param params: The parameters for classifiers.
    """

    def __init__(self, params: Optional[Dict] = None) -> None:
        #: Holds the type string of the classifier.
        self.type = ""

        if params is None:
            params = {}

        #: Parameters for the classifier.
        self.params = params

        #: Holds the specific classifier.
        self.classifier = None

    @abstractmethod
    def init_classifier(self) -> NoReturn:
        """
        Initializes the classifier.

        :return: ``None`` in the base class.
        """
        return NotImplementedError("To be overridden")

    @abstractmethod
    def fit(self, train_data: np.ndarray, train_labels: np.array) -> NoReturn:
        """
        Builds a classification model from the training dataset. After the training,
        the ``self.classifier`` variable holds the model instance.

        :param train_data: Training input samples.
        :param train_labels: Training labels.
        :return: ``None`` in the base class.
        """
        return NotImplementedError("To be overridden")

    @abstractmethod
    def tune(
        self,
        train_data: np.ndarray,
        train_labels: np.ndarray,
        param_grid: Union[Dict[str, List], List[Dict[str, List]]],
    ) -> NoReturn:
        """
        Builds multiple classification models from the training dataset to search
        for the optimal ones.

        :param train_data: Training input samples.
        :param train_labels: Training labels.
        :param param_grid: Dictionary (or a list of such dictionaries) with parameters
        names as keys and lists of parameter settings for model tuning.
        :return: ``None`` in the base class.
        """
        return NotImplementedError("To be overridden")

    @abstractmethod
    def predict(self, test_data: np.ndarray) -> NoReturn:
        """
        Predicts classes of the testing inputs.

        :param test_data: Testing data input samples.
        :return: ``None`` in the base class.
        """
        return NotImplementedError("To be overridden")

    @abstractmethod
    def score(
        self, test_data: np.ndarray, test_labels: np.ndarray
    ) -> NoReturn:
        """
        Gets the mean accuracy of the given test data and labels. Note that accuracy
        may not always be the best metric to evaluate models.

        :param test_data: Testing data input samples.
        :param test_labels: Testing labels.
        :return: ``None`` in the base class.
        """
        return NotImplementedError("To be overridden")

    @abstractmethod
    def report(
        self, test_data: np.ndarray, test_labels: np.ndarray
    ) -> NoReturn:
        """
        Reports additional metrics (confusion matrix, precisions, etc.) of the
        classifier.

        :param test_data: Testing data input samples.
        :param test_labels: Testing labels.
        :return: ``None`` in the base class.
        """
        return NotImplementedError("To be overridden")

    @abstractmethod
    def save(self, path: str) -> NoReturn:
        """
        Saves the trained model.

        :param path: Target saved model path.
        :return: ``None`` in the base class.
        """
        return NotImplementedError("To be overridden")

    @abstractmethod
    def load(self, path: str) -> NoReturn:
        """
        Loads a trained model.

        :param path: Loaded model path.
        :return: ``None`` in the base class.
        """
        return NotImplementedError("To be overridden")
