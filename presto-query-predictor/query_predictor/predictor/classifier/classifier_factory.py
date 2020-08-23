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
This module contains the factory class to create classifiers.
"""
from typing import Dict
from typing import Optional

from ..exceptions import ClassifierException
from .classifier import Classifier
from .sklearn_classifier import LogisticRegressionClassifier
from .sklearn_classifier import RandomForestClassifier
from .xgboost_classifier import XGBoostClassifier


class ClassifierFactory:
    """
    The factory class to create classifiers.

    Unlike ``VectorizerFactory``, each type here may have several classifier
    instances. This is because there might be various classifiers of the same
    type for multiple training jobs (e.g. for CPU time and peak memory bytes).
    """

    def __init__(self):
        pass

    @staticmethod
    def create_classifier(
        classifier_type: str, params: Optional[Dict] = None
    ) -> Classifier:
        """
        A static methods to create classifiers.

        :param classifier_type: Type of the classifier. e.g. RandomForest
        :param params: Parameters for the classifiers. These will be passed to
        specific classifier created.
        :return: A ``Classifier`` instance.
        :raise ClassifierException: If the type is not recognized.
        """
        if classifier_type == "RandomForest":
            return RandomForestClassifier(params)
        elif classifier_type == "LogisticRegression":
            return LogisticRegressionClassifier(params)
        elif classifier_type == "XGBoost":
            return XGBoostClassifier(params)
        else:
            raise ClassifierException(
                f"Unknown classifier type {classifier_type}"
            )
