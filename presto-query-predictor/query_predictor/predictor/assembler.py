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
This module contains the classes for assembling components for a machine
learning pipeline.
"""
from abc import ABC
from abc import abstractmethod
from typing import Dict
from typing import NoReturn

import pandas as pd

from .classifier.classifier import Classifier
from .classifier.classifier_factory import ClassifierFactory
from .data_transformer import DataTransformer
from .data_vectorizer import DataVectorizer
from .data_vectorizer import VectorizerFactory
from .exceptions import AssemblerException


class Assembler(ABC):
    """
    The base class to assembly a machine learning component.

    :param config: The dictionary of config.
    """

    def __init__(self, config: Dict) -> None:
        self.config = config

        #: Whether a persist of the component is required.
        self.persist_required = False

        #: The path for persist if required.
        self.persist_path = None

    @abstractmethod
    def assemble(self) -> NoReturn:
        """
        The main entrance to assembly a machine learning component.

        :return: ``None`` in the base class.
        """
        return NotImplementedError("To be overridden")

    def _set_persist(self, config: Dict) -> None:
        """
        Validate the ``persist`` field of a config. It will set the ``persist_required``
        and ``persist_path`` variables based on specific configs.

        :param config: A dictionary of config.
        :return: ``None``.
        :raise AssemblerException: If ``persist`` is True but ``persist_path`` is
        not provided.
        """
        if "persist" in config and config["persist"]:
            self.persist_required = True
            if "persist_path" not in config:
                raise AssemblerException(
                    "A persist_path is required when persist is set to be true"
                )
            self.persist_path = config["persist_path"]


class TransformerAssembler(Assembler):
    """
    The class to assembly transformers.
    """

    def __init__(self, config) -> None:
        super().__init__(config)

        #: Holds the data frame to transform.
        self.data_frame = None

    def assemble(self, data_frame: pd.DataFrame = None) -> DataTransformer:
        """
        Assembles a list of transformers based on the passed-in config.

        :param data_frame: The data frame for transformations.
        :return: A ``DataTransformer`` instance.
        """
        if data_frame is not None:
            self.data_frame = data_frame

        self._set_persist(self.config)

        transformers = self.config["transformers"]
        transformer_list = [transformer for transformer in transformers]

        return DataTransformer(self.data_frame, transformer_list)


class VectorizerAssembler(Assembler):
    """
    The class to assembly vectorizers.
    """

    def __init__(self, config) -> None:
        super().__init__(config)

    def assemble(self) -> DataVectorizer:
        """
        Assembles a vectorizer based on the passed-in config. It uses
        ``VectorizerFactory`` to create a ``DataVectorizer`` instance.

        :return: A ``DataVectorizer`` instance.
        """
        self._validate_vectorizer_fields()
        config = self.config["vectorizer"]
        self._set_persist(config)

        vec_type = config["type"]
        params = config["params"]

        factory = VectorizerFactory()
        return factory.create_vectorizer(vec_type, params)

    def _validate_vectorizer_fields(self) -> None:
        """
        Validates the ``vectorizer`` field in the config.

        :return: ``None``
        :raise AssemblerException: If the ``vectorizer`` field does not exist.
        """
        if "vectorizer" not in self.config:
            raise AssemblerException(
                "vectorizer field is required but not provided"
            )


class ClassifierAssembler(Assembler):
    """
    The class to assembly classifiers.
    """

    def __init__(self, config) -> None:
        super().__init__(config)

    def assemble(self) -> Classifier:
        """
        Assembles a classifier based on the passed-in config. It uses ``ClassifierFactory``
        to create classifiers.

        :return: A ``Classifier`` instance.
        """
        self._validate_classifier_field()
        config = self.config["classifier"]
        self._set_persist(config)

        classifier_type = config["type"]
        params = config["params"]

        factory = ClassifierFactory()
        return factory.create_classifier(classifier_type, params)

    def _validate_classifier_field(self) -> None:
        """
        Validates the ``classifier`` field in the config.

        :return: ``None``
        :raise AssemblerException: If the ``classifier`` field does not exist.
        """
        if "classifier" not in self.config:
            raise AssemblerException(
                "classifier field is required but not provided"
            )
