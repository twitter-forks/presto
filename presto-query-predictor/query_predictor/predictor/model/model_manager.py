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
from abc import ABC
from abc import abstractmethod
from typing import Callable
from typing import Dict
from typing import List
from typing import NoReturn
from typing import Union

from ..classifier import ClassifierFactory
from ..config import ConfigManager
from ..config import ServingConfigValidator
from ..data_vectorizer import VectorizerFactory
from ..exceptions import ModelManagerException
from ..logging_utils import get_module_logger
from ..metadata import Metadata
from ..version import Version
from .ml_model import MLModel
from .ml_model import MLModelFactory
from .vectorizer_model import VectorizerModel
from .vectorizer_model import VectorizerModelFactory

_logger = get_module_logger(__name__)


class ModelManager(ABC):
    """
    The model manager class helps to manage, e.g. loading models.

    Singleton pattern is applied to this class such that there's only one
    ``ModelManager`` instance.
    """

    #: The singleton instace of the ``ModelManager``.
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ModelManager, cls).__new__(cls)
        return cls._instance

    def __init__(self) -> None:
        #: Holds a list of models.
        self.models: List[Union[MLModel, VectorizerModel]] = []

        #: A dictionary to hold the last loaded model configuration.
        self.model_config: Dict = {}

    @abstractmethod
    def load_models(self, model_config_path: str) -> NoReturn:
        """
        Loads models from a config in a path. After the invocation of this
        method, the ``self.models`` variable will hold a list of models.

        :param model_config_path: The path for the config loaded.
        :return: ``None`` in the base class.
        """
        return NotImplementedError("To be overridden")

    def get_models_by_name(self, name: str) -> List:
        """
        Returns models whose names equal to the name provided.

        :param name: The name string to filter.
        :return: A list of models filtered.
        """
        return [model for model in self.models if model.metadata.name == name]

    def get_models_by_version(self, version: str) -> List:
        """
        Returns models whose version equal to the version provided.

        :param version: The version string to filter.
        :return: A list of models filtered.
        """
        return [
            model
            for model in self.models
            if model.metadata.version.version == version
        ]

    def get_models_by_filter(self, filter_func: Callable) -> List:
        """
        Returns models filtered by a function passed in.

        :param filter_func: The filter function.
        :return: A list of models filtered.
        """
        return [model for model in self.models if filter_func(model)]

    def get_models_by_category(self, category: str) -> List:
        """
        Returns models whose category equal to the category provided. The
        category should be a string in the ``constant.CATEGORIES`` list.

        :param category: The category to filter.
        :return: A list of models filtered.
        """
        return [
            model
            for model in self.models
            if model.metadata.category == category
        ]

    def _load_models_helper(
        self,
        model_config_path: str,
        load_func: Callable[[str, Dict], Union[MLModel, VectorizerModel]],
        config_key: str,
    ) -> List:
        """
        A helper function to load models. It uses a function passed-in to load
        models. It parses the config and use the ``config_key`` to identify the
        fields for each model.

        :param model_config_path: The path for the config loaded.
        :param load_func: A function to specify how to load models with a config.
        :param config_key: The key for the model configs.
        :return: A list of models loaded.
        """
        config_manager = ConfigManager()
        self.model_config = config_manager.load_config(model_config_path)
        config_manager.validate(ServingConfigValidator(self.model_config))
        if config_key not in self.model_config:
            raise ModelManagerException(
                f"config key {config_key} does not exist in the config"
            )
        models = self.model_config[config_key]
        model_list = [
            load_func(category, model_config)
            for category, model_config in models.items()
        ]
        self.models += model_list

        _logger.debug(
            "%s models are loaded from config file %s",
            len(model_list),
            model_config_path,
        )

        return model_list


class MLModelManager(ModelManager):
    """
    The class to manager ML models. It is also a singleton class.
    """

    def __new__(cls):
        return super(MLModelManager, cls).__new__(cls)

    def __init__(self) -> None:
        super().__init__()

    def get_models_by_label(self, label) -> List:
        """
        Returns models whose label is equal to the label provided.

        :param label: The label string for prediction. e.g. cpu_time_label
        :return: A list of models filtered.
        """
        return [model for model in self.models if model.label == label]

    def load_models(self, model_config_path: str) -> List[MLModel]:
        """
        Loads models from a config in a path. After the invocation of this
        method, the ``self.models`` variable will hold a list of ML models.

        A sample config of ML models in YAML:
        models:
          cpu_model:
            label: cpu_time_label
            feature: query
            type: XGBoost
            path: models/model-cpu.bin
            name: XGBoost-CPU
            description: An XGBoost model to predict cpu time of each SQL query
            version: 0.1.0
          memory_model:
            label: peak_memory_label
            feature: query
            type: XGBoost
            path: models/model-memory.bin
            name: XGBoost-Memory
            description: An XGBoost model to predict peak memory bytes of each SQL query
            version: 0.1.0

        :param model_config_path: The path for the config loaded.
        :return: A list of ML models.
        """
        _logger.info("Loading ML models from %s...", model_config_path)
        return self._load_models_helper(
            model_config_path=model_config_path,
            load_func=self._load_ml_model,
            config_key="models",
        )

    @staticmethod
    def _load_ml_model(category: str, model_config: Dict) -> MLModel:
        """
        Utility function to load a ML model from a config in a dictionary.
        It calls factories to create classifiers and attach classifiers to
        corresponding models.

        :param category: The category of the model. It should be a string in the
        ``constant.CATEGORIES`` list.
        :param model_config: The configuration for the model.
        :return: A ML model loaded.
        """
        category = Metadata.get_category_from_str(category)
        model_type = model_config["type"]
        classifier = ClassifierFactory.create_classifier(model_type)
        classifier.load(model_config["path"])

        return MLModelFactory.create_ml_model(
            model_type=model_type,
            model=classifier,
            metadata=Metadata(
                name=model_config["name"],
                description=model_config["description"],
                version=Version(model_config["version"]),
                category=category,
            ),
            label=model_config["label"],
        )


class VectorizerModelManager(ModelManager):
    def __new__(cls):
        return super(VectorizerModelManager, cls).__new__(cls)

    def __init__(self) -> None:
        super().__init__()

    def load_models(self, model_config_path: str) -> List[VectorizerModel]:
        """
        Loads models from a config in a path. After the invocation of this
        method, the ``self.models`` variable will hold a list of vectorizer
        models.

        A sample config of vectorizer models in YAML:
        vectorizers:
          cpu_vectorizer:
            feature: query
            type: tfidf
            path: models/vec-cpu.bin
            name: tfidf-cpu
            description: A TF-IDF vectorizer for SQL queries
            version: 0.1.0
          memory_vectorizer:
            feature: query
            type: tfidf
            path: models/vec-memory.bin
            name: tfidf-memory
            description: A TF-IDF vectorizer for SQL queries
            version: 0.1.0

        :param model_config_path: The path for the config loaded.
        :return: A list of vectorizer models.
        """
        _logger.info("Loading vectorizer models from %s", model_config_path)
        return self._load_models_helper(
            model_config_path=model_config_path,
            load_func=self._load_vectorizer_model,
            config_key="vectorizers",
        )

    @staticmethod
    def _load_vectorizer_model(
        category: str, model_config: Dict
    ) -> VectorizerModel:
        """
        Utility function to load a vectorizer model from a config in a
        dictionary. It calls factories to create vectorizers and attach them
        to corresponding models.

        :param category: The category of the model. It should be a string in the
        ``constant.CATEGORIES`` list.
        :param model_config: The configuration for the model.
        :return: A vectorizer model loaded.
        """
        category = Metadata.get_category_from_str(category)
        model_type = model_config["type"]
        vectorizer_factory = VectorizerFactory()
        vectorizer = vectorizer_factory.create_vectorizer(vec_type=model_type)
        vectorizer.load(model_config["path"])

        return VectorizerModelFactory.create_vectorizer_model(
            model_type=model_type,
            model=vectorizer,
            metadata=Metadata(
                name=model_config["name"],
                description=model_config["description"],
                version=Version(model_config["version"]),
                category=category,
            ),
        )
