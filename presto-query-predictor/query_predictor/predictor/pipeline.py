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
This module contains the class to create a machine learning pipeline.
"""
from __future__ import annotations

import pandas as pd
from sklearn import model_selection

from .assembler import ClassifierAssembler
from .assembler import TransformerAssembler
from .assembler import VectorizerAssembler
from .config import ConfigManager
from .config import TrainerConfigValidator
from .config import TransformerConfigValidator
from .data_loader import DataLoader


class Pipeline:
    """
    The class to create a machine learning pipeline, including data loading,
    transformation, vectorization, training, and evaluation.

    Each operator in the class returns ``self`` to allow chaining of these
    operators.

    :param transformer_config_path: Path for the transformation config.
    :param trainer_config_path: Path for the trainer config.
    :param data_path: Path for the data loaded. This parameter will be ignored
    if ``data_frame`` is provided.
    :param data_frame: Data frame to handle.
    :param transformation_required: Whether transformation is required.

    To use:
    >>> from query_predictor.datasets import load_tpch
    >>>
    >>> data_frame = load_tpch()
    >>> transformer_config_path = "../conf/transformer.yaml"
    >>> trainer_config_path = "../conf/trainer-cpu.yaml"
    >>> pipeline = Pipeline(
    >>>     data_frame=data_frame,
    >>>     transformer_config_path=transformer_config_path,
    >>>     trainer_config_path=trainer_config_path,
    >>> )
    >>> pipeline.exec()
    """

    def __init__(
        self,
        transformer_config_path: str,
        trainer_config_path: str,
        data_path: str = None,
        data_frame: pd.DataFrame = None,
        transformation_required: bool = True,
    ) -> None:
        self.transformer_config_path = transformer_config_path
        self.trainer_config_path = trainer_config_path
        self.data_path = data_path
        self.data_frame = data_frame
        self.transformation_required = transformation_required

        #: Holds the config dictionary for transformers.
        self.transformer_config = None

        #: Holds the config dictionary for vectorizers and classifiers.
        self.trainer_config = None

        #: Type of the classifier. e.g. RandomForest
        self.classifier_type = None

        #: Holds the specific classifier instance.
        self.classifier = None

        #: Type of the vectorizer. e.g. tfidf
        self.vec_type = None

        #: Holds the specific vectorizer instance.
        self.vectorizer = None

        #: String for the predicted label.
        self.pred_label = None

        #: Training input samples.
        self.x_train = None

        #: Testing input samples.
        self.x_test = None

        #: Training labels.
        self.y_train = None

        #: Testing labels.
        self.y_test = None

        #: Holds the dictionary of evaluation information.
        self.report = None

    def exec(self) -> Pipeline:
        """
        Executes the components on the machine learning pipeline one by one.

        :return: ``self``
        """
        if self.data_frame is None:
            self.load_data()
        self.load_config()
        if self.transformation_required:
            self.transform()
        return self.split().vectorize().train().eval()

    def load_data(self) -> Pipeline:
        """
        Loads the data frame to handle.

        :return: ``self``
        """
        self.data_frame = DataLoader(file_path=self.data_path).load()
        return self

    def load_config(self) -> Pipeline:
        """
        Loads the transformer and trainer configs.

        :return: ``self``
        """
        config_manager = ConfigManager()
        self.transformer_config = config_manager.load_config(
            self.transformer_config_path
        )
        config_manager.validate(
            TransformerConfigValidator(self.transformer_config)
        )

        self.trainer_config = config_manager.load_config(
            self.trainer_config_path
        )
        config_manager.validate(TrainerConfigValidator(self.trainer_config))

        return self

    def transform(self) -> Pipeline:
        """
        Executes transformers on the target data frame.

        :return: ``self``
        """
        assembler = TransformerAssembler(self.transformer_config)
        data_transformer = assembler.assemble(self.data_frame)
        self.data_frame = data_transformer.transform()

        if assembler.persist_required:
            data_transformer.save(assembler.persist_path)
        return self

    def split(self) -> Pipeline:
        """
        Splits the data frame into a training dataset and a testing dataset.

        :return: ``self``
        """
        self.pred_label = self.trainer_config["label"]
        labels = self.data_frame[self.pred_label]
        feature = self.trainer_config["feature"]
        features = self.data_frame[feature]

        test_size = self.trainer_config["test_size"]
        (
            self.x_train,
            self.x_test,
            self.y_train,
            self.y_test,
        ) = model_selection.train_test_split(
            features,
            labels,
            test_size=test_size,
            stratify=self.data_frame[self.pred_label],
        )
        return self

    def vectorize(self) -> Pipeline:
        """
        Vectorizes the input samples.

        :return: ``self``
        """
        assembler = VectorizerAssembler(self.trainer_config)
        self.vectorizer = assembler.assemble()
        self.x_train = self.vectorizer.vectorize(self.x_train)

        if assembler.persist_required:
            self.vectorizer.save(assembler.persist_path)
        return self

    def train(self) -> Pipeline:
        """
        Model training from the input samples.

        :return: ``self``
        """
        assembler = ClassifierAssembler(self.trainer_config)
        self.classifier = assembler.assemble()
        self.classifier.fit(self.x_train, self.y_train)

        if assembler.persist_required:
            self.classifier.save(assembler.persist_path)
        return self

    def eval(self) -> Pipeline:
        """
        Evaluates the trained model against the testing dataset.

        :return: ``self``
        """
        self.x_test = self.vectorizer.transform(self.x_test)
        self.report = self.classifier.report(self.x_test, self.y_test)
        return self
