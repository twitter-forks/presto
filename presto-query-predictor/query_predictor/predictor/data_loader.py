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
This module contains the component to load a Presto log dataset from a file.
"""
from pathlib import Path
from typing import Callable
from typing import Iterator
from typing import Optional

import pandas as pd

from .exceptions import DataLoaderException
from .logging_utils import get_module_logger

_logger = get_module_logger(__name__)


class DataLoader:
    """
    The class to load presto request logs from a file.

    It allows various extra operations such as shuffling, sampling, and customized
    operations on the dataset loaded.

    :param file_path: The string of the path of the dataset.
    :param shuffle: Whether to shuffle the dataset.
    :param sample: Whether to sample the dataset.
    :param sample_size: The number of records of sampled data. It only works
    when ``sample`` is set True.
    :param funcs: The list of extra functions executed on the dataset.
    """

    def __init__(
        self,
        file_path: str,
        shuffle: bool = False,
        sample: bool = False,
        sample_size: int = 1,
        funcs: Iterator[Callable[[pd.DataFrame], pd.DataFrame]] = (),
    ) -> None:
        self.file_path = file_path
        self.shuffle = shuffle
        self.sample = sample
        self.sample_size = sample_size
        self.funcs = funcs
        self.data_frame = None

    def load(self, delimiter: str = ",") -> pd.DataFrame:
        """
        The main function to load a dataset for training from a presto request log.
        It will also execute a series of operations on the datasets if enabled.

        :param delimiter: The delimiter of the dataset. Comma by default.
        :return: A pandas data frame.
        """
        _logger.info("Loading data from %s...", self.file_path)
        if not Path(self.file_path).exists():
            raise DataLoaderException(f"File {self.file_path} not existed")

        self.data_frame = pd.read_csv(self.file_path, delimiter=delimiter)
        _logger.debug(
            "Loaded the dataset with the shape %s", self.data_frame.shape
        )

        if self.shuffle:
            self.data_frame = self._shuffle()
        if self.sample:
            self.data_frame = self._sample()

        for func in self.funcs:
            self.data_frame = func(self.data_frame)

        return self.data_frame

    def save(
        self, file_path: Optional[str] = None, index: bool = False
    ) -> None:
        """
        Saves the dataset to a file. This is helpful when we wants to cache cleaned
        datasets locally for further analysis. Note that it will overwrite the
        file if already existed.

        :param file_path: The string of the path for the saved file.
        :param index: Whether writing row names or not. False by default.
        :return: ``None``
        """
        save_file_path = file_path if file_path is not None else self.file_path
        _logger.info("Saving data to %s...", save_file_path)
        self.data_frame.to_csv(save_file_path, index=index)

    def _shuffle(self) -> pd.DataFrame:
        """
        Shuffle the dataset loaded.

        :return: A pandas data frame.
        """
        return self.data_frame.sample(frac=1)

    def _sample(self) -> pd.DataFrame:
        """
        Sample the dataset loaded without replacement.

        :return: A pandas data frame.
        """
        return self.data_frame.sample(n=self.sample_size, replace=False)
