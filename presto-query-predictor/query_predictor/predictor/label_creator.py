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
This module contains the components to create labels for training/testing.
"""
from abc import ABC
from abc import abstractmethod
from typing import NoReturn
from typing import Union

import pandas as pd

from .constant import CPU_TIME_COLUMN
from .constant import FIVE_HOUR
from .constant import ONE_HOUR
from .constant import ONE_MB
from .constant import ONE_TB
from .constant import PEAK_MEMORY_COLUMN
from .constant import THIRTY_SECOND
from .exceptions import LabelCreatorException


class LabelCreator(ABC):
    """
    The class to create a labeled column.
    """

    def __init__(self) -> None:
        pass

    @abstractmethod
    def label(self, row: pd.Series) -> Union[NoReturn, int]:
        """
        Create a label from a data row.

        :param row: A row of a data frame.
        :return: ``None`` here in the base class. The integer representing the
        category in the inherited classes.
        """
        return NotImplementedError("To be overridden")

    @abstractmethod
    def label_to_str(self, index) -> Union[NoReturn, str]:
        """
        Convert a label to the representation in a string.

        :param index: The index of the metric.
        :return: ``None`` here in the base class. The string name representing
        the category in the inherited classes.
        """
        return NotImplementedError("To be overridden")


class CPUTimeLabelCreator(LabelCreator):
    """
    The class to create a labeled CPU time column.

    This is based on the predefined categories of CPU time, e.g. < 5h and > 5h.
    It converts a continuous CPU time to a category.
    """

    def __init__(self) -> None:
        super().__init__()

    def label(self, row: pd.Series) -> int:
        if CPU_TIME_COLUMN not in row:
            raise LabelCreatorException(
                f"{CPU_TIME_COLUMN} column does not exist"
            )

        cpu_time_ms = row[CPU_TIME_COLUMN]

        # This might be different discrete categories here. The specific types
        # depend on the distribution of queries and the prediction purposes of
        # the ML model.
        if cpu_time_ms < THIRTY_SECOND.num_ms:
            return THIRTY_SECOND.index
        elif cpu_time_ms < ONE_HOUR.num_ms:
            return ONE_HOUR.index
        elif cpu_time_ms < FIVE_HOUR.num_ms:
            return FIVE_HOUR.index
        else:
            return FIVE_HOUR.index + 1

    def label_to_str(self, label_index: int) -> str:
        if label_index == THIRTY_SECOND.index:
            return "< 30s"
        elif label_index == ONE_HOUR.index:
            return "30s - 1h"
        elif label_index == FIVE_HOUR.index:
            return "1h - 5h"
        else:
            return "> 5h"


class PeakMemoryLabelCreator(LabelCreator):
    """
    The class to create a labeled peak memory column.

    This is based on the predefined categories of peak memory, e.g. < 1TB and > 1TB.
    It converts a continuous peak memory to a category.
    """

    def __init__(self) -> None:
        super().__init__()

    def label(self, row: pd.Series) -> int:
        if PEAK_MEMORY_COLUMN not in row:
            raise LabelCreatorException(
                f"{PEAK_MEMORY_COLUMN} column does not exist"
            )

        peak_memory_bytes = row[PEAK_MEMORY_COLUMN]

        if peak_memory_bytes < ONE_MB.num_bytes:
            return ONE_MB.index
        elif peak_memory_bytes < ONE_TB.num_bytes:
            return ONE_TB.index
        else:
            return ONE_TB.index + 1

    def label_to_str(self, label_index: int) -> str:
        if label_index == ONE_MB.index:
            return "< 1MB"
        elif label_index == ONE_TB.index:
            return "1MB - 1TB"
        else:
            return "> 1TB"
