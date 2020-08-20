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
This module contains the methods to load datasets.
"""
from pathlib import Path

import pandas as pd


def load_tpch() -> pd.DataFrame:
    """
    Loads and returns a TPC-H based fake dataset for classification.

    This dataset is created with TPC-H SQL queries and corresponding fake attributes.
    The values are not obtained from real production environment.

    The dataset has 22 samples. It contains the columns: query_id, user_, source,
    environment, catalog, query_state, query, peak_memory_bytes, and cpu_time_ms.

    :return: A pandas data frame with the tpch dataset.

    To use:
    >>> from query_predictor.datasets import load_tpch
    >>> data = load_tpch()
    >>> print(data.columns)
    """
    curr_dir = Path(__file__).parent
    tpch_path = curr_dir / "data/tpch.csv"

    return _load_pandas_csv(tpch_path)


def _load_pandas_csv(file_path: str, delimiter: str = ",") -> pd.DataFrame:
    """
    Loads and returns a pandas dataset.

    :param file_path: The path of the dataset.
    :param delimiter: The delimiter of the dataset. Comma by default.
    :return: A pandas data frame.
    """
    return pd.read_csv(file_path, delimiter=delimiter)
