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
This file contains the exceptions of the query predictor.
"""
from typing import Optional

INTERNAL_ERROR = 500


class PredictorException(Exception):
    """
    The base class for query predictor exceptions

    :param message: The string of the error message.
    :param status: The error code in the HTTP format.
    :param payload: Additional parameters.
    """

    def __init__(
        self,
        message: str,
        status: str = INTERNAL_ERROR,
        payload: Optional = None,
    ) -> None:
        self.message = message
        self.status = status
        self.payload = payload


class DataLoaderException(PredictorException):
    """
    The exception class for errors in loading data.
    """

    pass


class DataTransformerException(PredictorException):
    """
    The exception class for errors in transforming data.
    """

    pass


class LabelCreatorException(PredictorException):
    """
    The exception class for errors in creating labels.
    """

    pass
