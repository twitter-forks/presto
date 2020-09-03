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
This file contains the constant variables for the query predictor.
"""
from collections import namedtuple

CPU_TIME_COLUMN = "cpu_time_ms"
CPU_TIME_LABEL = "cpu_time_label"
CPUTime = namedtuple("CPUTime", "index num_ms")
THIRTY_SECOND = CPUTime(0, 1000 * 30)
ONE_HOUR = CPUTime(1, 1000 * 60 * 60)
FIVE_HOUR = CPUTime(2, 1000 * 60 * 60 * 5)

PEAK_MEMORY_COLUMN = "peak_memory_bytes"
PEAK_MEMORY_LABEL = "peak_memory_label"
PeakMemory = namedtuple("PeakMemory", "index num_bytes")
ONE_MB = PeakMemory(0, 1024)
ONE_TB = PeakMemory(1, 1024 * 1024 * 1024)

MLModel = namedtuple("MLModel", "metric label")
CPU_TIME_MODEL = MLModel("cpu", CPU_TIME_LABEL)
PEAK_MEMORY_MODEL = MLModel("memory", PEAK_MEMORY_LABEL)

QUERY_COLUMN = "query"
QUERY_STATE_COLUMN = "query_state"

DEFAULT_PERSIST = False
DEFAULT_TEST_SIZE = 0.2

CPU_CATEGORY = "cpu"
MEMORY_CATEGORY = "memory"
CATEGORIES = [CPU_CATEGORY, MEMORY_CATEGORY]
