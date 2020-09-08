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
This file shows an example of transforming datasets.
"""
from pathlib import Path
from query_predictor.predictor.data_loader import DataLoader
from query_predictor.predictor.data_transformer import DataTransformer


if __name__ == "__main__":
    curr_dir = Path(__file__).parent.absolute()

    # This is the fake dataset embedded in the query predictor.
    # The dataset can also be loaded by running ``load_tpch`` method.
    file_path = curr_dir.parent / "query_predictor/datasets/data/tpch.csv"
    data_loader = DataLoader(file_path=file_path)
    data_frame = data_loader.load()

    print("Dataset loaded:")
    print(data_frame)

    transformer_list = [
        "drop_failed_queries",
        "create_labels",
        "to_lower_queries",
        "select_training_columns",
    ]
    data_transformer = DataTransformer(data_frame, transformer_list)
    data_frame = data_transformer.transform()

    print("Dataset transformed:")
    print(data_frame)
