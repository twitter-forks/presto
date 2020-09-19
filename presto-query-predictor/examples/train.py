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
This file contains examples to run ML pipelines to train CPU and memory models.
"""
import pprint
from pathlib import Path
from query_predictor.datasets import load_tpch
from query_predictor.predictor.pipeline import Pipeline

# If you don't want to see the logs during training, import logging package and
# use logging.disable() to disable logging utilities.
if __name__ == "__main__":
    curr_dir = Path(__file__).parent

    data_frame = load_tpch()
    transformer_config_path = curr_dir / "conf/transformer.yaml"
    cpu_trainer_config_path = curr_dir / "conf/trainer-cpu.yaml"
    memory_trainer_config_path = curr_dir / "conf/trainer-memory.yaml"

    # Runs the pipeline to train a model to predict cpu time.
    cpu_pipeline = Pipeline(
        data_frame=data_frame,
        transformation_required=True,
        transformer_config_path=transformer_config_path,
        trainer_config_path=cpu_trainer_config_path,
    )
    cpu_pipeline.exec()

    pp = pprint.PrettyPrinter()
    pp.pprint(cpu_pipeline.report)

    # Runs the pipeline to train a model to predict peak memory bytes.
    memory_pipeline = Pipeline(
        data_frame=data_frame,
        transformation_required=True,
        transformer_config_path=transformer_config_path,
        trainer_config_path=memory_trainer_config_path,
    )
    memory_pipeline.exec()

    pp = pprint.PrettyPrinter()
    pp.pprint(memory_pipeline.report)
