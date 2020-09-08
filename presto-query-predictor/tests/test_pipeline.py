#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#  #
#      http://www.apache.org/licenses/LICENSE-2.0
#  #
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
from query_predictor.datasets import load_tpch
from query_predictor.predictor.pipeline import Pipeline


def test_pipeline(create_transformer_tmp_config, create_trainer_tmp_config):
    data_frame = load_tpch()
    transformer_config_path = create_transformer_tmp_config
    trainer_config_path = create_trainer_tmp_config
    pipeline = Pipeline(
        data_frame=data_frame,
        transformer_config_path=transformer_config_path,
        trainer_config_path=trainer_config_path,
    )
    pipeline.exec()
    report = pipeline.report

    assert isinstance(
        report, dict
    ), "The report returned from a pipeline should be a dictionary"
    assert (
        "confusion_matrix" in report
    ), "Confusion matrix should be included in the report"
