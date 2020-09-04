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
import json
from pathlib import Path
from typing import List
from typing import NamedTuple
from typing import Tuple

from flask import Flask
from flask import jsonify
from flask import render_template
from flask import request
from flask_cors import CORS
from flask_cors import cross_origin

from .constant import QUERY_COLUMN
from .exceptions import BadRequestException
from .exceptions import InternalServerException
from .exceptions import PredictorException
from .label_creator import CPUTimeLabelCreator
from .label_creator import LabelCreator
from .label_creator import PeakMemoryLabelCreator
from .model import MLModel
from .model import MLModelManager
from .model import VectorizerModel
from .model import VectorizerModelManager

# The PEX file fails to find the templates with ``resource_filename`` function.
# So the templates are wrapped in a ``resources`` dependency in the BUILD file.
# And __file__ is relied on to find the correct path.
# This method works in both a normal folder structure and a compressed file.
curr_dir = Path(__file__).parent.absolute()
template_path = curr_dir / "webapp/templates"
predictor_app = Flask(__name__, template_folder=template_path)

cors = CORS(predictor_app)
predictor_app.config["CORS_HEADERS"] = "Content-Type"


@predictor_app.route("/")
def index():
    return render_template("index.html")


@predictor_app.route("/v1/cpu", methods=["POST"])
@cross_origin()
def predict_cpu_time():
    return _predict_query_metric(cpu_predictor_config)


@predictor_app.route("/v1/memory", methods=["POST"])
@cross_origin()
def predict_peak_memory_bytes():
    return _predict_query_metric(memory_predictor_config)


@predictor_app.errorhandler(PredictorException)
def handle_predictor_exception(error):
    payload = dict(error.payload or ())
    payload["message"] = error.message
    payload["status"] = error.status

    return jsonify(payload), error.status


class PredictorConfig(NamedTuple):
    """
    The representation of the config for the query predictor service.
    """

    #: Holds the classifier for the prediction of CPU time or peak memory bytes.
    classifier: MLModel

    #: Holds the vectorizer to convert sql queries to vectors.
    vectorizer: VectorizerModel

    #: Holds the ``LabelCreator`` instance to get the string representation from
    #: the predicted number.
    label_creator: LabelCreator

    #: Holds the label for prediction.
    pred_label: str

    #: Holds the string meaning for the prediction.
    pred_str: str


def _predict_query_metric(config: PredictorConfig) -> str:
    """
    Predicts the CPU time or peak memory bytes of the sql queries embedded in
    each request.

    It extracts sql queries, convert them to lowercase, vectorize them with the
    vectorizer loaded, and predict metrics with classifiers loaded.

    :param config: The predictor service config for prediction.
    :return: A json string of the prediction result.
    """
    try:
        queries = _extract_query()
        queries = _to_lower_queries(queries)
        pred_label = _predict(queries, config.classifier, config.vectorizer)
        pred_str = config.label_creator.label_to_str(pred_label)

        pred_info = {
            config.pred_label: pred_label,
            config.pred_str: pred_str,
        }

        predictor_app.logger.info(json.dumps(pred_info))

        return jsonify(pred_info)
    except PredictorException as err:
        predictor_app.logger.error(err)
        raise err
    except Exception as err:
        predictor_app.logger.error(err)
        raise InternalServerException(str(err))


def _extract_query() -> List[str]:
    """
    Extracts sql queries from each request based on the ``QUERY_COLUMN``.

    :return: A list of queries.
    :raise BadRequestException: If the json data is ``None`` (e.g. the request
    is not in the json format), or the ``QUERY_COLUMN`` is not in the request.
    """
    req_data = request.get_json()
    if req_data is None:
        raise BadRequestException("Empty request body")
    if QUERY_COLUMN not in req_data:
        raise BadRequestException("No query statement")
    predictor_app.logger.info(f"Receive request: {req_data}")

    return [req_data[QUERY_COLUMN]]


def _to_lower_queries(queries: List[str]) -> List[str]:
    """
    Converts a list of queries to lowercase.

    :param queries: Input queries.
    :return: A list of strings in lowercase.
    """
    return [q.lower() for q in queries]


def _predict(queries: List[str], model: MLModel, vec: VectorizerModel) -> int:
    """
    Predict the metric (CPU time or peak memory bytes range) of the first query
    in a list. This is because we only receive one request of a query each time.
    But as the ``transform`` function in the vectorizers require a sequence of
    inputs, we change this query to a list of size 1.

    :param queries: A list of queries for prediction.
    :param model: The ``MLModel`` instance for classification.
    :param vec: The ``VectorizerModel`` instance for vectorization.
    :return: An integer representing the prediction result.
    """
    queries = vec.transform(queries)
    return int(model.predict(queries)[0])


def load_classifiers(
    model_config_path: str = curr_dir / "../conf/serving.yaml",
) -> Tuple[MLModel, MLModel]:
    """
    Loads the classifiers for prediction.

    :param model_config_path: Path for the model config.
    :return: Two ``MLModel`` instances for CPU time and peak memory bytes prediction.
    """
    ml_model_manager = MLModelManager()
    ml_model_manager.load_models(model_config_path)
    cpu_clf = ml_model_manager.get_models_by_category("cpu")[0]
    memory_clf = ml_model_manager.get_models_by_category("memory")[0]

    return cpu_clf, memory_clf


def load_vectorizers(
    model_config_path: str = curr_dir / "../conf/serving.yaml",
) -> Tuple[VectorizerModel, VectorizerModel]:
    """
    Loads the vectorizers for prediction.

    :param model_config_path: Path for the model config.
    :return: Two ``VectorizerModel`` instances for vectorization for CPU time
    and peak memory bytes prediction.
    """
    vec_model_manager = VectorizerModelManager()
    vec_model_manager.load_models(model_config_path)
    cpu_vec = vec_model_manager.get_models_by_category("cpu")[0]
    memory_vec = vec_model_manager.get_models_by_category("memory")[0]

    return cpu_vec, memory_vec


cpu_clf, memory_clf = load_classifiers()
cpu_vec, memory_vec = load_vectorizers()


cpu_predictor_config = PredictorConfig(
    classifier=cpu_clf,
    vectorizer=cpu_vec,
    label_creator=CPUTimeLabelCreator(),
    pred_label="cpu_pred_label",
    pred_str="cpu_pred_str",
)

memory_predictor_config = PredictorConfig(
    classifier=memory_clf,
    vectorizer=memory_vec,
    label_creator=PeakMemoryLabelCreator(),
    pred_label="memory_pred_label",
    pred_str="memory_pred_str",
)
