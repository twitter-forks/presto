# Model Serving

The `presto-query-predictor` package implemented a Flask web application for
model serving. The service is encapsulated in the `predictor_app` variable,
which can be easily used by running

```python
from query_predictor.predictor.predictor_app import predictor_app

predictor_app.run()
```

There are two API endpoints:

* `/v1/cpu`

This API endpoint receives an HTTP request with the query statement carried in
the `query` field as a JSON message in the request body. It returns a response
with the expected CPU time range wrapped in the response body. An example of the
response body is shown below.

``` json
{
    "cpu_pred_label": 0,
    "cpu_pred_str": "< 30s"
}
```

* `/v1/memory`

This API endpoint receives an HTTP request with the query statement carried in
the `query` field as a JSON message in the request body. It returns a response
with the expected peak memory bytes range wrapped in the response body. An example
of the response body is shown below.

```json
{
    "memory_pred_label": 0,
    "memory_pred_str": "< 1MB"
}
```

The web service requires four models trained beforehand:

* CPU vectorization model
* Memory vectorization model
* CPU classification model
* Memory classification model

The parameters about these models can be provisioned through a serving configuration
YAML file. An example is shown below.

```yaml
models:
  cpu_model:
    label: cpu_time_label
    feature: query
    type: XGBoost
    path: models/model-cpu.bin
    name: XGBoost-CPU
    description: An XGBoost model to predict cpu time of each SQL query
    version: 0.1.0
  memory_model:
    label: peak_memory_label
    feature: query
    type: XGBoost
    path: models/model-memory.bin
    name: XGBoost-Memory
    description: An XGBoost model to predict peak memory bytes of each SQL query
    version: 0.1.0
vectorizers:
  cpu_vectorizer:
    feature: query
    type: tfidf
    path: models/vec-cpu.bin
    name: tfidf-cpu
    description: A TF-IDF vectorizer for SQL queries
    version: 0.1.0
  memory_vectorizer:
    feature: query
    type: tfidf
    path: models/vec-memory.bin
    name: tfidf-memory
    description: A TF-IDF vectorizer for SQL queries
    version: 0.1.0
```

!!! info
    The `predictor_app` provides a simple interface to serve the models in the
    production environment. You can also use other web frameworks to serve
    these models.
