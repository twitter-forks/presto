# ML Pipeline

## Overview

A typical machine learning pipeline may contain data loading, data transformation,
training(train-dev)/testing datasets splitting, vectorization, classification/
regression, etc.

In the `presto-query-predictor` package, the `Pipeline` class provides a
high-level interface to create a model training pipeline without the necessity to
program in each detailed step. The dataset path or dataset, the transformer
config path, and the trainer config path are the input parameters for a ``Pipeline``
instance. Below is an example to use the class to create a CPU model training
pipeline.

``` python
from pathlib import Path
from query_predictor.predictor.pipeline import Pipeline

# Path of the embedded fake dataset.
# The path can be replaced by other datasets in practical usages.
data_path = parent_dir / "query_predictor/datasets/data/tpch.csv"

# Paths of the default transformer and trainer configs.
# The paths should be replaced by other configs in practical usages.
transformer_config_path = (
    parent_dir / "query_predictor/conf/transformer.yaml"
)
cpu_trainer_config_path = (
    parent_dir / "query_predictor/conf/trainer-cpu.yaml"
)

# Runs the pipeline to train a model to predict cpu time.
cpu_pipeline = Pipeline(
    data_path=data_path,
    transformation_required=True,
    transformer_config_path=transformer_config_path,
    trainer_config_path=cpu_trainer_config_path,
)
cpu_pipeline.exec()

pp = pprint.PrettyPrinter()
pp.pprint(cpu_pipeline.report)
```

## Datasets

The package contains a faked dataset created with some TPC-H SQL queries. The
faked dataset has 22 samples with columns: *query_id, user_, source, environment,
catalog, query_state, query, peak_memory_bytes, and cpu_time_ms*. The dataset
can be loaded through the `load_tpch` method.

``` python
from query_predictor.datasets import load_tpch

data = load_tpch()
print(data)
```

!!! warning
    The faked dataset is for demo purposes only. You need to train models from
    some specific Presto request logs for production purposes.


## Data Transformation
After loading a raw Presto request log dataset, we need to transform the dataset,
e.g. converting SQL queries to lowercase, creating prediction labels, etc. The
package provides a ``DataTransformer`` class for data transformation. The required
transformations are provided through a transformer configuration file.

``` yaml
transformers:               # The transformations executed on the dataset
  - drop_failed_queries     # Drops failed queries whose query state is FAILURE
  - create_labels           # Creates prediction labels for CPU time and peak memory bytes
  - to_lower_queries        # Converts SQL queries to lowercase
  - select_training_columns # Removes unnecessary columns
persist: true               # Whether the dataset after transformations should be persisted or not
persist_path: clean.csv     # Persistence path
```

## Model Training

We apply data vectorization to the query strings in the transformed dataset.
For now, based on the [`scikit-learn`](https://scikit-learn.org/stable/) vectorizers, the package supports

* `DataCountVectorizer` - token count approach
* `DataTfidfVectorizer` - TF-IDF (term frequency-inverse document frequency) approach

After vectorization, we'll split the dataset to training and testing datasets and
apply specific classification algorithms.

* `RandomForestClassifier` - A random forest classifier based on the `scikit-learn` package.
* `LogisticRegressionClassifier` - A logistic regression classifier based on the `scikit-learn` package.
* `XGBoostClassifier` - An XGBoost classifier based on the `xgboost` package.

Any contributions to more classifiers are welcome!

Both the vectorizer's and the classifier's parameters can be provided through a
trainer configuration file. An example of training a CPU model is shown below.

``` yaml
label: cpu_time_label                 # Predictiona label: cpu_time_label or peak_memory_label
feature: query                        # Feature column
vectorizer:       
  type: tfidf                         # Vectorizer type: tfidf or count
  params:                             # Params for the vectorizer, following scikit-learn parameters.
    max_features: 100
    min_df: 1
    max_df: 0.8
  persist: true                       # Whether the vectorizer trained should be persisted or not
  persist_path: models/vec-cpu.bin    # Persistence path
test_size: 0.2                        # Testing dataset proportion during splitting
classifier:
  type: XGBoost                       # Classifier type
  params:                             # Params for the classifier
    max_depth: 2
    objective: 'binary:logistic'
  persist: true                       # Whether the model trained should be persisted or not
  persist_path: models/model-cpu.bin  # Persistence path
```

After the training, a CPU model should be generated in the `models/` folder.
This model can be used to predict CPU usages of future Presto requests.

!!! info
    The vectorizer's and classifier's parameters are for demo purposes. They are
    not optimized. The parameters usually require tuning when changed to another dataset.
