# presto-query-predictor

**presto-query-predictor** is a Python module introducing machine learning
techniques to the Presto ecosystem. It contains a machine learning pipeline for
the model training/evaluation and a query predictor web service to predict CPU
and memory usages of Presto queries.

## Installation

After cloning the GitHub repository,

``` bash
pip3 install -e .  # Installs the presto-query-predictor package locally
pip3 install -r requirements.txt  # Installs dependencies
```

An alternative way is to install the package from PyPi,

``` bash
pip3 install presto-query-predictor
```

We recommend installing the package in a Python virtual environment instead
of installing it globally.

## Examples

The `query_predictor/` folder contains the core of the package. We have prepared
some examples in the `example/` folder, including

* `load_data.py` - An example to load the embedded fake TPCH-based dataset.
* `transform.py` - An example to transform datasets for further training.
* `train.py` - An example to train CPU and memory models.
* `tune.py` - An example to tune classification algorithms.
* `app.py` - An example to create a query predictor web service.

### Training

A simple way to get a sense of the CPU and memory model training is running the
examples in the `example/` folder.

``` bash
cd examples
python3 transform.py
python3 train.py
```

The *presto-query-predictor* package can only be executed in a Python 3
environment. It does not support Python 2.

Afterward, the trained models should be generated in the `models` folder, including

``` bash
models/
    vec-cpu.bin
    vec-memory.bin
    model-cpu.bin
    model-memory.bin
```

By default, the vectorizers are trained from the **TF-IDF** algorithm, and the models
are trained from **XGBoost** classifiers. The dataset used for training is a
faked dataset based on the TPC-H benchmark with only 22 samples.

### Serving

After running

``` bash
python3 app.py
```

A [Flask](https://flask.palletsprojects.com/) web application should be created
at [http://0.0.0.0:8000/](http://0.0.0.0:8000/).
There is a web UI for the application where you can fill in the form with a
query for resources prediction.

<img src="https://github.com/twitter-forks/presto/blob/query-predictor/presto-query-predictor/docs/img/web-app.png?raw=true" width="500">
