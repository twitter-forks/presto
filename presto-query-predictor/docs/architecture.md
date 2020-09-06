# Architecture

The high-level design of the package is shown below.

<img src="/img/architecture.png" width="600">

In general, the package encapsulates two machine learning models trained from
historical Presto queries into a web-based predictor service to predict CPU
time and peak memory bytes of future Presto queries. It creates an ML pipeline,
including data ingestion, data transformation, vectorization, model training,
model evaluation, and model serving.

!!! note
    As the package employs machine learning techniques, it requires historical
    request logs for training. And the dataset size may have an impact on the
    performance of models trained.

As we don't need estimation of exact values of CPU time and peak memory bytes,
the package applies data discretization to convert the continuous data to discrete
data. As a consequence, the package categories the CPU time and peak memory
bytes into multiple ranges or buckets. By default, each CPU time falls into one
of the four categories: *< 30s, 30s - 1h, 1h - 5h, and > 5h*; peak memory bytes fall
into one of the three types: *< 1MB, 1MB - 1TB, and > 1TB*. This approach converts
the regression task to a classification problem.
