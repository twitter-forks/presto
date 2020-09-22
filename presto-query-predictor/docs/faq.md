# FAQ

### How to determine the categories for CPU time and peak memory bytes?

It is a critical design decision of categorising the continuous CPU time and peak
memory bytes. We believe the specific approach highly depends on the distribution
of queries in the dataset and the usages of the models trained. For example, if
we would like to apply the models to balance *resource-consuming* queries across
multiple Presto clusters, we may want to put these queries into a specific category.

### Does the package have real Presto log datasets?

No. We don't provide any real Presto log datasets fetched from the production
environment. Instead, we created a faked dataset based on TPC-H queries. It has
22 samples in 9 columns. You can also load your datasets for the machine learning
pipeline by passing the dataset path to the `Pipeline` class. See [ML Pipeline](pipeline.md)
for more details.

### Does the package support TensorFlow and/or PyTorch?

TensorFlow and PyTorch are both popular deep learning libraries. Currently,
we don't support deep learning algorithms, though we have conducted some experiments
in RNN/CNN built with TensorFlow. We did see performance improvement with deep
learning, but it is not significant. The purpose of this package is to introduce
machine learning techniques to the Presto ecosystem, so we want to lower the
possible technical barrier as much as possible. But as the design of the
pipeline is well-extendable and there is increasing popularity of deep learning,
we have a plan to add support of TensorFlow and PyTorch.

### Does the package support tuning?

Hyperparameter tuning is a necessary step to obtain an ideal model. As the purpose
of this package is to create an end-to-end ML pipeline for Presto query prediction
in the production environment, for now, we only provide limited support for tuning
of `RandomForestClassifer`and `LogisticRegressionClassifier`.

Any contributions are appreciated!
