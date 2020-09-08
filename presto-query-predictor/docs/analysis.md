# Log Analysis

Usually, we would like to implement an exploratory analysis on the dataset to gain
insights before creating an end-to-end machine learning pipeline. The
`presto-query-predictor` contains a Jupyter notebook to achieve that analysis.

It contains

* Basic analysis such as the distribution of principals.
* Analysis of CPU and memory usages of historical queries.
* Analysis of Failed queries.

To run the notebook, make sure you have installed the Jupyter notebook server
locally. Afterward,

```bash
cd jupyter_notebooks
jupyter notebook
```

The notebook server should start up and a browser window should open on your
machine, allowing you to choose a notebook from this directory.
