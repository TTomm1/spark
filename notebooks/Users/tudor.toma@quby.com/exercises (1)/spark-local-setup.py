# Databricks notebook source
# MAGIC %md # Local Spark with Jupyter Notebook 
# MAGIC 
# MAGIC Download Spark from the [website](https://spark.apache.org/downloads.html) (the file should end with `.tgz`) and extract the folder within the archive to a suitable location (e.g. `~/bin/`).
# MAGIC You should now have a folder called `spark-2.1.0-bin-hadoop2.7` in that location and that folder contains folders like `bin`, `conf`, `data`, etc.
# MAGIC 
# MAGIC This folder is your `SPARK_HOME` and its path is often needed for applications running Spark.
# MAGIC For instance,  when doing `spark-submit` from the command shell, you have to create the environment variable `$SPARK_HOME` pointing to Spark's home:
# MAGIC 
# MAGIC ```{bash}
# MAGIC $ export SPARK_HOME=~/bin/spark-2.1.0-bin-hadoop2.7
# MAGIC ...
# MAGIC $ spark-submit app.py
# MAGIC ```
# MAGIC 
# MAGIC Later we'll use it to tell the Python package `findspark` where to find Spark.
# MAGIC 
# MAGIC 
# MAGIC Install `findspark` and `jupyter` in your virtual environment (note: Python 3.6 is not yet supported):
# MAGIC 
# MAGIC ```{bash}
# MAGIC $ conda install jupyter -y
# MAGIC $ pip install findspark
# MAGIC ```
# MAGIC 
# MAGIC or if you're not using `conda`:
# MAGIC 
# MAGIC ```
# MAGIC $ pip install jupyter findspark
# MAGIC ```
# MAGIC 
# MAGIC (Create a new [`conda`](http://conda.pydata.org/miniconda.html) or [`virtualenv`](https://virtualenv.pypa.io/en/stable/) environment if you don't have a virtual environment yet.)
# MAGIC 
# MAGIC Run your Jupyter Notebook server:
# MAGIC 
# MAGIC ```{bash}
# MAGIC $ jupyter notebook
# MAGIC ```
# MAGIC 
# MAGIC Import `findspark` and call `.init()` setting `spark_home` to your `SPARK_HOME`. 
# MAGIC For instance:
# MAGIC 
# MAGIC ```{python}
# MAGIC import findspark
# MAGIC findspark.init(spark_home='~/bin/spark-2.1.0-bin-hadoop2.7/
# MAGIC ```
# MAGIC 
# MAGIC You can now import `pyspark` and create a `SparkContext`:
# MAGIC 
# MAGIC ```{python}
# MAGIC import pyspark
# MAGIC 
# MAGIC spark = pyspark.sql.SparkSession.builder \
# MAGIC           .master("local") \
# MAGIC           .getOrCreate()
# MAGIC sc = spark.sparkContext
# MAGIC 
# MAGIC #sc = pyspark.SparkContext(appName="first spark based notebook")
# MAGIC ```
# MAGIC 
# MAGIC Now try to do some computations with Spark!

# COMMAND ----------

